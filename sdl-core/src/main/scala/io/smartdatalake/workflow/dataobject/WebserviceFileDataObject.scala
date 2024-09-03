/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package io.smartdatalake.workflow.dataobject

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.WebserviceMethod.WebserviceMethod
import io.smartdatalake.util.webservice._
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.authMode.HttpAuthMode
import org.apache.tika.Tika

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import scala.util.{Failure, Success, Try}

/**
 * List of partitions possible values for this partition
 * @param name partition name
 * @param values possible values for this partition
 */
case class WebservicePartitionDefinition(name: String, values: Seq[String])

/**
 * Proxy configuration used to make HTTP-connection.
 * @param host proxy host
 * @param port proxy port
 */
case class HttpProxyConfig(host: String, port: Int, user: Option[StringOrSecret] = None, password: Option[StringOrSecret] = None)

case class HttpTimeoutConfig(connectionTimeoutMs: Int, readTimeoutMs: Int)

/**
 * [[DataObject]] to call webservice and return response as InputStream, or upload data as OutputStream to webservice.
 *
 * The corresponding Action to process the response or upload data should be a FileTransferAction.
 * This is implemented as FileRefDataObject because the response / upload data is treated as some file content.
 *
 * FileRefDataObjects support partitioned data. For a WebserviceFileDataObject partitions are mapped as query parameters to create query string.
 *
 * Query parameter (partitions) and possible values can be configured through `partitionDefs` attribute.
 * If no values are given for a query parameter, the values are taken from the partition values of the input SubFeed, e.g. the command line if it is the first Action in the DAG.
 *
 * @param url URL of the webservice
 * @param additionalHeaders Additional headers to pass with the http request
 * @param timeouts optional configuration of HTTP timeouts
 * @param authMode Optional configuration of webservice authentication. Supported `AuthMode`s are BasicAuthMode and CustomHttpAuthMode.
 *                 CustomHttpAuthMode can be used to implement a custom authentication protocol, e.g. AzureADClientGrantAuthMode in sdl-azure module.
 * @param mimeType Optionally specify mime-type of Webservice response. If not specified `tika`-library is used to guess the type.
 * @param writeMethod HTTP method used when uploading data to a webservice.
 *                    Default method is POST.
 * @param proxy optional Proxy configuration used to make HTTP-connection.
 * @param followRedirects if redirects should be followed when creating HTTP-connection. Default is false because of security concerns.
 * @param partitionDefs Optional list of partitions and possible partition values.
 *                      Partitions and their values are mapped as query parameter in webservice requests.
 *                      Multiple values are translated into multiple requests. Each request handles one combination of partition values.
 * @param partitionLayout definition of partitions in query string. Use %<partitionColName>% as placeholder for partition column value in layout.
 * @param pagingLinkRegex if Webservice implements paging, configure a regular expression to extract a link for the next page.
 *                        Example: "\slink=(\S*)"
 */
case class WebserviceFileDataObject(override val id: DataObjectId,
                                    url: String,
                                    additionalHeaders: Map[String, String] = Map(),
                                    timeouts: Option[HttpTimeoutConfig] = None,
                                    authMode: Option[HttpAuthMode] = None,
                                    mimeType: Option[String] = None,
                                    writeMethod: WebserviceMethod = WebserviceMethod.Post,
                                    proxy: Option[HttpProxyConfig] = None,
                                    followRedirects: Boolean = false,
                                    partitionDefs: Seq[WebservicePartitionDefinition] = Seq(),
                                    override val partitionLayout: Option[String] = None,
                                    pagingLinkRegex: Option[String] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends FileRefDataObject with CanCreateInputStream with CanCreateOutputStream with SmartDataLakeLogger {

  // Used to determine mimetype of post data
  val tika = new Tika

  // Always set to Append as we use Webservice to push files
  override val saveMode: SDLSaveMode = SDLSaveMode.Append

  override def partitions: Seq[String] = partitionDefs.map(_.name)

  override def expectedPartitionsCondition: Option[String] = {
    if (partitionDefs.exists(_.values.isEmpty)) Some("false") // if some partitionDef values is not defined, we cannot now what partitions exist...
    else None // all partitions are expected to exist
  }

  override def createsMultiInputStreams: Boolean = pagingLinkRegex.isDefined

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    // prepare auth mode if defined
    authMode.foreach(_.prepare())
  }

  /**
   * Calls webservice and returns response
   *
   * @param query optional URL with replaced placeholders to call
   * @return Response as Array[Byte]
   */
  def getResponse(url: String): Array[Byte] = {
    val webserviceClient = ScalaJWebserviceClient(this, Some(url))

    webserviceClient.get() match {
      case Success(c) => c
      case Failure(e) => logger.error(e.getMessage, e)
        throw new WebserviceException(e.getMessage)
    }
  }

  /**
   * Calls webservice POST method with binary data as body
   *
   * @param body  post body as Byte Array, type will be determined by Tika
   * @param query optional URL with replaced placeholders to call
   * @return Response as Array[Byte]
   */
  def postResponse(body: Array[Byte], query: Option[String] = None): Array[Byte] = {
    val webserviceClient = ScalaJWebserviceClient(this, query.map(url + _))

    // Try to extract Mime Type
    // JSON is detected as text/plain, try to parse it as JSON to more precisely define it as
    // application/json
    val mimetype: String = mimeType.getOrElse {
      tika.detect(body) match {
        case "text/plain" => try {
          new ObjectMapper().readTree(body)
          "application/json"
        } catch {
          case _: Throwable => "text/plain"
        }
        case s => s
      }
    }
    val response = writeMethod match {
      case WebserviceMethod.Post => webserviceClient.post(body, mimetype)
      case WebserviceMethod.Put => webserviceClient.put(body, mimetype)
    }
    response match {
      case Success(c) => c
      case Failure(e) => logger.error(e.getMessage, e)
        throw new WebserviceException(e.getMessage)
    }
  }

  /**
   * getResponse, implement paging, return as InputStreams
   *
   * @param query it should be possible to define the partition to read as query string, but this is not yet implemented
   */
  override def createInputStreams(query: String)(implicit context: ActionPipelineContext): Iterator[InputStream] = {
    val targetUrl = url + query
    val responses: Iterator[Array[Byte]] = new Iterator[Array[Byte]]() {
      var nextLink: Option[String] = Some(targetUrl)
      override def hasNext: Boolean = nextLink.isDefined
      override def next(): Array[Byte] = {
        assert(nextLink.nonEmpty)
        val response = getResponse(nextLink.get)
        nextLink = pagingLinkRegex.flatMap{ patternStr =>
          val pattern = patternStr.r.unanchored
          new String(response) match {
            case pattern(link) =>
              logger.debug(s"next pagingLink found: $link")
              Some(link)
            case _ => None
          }
        }
        response
      }
    }
    responses.map(e => new ByteArrayInputStream(e))
  }

  override def startWritingOutputStreams(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = ()

  /**
   * @param path      is ignored for webservices
   * @param overwrite is ignored for webservices
   * @return outputstream that writes to WebService once it's closed
   */
  override def createOutputStream(path: String, overwrite: Boolean)(implicit context: ActionPipelineContext): OutputStream = {
    new ByteArrayOutputStream() {
      override def close(): Unit = Try {
        super.close()
        val bytes = this.toByteArray
        postResponse(bytes, None)
      } match {
        case Success(_) => ()
        case Failure(e) => throw new RuntimeException(s"($id) Could not post to webservice: ${e.getMessage}", e)
      }
    }
  }

  override def endWritingOutputStreams(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = ()

  override def postWrite(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.postWrite(partitionValues)
    authMode.foreach(_.close())
  }

  /**
   * For WebserviceFileDataObject, every partition is mapped to one FileRef
   */
  override def getFileRefs(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Seq[FileRef] = {
    val partitionValuesToProcess = if (partitions.nonEmpty) {
      // as partitionValues don't need to define a value for every partition, we need to create all partition values and filter them
      val allPartitionValues = createAllPartitionValues
      if (partitionValues.isEmpty || partitionValues.contains(PartitionValues(Map()))) allPartitionValues
      else if (allPartitionValues.nonEmpty) allPartitionValues.filter(allPv => partitionValues.exists(_.elements.forall(filterPvElement => allPv.get(filterPvElement._1).contains(filterPvElement._2))))
      else partitionValues
    } else Seq(PartitionValues(Map())) // create empty default PartitionValue
    // create one FileRef for every PartitionValue
    partitionValuesToProcess.map(createFileRef)
  }

  /**
   * create a FileRef for one given partitionValues
   */
  private def createFileRef(partitionValues: PartitionValues)(implicit context: ActionPipelineContext): FileRef = {
    val queryString = getPartitionString(partitionValues)

    // translate urls special characters into a regular filename
    val translationMap = Map('?' -> '.', '&' -> '.', '=' -> '-')
    def translate(s: String): String = {
      s.map(c => translationMap.getOrElse(c, c))
        .replaceAll("[^A-Za-z0-9\\-._]","")
    }

    val translatedFileName = translate(queryString.getOrElse("result"))
      .dropWhile(_ == '.') // Prevent file names starting with "."

    FileRef(fullPath = queryString.getOrElse(""), fileName = translatedFileName, partitionValues)
  }

  /**
   * generate all partition value combinations from possible query parameter values
   */
  private def createAllPartitionValues = {
    // create partition values from first partition definition
    val headPartitionValuess: Seq[PartitionValues] = partitionDefs.head.values.map(v => PartitionValues(Map(partitionDefs.head.name -> v)))
    // add the following partition definitions
    partitionDefs.tail.foldLeft(headPartitionValuess) {
      case (partitionValuess, partitionDef) =>
        partitionValuess.flatMap(partitionValues => partitionDef.values.map(v => PartitionValues(partitionValues.elements + (partitionDef.name -> v))))
    }
  }

  /**
   * List partition values defined for this web service.
   * Note that this is a fixed list.
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = createAllPartitionValues

  /**
   * No root path needed for Webservice. It can be included in webserviceOptions.url.
   */
  override def path: String = ""

  override def relativizePath(filePath: String)(implicit context: ActionPipelineContext): String = filePath

  override def factory: FromConfigFactory[DataObject] = WebserviceFileDataObject
}

object WebserviceFileDataObject extends FromConfigFactory[DataObject] with SmartDataLakeLogger {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): WebserviceFileDataObject = {
    extract[WebserviceFileDataObject](config)
  }
}
