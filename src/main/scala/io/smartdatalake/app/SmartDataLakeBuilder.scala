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
package io.smartdatalake.app

import java.io.File
import java.time.LocalDateTime

import configs.syntax._
import io.smartdatalake.config.{ConfigLoader, ConfigParser, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{LogUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow._
import org.apache.spark.sql.SparkSession
import scopt.OptionParser


/**
 * This case class represents a default configuration for the App.
 * It is populated by parsing command-line arguments.
 * It also specifies default values.
 *
 * @param feedSel         Regex pattern to select the feed to execute.
 * @param applicationName Application name.
 * @param configuration   A configuration file or a directory containing configuration files.
 * @param master          The Spark master URL passed to SparkContext when in local mode.
 * @param deployMode      The Spark deploy mode passed to SparkContext when in local mode.
 * @param username        Kerberos user name (`username`@`kerberosDomain`) for local mode.
 * @param kerberosDomain  Kerberos domain (`username`@`kerberosDomain`) for local mode.
 * @param keytabPath      Path to Kerberos keytab file for local mode.
 */
case class SmartDataLakeBuilderConfig(feedSel: String = null,
                                      applicationName: String = null,
                                      configuration: Option[String] = None,
                                      master: Option[String] = None,
                                      deployMode: Option[String] = None,
                                      username: Option[String] = None,
                                      kerberosDomain: Option[String] = None,
                                      keytabPath: Option[File] = None,
                                      partitionValues: Option[Seq[PartitionValues]] = None,
                                      multiPartitionValues: Option[Seq[PartitionValues]] = None,
                                      parallelism: Int = 1,
                                      overrideJars: Option[Seq[String]] = None
                                ) {
  def validate(): Unit = {
    assert(master.nonEmpty, "spark master must be defined in configuration")
    assert(!master.contains("yarn") || deployMode.nonEmpty, "spark deploy-mode must be set if spark master=yarn")
    assert(partitionValues.isEmpty || multiPartitionValues.isEmpty, "partitionValues and multiPartitionValues cannot be defined at the same time")
  }
  def getPartitionValues: Option[Seq[PartitionValues]] = partitionValues.orElse(multiPartitionValues)
}

case class GlobalConfig( kryoClasses: Option[Seq[String]] = None, sparkOptions: Option[Map[String,String]] = None)

/**
 * Abstract Smart Data Lake Command Line Application.
 */
abstract class SmartDataLakeBuilder extends SmartDataLakeLogger {

  // read version from package manifest (not defined if project is executed in IntellJ)
  val appVersion = Option(getClass.getPackage.getImplementationVersion).getOrElse("develop")
  val appName = getClass.getSimpleName.replaceAll("\\$$","") // remove $ from object name and use it as appName

  /**
   * Create a new SDL configuration and initialize it with environment variables if they are set.
   *
   * This method also sets default values if environment variables are not set.
   *
   * @return a new, initialized [[SmartDataLakeBuilderConfig]].
   */
  def initConfigFromEnvironment: SmartDataLakeBuilderConfig = {
    SmartDataLakeBuilderConfig(
      master = sys.env.get("SDL_SPARK_MASTER_URL").orElse(Some("local[*]")),
      deployMode = sys.env.get("SDL_SPARK_DEPLOY_MODE").orElse(Some("client")),
      username = sys.env.get("SDL_KERBEROS_USER"),
      kerberosDomain = sys.env.get("SDL_KERBEROS_DOMAIN"),
      keytabPath = sys.env.get("SDL_KEYTAB_PATH").map(new File(_))
    )
  }

  /**
   * The Parser defines how to extract the options from the command line args.
   * Subclasses SmartDataLakeBuilder can define additional options to be extracted.
   */
  protected val parser = new OptionParser[SmartDataLakeBuilderConfig](appName) {
    override def showUsageOnError = true

    head(appName, appVersion)

    opt[String]('f', "feed-sel")
      .required
      .action( (arg, config) => config.copy(feedSel = arg) )
      .text("Regex pattern to select the feed to execute.")
    opt[String]('n', "name")
      .required
      .action( (arg, config) => config.copy(applicationName = arg) )
      .text("The name of the application.")
    opt[String]('c', "config")
      .action( (arg, config) => config.copy(configuration = Some(arg)) )
      .text("A configuration file or a directory containing configuration files.")
    opt[String]('m', "master")
      .action( (arg, config) => config.copy(master = Some(arg)))
      .text("The Spark master URL passed to SparkContext (default=local[*], yarn, spark://HOST:PORT, mesos://HOST:PORT, k8s://HOST:PORT).")
    opt[String]('x', "deploy-mode")
      .action( (arg, config) => config.copy(deployMode = Some(arg)))
      .text("The Spark deploy mode passed to SparkContext (default=client, cluster).")
    opt[String]("partition-values")
      .action((arg, config) => config.copy(partitionValues = Some(PartitionValues.parseSingleColArg(arg))))
      .text(s"Partition values to process in format ${PartitionValues.singleColFormat}.")
    opt[String]("multi-partition-values")
      .action((arg, config) => config.copy(partitionValues = Some(PartitionValues.parseMultiColArg(arg))))
      .text(s"Multi partition values to process in format ${PartitionValues.multiColFormat}.")
    opt[Int]("parallelism")
      .action((arg, config) => config.copy(parallelism = arg))
      .text(s"Parallelism for DAG run.")
    opt[String]("override-jars")
      .action((arg, config) => config.copy(overrideJars = Some(arg.split(','))))
      .text("Comma separated list of jars for child-first class loader. The jars must be present in classpath.")

    help("help").text("Display the help text.")

    version("version").text("Display version information.")
  }


  /**
   * Parses the supplied (command line) arguments.
   *
   * This method parses command line arguments and creates the corresponding [[SmartDataLakeBuilderConfig]]
   *
   * @param args an Array of command line arguments.
   * @param config a configuration initialized with default values.
   * @return a new configuration with default values overwritten from the supplied command line arguments.
   */
  def parseCommandLineArguments(args: Array[String], config: SmartDataLakeBuilderConfig): Option[SmartDataLakeBuilderConfig] = {
    parser.parse(args, config)
  }

  /**
   * Run the application with the provided configuarion.
   *
   * @param appConfig Application configuration (parsed from command line).
   */
  def run(appConfig: SmartDataLakeBuilderConfig): Unit = {

    // validate application config
    appConfig.validate()

    // init config
    logger.info(s"Feed selector: ${appConfig.feedSel}")
    logger.info(s"Application: ${appConfig.applicationName}")
    logger.info(s"Master: ${appConfig.master}")
    logger.info(s"Deploy-Mode: ${appConfig.deployMode}")

    // load config
    val config = appConfig.configuration match {
      case Some(configuration) => ConfigLoader.loadConfigFromFilesystem(configuration)
      case None => ConfigLoader.loadConfigFromClasspath
    }
    val globalConfig = config.get[Option[GlobalConfig]]("global").value.getOrElse(GlobalConfig())

    // parse config objects and search actions to execute by feedSel
    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    val actions = registry.getActions.filter(_.metadata.flatMap(_.feed).exists( _.matches(appConfig.feedSel)))
    logger.info(s"selected actions ${actions.map(_.id).mkString(", ")}")

    // create Spark Session
    implicit val session: SparkSession = AppUtil.createSparkSession(
      name = s"${appConfig.applicationName} ${appConfig.feedSel}",
      appConfig.master.get,
      appConfig.deployMode,
      globalConfig.kryoClasses,
      globalConfig.sparkOptions)
    LogUtil.setLogLevel(session.sparkContext)

    // create and execute actions
    implicit val context: ActionPipelineContext = ActionPipelineContext(appConfig.feedSel, appConfig.applicationName, registry, referenceTimestamp = Some(LocalDateTime.now))
    // TODO: what about runId?
    val actionDAGRun = ActionDAGRun(actions, runId = "test", appConfig.getPartitionValues.getOrElse(Seq()), appConfig.parallelism )
    actionDAGRun.prepare
    actionDAGRun.init
    actionDAGRun.exec
  }
}
