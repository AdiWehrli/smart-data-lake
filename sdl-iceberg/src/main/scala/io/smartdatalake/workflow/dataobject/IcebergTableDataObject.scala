/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions._
import io.smartdatalake.metrics.SparkStageMetricsListener
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc._
import io.smartdatalake.util.spark.SparkQueryUtil
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.connection.IcebergTableConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.{ActionPipelineContext, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.Spark3Util.{CatalogAndIdentifier, identifierToTableIdentifier}
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.iceberg.spark.{Spark3Util, SparkSchemaUtil, SparkWriteOptions}
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.{CachingCatalog, PartitionSpec, TableProperties}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsNamespaces, TableCatalog}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, rank}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.lang.reflect.Field
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * [[DataObject]] of type IcebergTableDataObject.
 * Provides details to access Tables in Iceberg format to an Action.
 *
 * Iceberg format maintains a transaction log in a separate metadata subfolder.
 * The schema is registered in Metastore by IcebergTableDataObject.
 * For this either the default spark catalog must be wrapped in an IcebergSessionCatalog,
 * or an additional IcebergCatalog has to be configured. See also [[https://iceberg.apache.org/docs/latest/getting-started/]].
 *
 * The following anomalies between metastore and filesystem might occur:
 * - table is registered in metastore but path does not exist -> table is dropped from metastore
 * - table is registered in metastore but path is empty -> error is thrown. Delete the path manually to clean up.
 * - table is registered and path contains parquet files, but metadata subfolder is missing -> path is converted to Iceberg format
 * - table is not registered but path contains parquet files and metadata subfolder -> Table is registered in catalog
 * - table is not registered but path contains parquet files without metadata subfolder -> path is converted to Iceberg format and table is registered in catalog
 * - table is not registered and path does not exists -> table is created on write
 *
 * IcebergTableDataObject implements
 * - [[CanMergeDataFrame]] by writing a temp table and using one SQL merge statement.
 * - [[CanEvolveSchema]] by using internal Iceberg API.
 * - Overwriting partitions is implemented by using DataFrameWriterV2.overwrite(condition) API in one transaction.
 *
 * @param path hadoop directory for this table. If it doesn't contain scheme and authority, the connections pathPrefix is applied.
 *             If pathPrefix is not defined or doesn't define scheme and authority, default schema and authority is applied.
 *             If Iceberg table is defined on a hadoop catalog, path must be None as it is defined through the catalog directory structure.
 * @param options Options for Iceberg tables see: [[https://iceberg.apache.org/docs/latest/configuration/]]
 * @param table Iceberg table to be written by this output
 * @param saveMode [[SDLSaveMode]] to use when writing files, default is "overwrite". Overwrite, Append and Merge are supported for now.
 * @param allowSchemaEvolution If set to true schema evolution will automatically occur when writing to this DataObject with different schema, otherwise SDL will stop with error.
 * @param historyRetentionPeriod Optional Iceberg retention threshold in hours. Files required by the table for reading versions younger than retentionPeriod will be preserved and the rest of them will be deleted.
 * @param acl override connection permissions for files created tables hadoop directory with this connection
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HiveTableConnection]]
 * @param metadata meta data
 * @param preReadSql SQL-statement to be executed in exec phase before reading input table. If the catalog and/or schema are not
 *                   explicitly defined, the ones present in the configured "table" object are used.
 * @param postReadSql SQL-statement to be executed in exec phase after reading input table and before action is finished. If the catalog and/or schema are not
 *                   explicitly defined, the ones present in the configured "table" object are used.
 * @param preWriteSql SQL-statement to be executed in exec phase before writing output table. If the catalog and/or schema are not
 *                   explicitly defined, the ones present in the configured "table" object are used.
 * @param postWriteSql SQL-statement to be executed in exec phase after writing output table. If the catalog and/or schema are not
 *                   explicitly defined, the ones present in the configured "table" object are used.
 */
case class IcebergTableDataObject(override val id: DataObjectId,
                                  path: Option[String] = None,
                                  override val partitions: Seq[String] = Seq(),
                                  override val options: Map[String,String] = Map(),
                                  override val schemaMin: Option[GenericSchema] = None,
                                  override var table: Table,
                                  override val constraints: Seq[Constraint] = Seq(),
                                  override val expectations: Seq[Expectation] = Seq(),
                                  saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                  override val allowSchemaEvolution: Boolean = false,
                                  historyRetentionPeriod: Option[Int] = None, // hours
                                  acl: Option[AclDef] = None,
                                  connectionId: Option[ConnectionId] = None,
                                  override val expectedPartitionsCondition: Option[String] = None,
                                  override val housekeepingMode: Option[HousekeepingMode] = None,
                                  override val metadata: Option[DataObjectMetadata] = None,
                                  override val preReadSql: Option[String] = None,
                                  override val postReadSql: Option[String] = None,
                                  override val preWriteSql: Option[String] = None,
                                  override val postWriteSql: Option[String] = None)
                                 (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with CanMergeDataFrame with CanEvolveSchema with CanHandlePartitions with HasHadoopStandardFilestore with ExpectationValidation with CanCreateIncrementalOutput {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[IcebergTableConnection](c))

  // prepare final path and table
  @transient private var hadoopPathHolder: Path = _

  val filetypePattern: String = ".*(\\.parquet|\\.avro|\\.orc|c\\d\\d\\d)$" // Iceberg supports to read mixed tables! 'c000' can be the file ending for parquet files of legacy hive tables!

  def hadoopPath(implicit context: ActionPipelineContext): Path = {
    implicit val session: SparkSession = context.sparkSession
    val thisIsTableExisting = isTableExisting
    val thisIsPathBasedCatalog = isPathBasedCatalog(getIcebergCatalog)
    require(thisIsTableExisting || thisIsPathBasedCatalog || path.isDefined, s"($id) Iceberg table ${table.fullName} does not exist, so path must be set.")

    if (hadoopPathHolder == null) {
      hadoopPathHolder = if (thisIsPathBasedCatalog) {
        val hadoopCatalog = getHadoopCatalog(getIcebergCatalog).get
        new Path(getHadoopCatalogDefaultPath(hadoopCatalog, getTableIdentifier))
      } else if (thisIsTableExisting) {
        new Path(getIcebergTable.location)
      } else getAbsolutePath

      // For existing tables, check to see if we write to the same directory. If not, issue a warning.
      if (thisIsTableExisting && path.isDefined) {
        // Normalize both paths before comparing them (remove tick / tock folder and trailing slash)
        val hadoopPathNormalized = HiveUtil.normalizePath(hadoopPathHolder.toString)
        val definedPathNormalized = HiveUtil.normalizePath(getAbsolutePath.toString)

        if (definedPathNormalized != hadoopPathNormalized)
          logger.warn(s"($id) Table ${table.fullName} exists already with different path ${hadoopPathHolder}. New path definition ${getAbsolutePath} is ignored!")
      }
    }
    hadoopPathHolder
  }

  private def getAbsolutePath(implicit context: ActionPipelineContext) = {
    val prefixedPath = HdfsUtil.prefixHadoopPath(path.get, connection.map(_.pathPrefix))
    HdfsUtil.makeAbsolutePath(prefixedPath)(getFilesystem(prefixedPath, context.serializableHadoopConf)) // dont use "filesystem" to avoid loop
  }

  table = table.overrideCatalogAndDb(connection.flatMap(_.catalog), connection.map(_.db))
  if (table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  // prepare tmp table used for merge statement
  private val tmpTable = {
    val tmpTableName = s"${table.name}_sdltmp"
    table.copy(name = tmpTableName)
  }

  assert(Seq(SDLSaveMode.Overwrite, SDLSaveMode.Append, SDLSaveMode.Merge).contains(saveMode), s"($id) Only saveMode Overwrite, Append and Merge supported for now.")

  def getMetadataPath(implicit context: ActionPipelineContext) = {
    options.get("write.metadata.path").map(new Path(_))
      .getOrElse(new Path(hadoopPath,"metadata"))
  }

  @tailrec
  private def getHadoopCatalog(catalog: Catalog): Option[HadoopCatalog] = {
    catalog match {
      case c: HadoopCatalog => Some(c)
      case c: CachingCatalog =>
        val getWrappedCatalog: Field = c.getClass.getDeclaredField("catalog")
        getWrappedCatalog.setAccessible(true)
        getHadoopCatalog(getWrappedCatalog.get(c).asInstanceOf[Catalog])
      case _ => None
    }
  }

  private def isPathBasedCatalog(catalog: Catalog): Boolean = {
    getHadoopCatalog(catalog).isDefined
  }

  private def getHadoopCatalogDefaultPath(catalog: HadoopCatalog, tableIdentifier: TableIdentifier): String = {
    val getDefaultWarehouseLocation = catalog.getClass.getDeclaredMethod("defaultWarehouseLocation", classOf[TableIdentifier])
    getDefaultWarehouseLocation.setAccessible(true)
    getDefaultWarehouseLocation.invoke(catalog, tableIdentifier).asInstanceOf[String]
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.prepare
    if (connection.exists(_.checkIcebergSparkOptions)) {
      require(session.conf.getOption("spark.sql.extensions").toSeq.flatMap(_.split(',')).contains("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        s"($id) Iceberg spark properties are missing. Please set spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions and org.apache.iceberg.spark.SparkSessionCatalog")
    }
    val thisIsPathBasedCatalog = isPathBasedCatalog(getIcebergCatalog)
    if (thisIsPathBasedCatalog && path.nonEmpty) logger.warn(s"($id) path is ignored for path based catalogs like HadoopCatalog.")
    if (!isDbExisting) {
      // DB (schema) is created automatically by iceberg when creating tables. But we would like to keep the same behaviour as done by spark_catalog, where only default DB is existing, and others must be created manually.
      require(table.db.contains("default"), s"($id) DB ${table.db.get} doesn't exist (needs to be created manually).")
    }
    if (!isTableExisting) {
      require(path.isDefined || thisIsPathBasedCatalog, s"($id) If Iceberg table does not exist yet, path must be set.")
      if (filesystem.exists(hadoopPath)) {
        if (filesystem.exists(getMetadataPath)) {
          // define an iceberg table, metadata can be read from files.
          getIcebergCatalog.registerTable(getTableIdentifier, getMetadataPath.toString)
          logger.info(s"($id) Creating Iceberg table ${table.fullName} for existing path $hadoopPath")
        } else {
          // if path has existing parquet files, convert to iceberg table
          require(checkFilesExisting, s"($id) Path $hadoopPath exists but contains no data files. Delete whole base path to reset Iceberg table.")
          convertPathToIceberg
        }
      }
    } else if (filesystem.exists(hadoopPath)) {
      if (!filesystem.exists(getMetadataPath)) {
        // if path has existing parquet files but not in iceberg format, convert to iceberg format
        require(checkFilesExisting, s"($id) Path $hadoopPath exists but contains no data files. Delete whole base path to reset Iceberg table.")
        convertTableToIceberg
        logger.info(s"($id) Converted existing table ${table.fullName} to Iceberg table")
      }
    } else {
      dropTable
      logger.info(s"($id) Dropped existing Iceberg table ${table.fullName} because path was missing")
    }
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  /**
   * converts an existing hive table with parquet files to an iceberg table
   */
  private[smartdatalake] def convertTableToIceberg(implicit context: ActionPipelineContext): Unit = {
    SparkActions.get(context.sparkSession).migrateTable(getIdentifier.toString)
  }

  /**
   * converts an existing path with parquet files to an iceberg table
   */
  private[smartdatalake] def convertPathToIceberg(implicit context: ActionPipelineContext): Unit = {
    val dataPath = new Path(hadoopPath, "data")
    if (!filesystem.exists(dataPath)) {
      // move parquet files and partitions from table root folder to data subfolder (Iceberg standard)
      val filesToMove = filesystem.listStatus(hadoopPath)
        .filter(f => (f.isFile && f.getPath.getName.matches(filetypePattern)) || (f.isDirectory && f.getPath.getName.contains("=")))
      logger.info(s"($id) convertPathToIceberg: moving ${filesToMove.length} files to ./data subdirectory")
      filesystem.mkdirs(dataPath)
      filesToMove.foreach { f =>
        val newPath = new Path(dataPath, f.getPath.getName)
        if (!filesystem.rename(f.getPath, newPath)) throw new IllegalStateException(s"($id) Failed to rename ${f.getPath} -> $newPath")
      }
    }
    // create table
    logger.info(s"($id) convertPathToIceberg: creating iceberg table")
    // get schema using Spark. Note that this only work for parquet files.
    val sparkSchema = context.sparkSession.read.parquet(dataPath.toString).schema
    createIcebergTable(sparkSchema)
    // add files
    logger.info(s"($id) convertPathToIceberg: add_files")
    val parallelismStr = connection.flatMap(_.addFilesParallelism.map(", parallelism => " + _)).getOrElse("")
    context.sparkSession.sql(s"CALL ${getIcebergCatalog.name}.system.add_files(table => '${getIdentifier.toString}', source_table => '`parquet`.`$dataPath`'$parallelismStr)")
    // cleanup potential SDLB .schema directory
    HdfsUtil.deletePath(new Path(hadoopPath, ".schema"), doWarn = false)(filesystem)
    logger.info(s"($id) convertPathToIceberg: succeeded")
  }

  private def createIcebergTable(sparkSchema: StructType)(implicit context: ActionPipelineContext) = {
    val schema = SparkSchemaUtil.convert(sparkSchema)
    val partitionSpec = partitions.foldLeft(PartitionSpec.builderFor(schema)) {
      case (partitionSpec, colName) => partitionSpec.identity(colName)
    }.build
    getIcebergCatalog.createTable(getTableIdentifier, schema, partitionSpec, hadoopPath.toString, options.asJava)
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {

    val df = incrementalOutputExpr match {
      case Some(snapshotId) =>
        require(table.primaryKey.isDefined, s"($id) PrimaryKey for table [${table.fullName}] needs to be defined when using DataObjectStateIncrementalMode")
        val icebergTable = if(snapshotId == "0") context.sparkSession.table(table.fullName)
          else {

          // activate temporary cdc view
          context.sparkSession.sql(
            s"""CALL ${getIcebergCatalog.name}.system.create_changelog_view(table => '${getIdentifier.toString}'
               |, options => map('start-snapshot-id', '${snapshotId}')
               |, compute_updates => true
               |, identifier_columns => array('${table.primaryKey.get.mkString("','")}')
               |)""".stripMargin)

          // read cdc events
          val temporaryViewName = table.name + "_changes"

          val windowSpec = Window.partitionBy(table.primaryKey.get.map(col): _*).orderBy(col("_change_ordinal").desc)
          context.sparkSession.read
            .table(temporaryViewName)
            .where(expr("_change_type IN ('INSERT','UPDATE_AFTER')"))
            .withColumn("_rank", rank().over(windowSpec))
            .where("_rank == 1")
            .drop("_rank", "_change_type", "_change_ordinal", "_commit_snapshot_id")
        }
        incrementalOutputExpr = Some(getIcebergTable.currentSnapshot().snapshotId().toString)
        icebergTable
      case _ => context.sparkSession.table(table.fullName)
    }

    validateSchemaMin(SparkSchema(df.schema), "read")
    validateSchemaHasPartitionCols(df, "read")

    df
  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    val genericDf = SparkDataFrame(df)
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(genericDf)).getOrElse(genericDf).inner
    val targetSchema = targetDf.schema

    validateSchemaMin(SparkSchema(targetSchema), "write")
    validateSchemaHasPartitionCols(targetDf, "write")
    validateSchemaHasPrimaryKeyCols(targetDf, table.primaryKey.getOrElse(Seq()), "write")
    if (isTableExisting) {
      val existingSchema = SparkSchema(getSparkDataFrame().schema)
      if (!allowSchemaEvolution) validateSchema(SparkSchema(targetSchema), existingSchema, "write")
    }
  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists(a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined, s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates Iceberg table.
   */
  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                                  (implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = context.sparkSession
    implicit val helper: SparkSubFeed.type = SparkSubFeed

    val genericDf = SparkDataFrame(df)
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(genericDf)).getOrElse(genericDf).inner
    val targetSchema = targetDf.schema

    validateSchemaMin(SparkSchema(targetSchema), "write")
    validateSchemaHasPartitionCols(targetDf, "write")
    validateSchemaHasPrimaryKeyCols(targetDf, table.primaryKey.getOrElse(Seq()), "write")

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    // remember previous snapshot timestamp
    val previousSnapshotId: Option[Long] = if (isTableExisting) Option(getIcebergTable.currentSnapshot()).map(_.snapshotId()) else None
    // V1 writer is needed to create external table
    var dfWriter = targetDf.write
      .format("iceberg")
      .options(options)
    if (isPathBasedCatalog(getIcebergCatalog)) dfWriter = dfWriter.option("location", hadoopPath.toString)
    else dfWriter = dfWriter.option("path", hadoopPath.toString)
    val sparkMetrics = if (isTableExisting) {
      // check scheme
      if (!allowSchemaEvolution) validateSchema(SparkSchema(targetSchema), SparkSchema(session.table(table.fullName).schema), "write")
      // apply
      if (finalSaveMode == SDLSaveMode.Merge) {
        // handle schema evolution on merge because this is not yet supported in Spark <=3.5
        val existingSchema = SparkSchema(getSparkDataFrame().schema)
        if (allowSchemaEvolution && !existingSchema.equalsSchema(SparkSchema(targetSchema))) evolveTableSchema(targetSchema)
        // make sure SPARK_WRITE_ACCEPT_ANY_SCHEMA=false with SQL merge, because this is not supported in Spark 3.5. See also https://github.com/apache/iceberg/issues/9827.
        // TODO: This might be solved with Spark 4.0, as there will be a Spark API for merge.
        updateTableProperty(TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA, "false", TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT.toString)
        // merge operations still need all columns for potential insert/updateConditions. Therefore dfPrepared instead of saveModeTargetDf is passed on.
        mergeDataFrameByPrimaryKey(df, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()))
      } else SparkStageMetricsListener.execWithMetrics(this.id, {
        // Make sure SPARK_WRITE_ACCEPT_ANY_SCHEMA=true for schema evolution
        updateTableProperty(TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA, allowSchemaEvolution.toString, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT.toString)
        // V2 writer can be used if table is existing, it supports overwriting given partitions
        val dfWriterV2 = targetDf
          .writeTo(table.fullName)
          .option(SparkWriteOptions.MERGE_SCHEMA, allowSchemaEvolution.toString)
        if (partitions.isEmpty) {
          SDLSaveMode.execV2(finalSaveMode, dfWriterV2, partitionValues)
        } else {
          val overwriteModeIsDynamic = options.get("partitionOverwriteMode").orElse(session.conf.getOption("spark.sql.sources.partitionOverwriteMode")).contains("dynamic")
          if (finalSaveMode == SDLSaveMode.Overwrite && partitionValues.isEmpty && !overwriteModeIsDynamic) {
            throw new ProcessingLogicException(s"($id) Overwrite without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data. Set option.partitionOverwriteMode=dynamic on this IcebergTableDataObject to enable dynamic partitioning and get around this exception.")
          }
          SDLSaveMode.execV2(finalSaveMode, dfWriterV2, partitionValues, overwriteModeIsDynamic)
        }
      })
    } else SparkStageMetricsListener.execWithMetrics(this.id, {
      if (partitions.isEmpty) {
        dfWriter.saveAsTable(table.fullName)
      } else {
        dfWriter
          .partitionBy(partitions: _*)
          .saveAsTable(table.fullName)
      }
    })

    // get iceberg snapshot summary / stats
    val currentSnapshot = getIcebergTable.currentSnapshot()
    if (logger.isDebugEnabled) logger.debug(s"snapshot after write: ${currentSnapshot.toString}")
    val summary = currentSnapshot.summary().asScala
    if (previousSnapshotId.contains(currentSnapshot.snapshotId())) {
      logger.info(s"($id) No new iceberg snapshot was written. No data was written to this Iceberg table.")
      throw NoDataToProcessWarning(id.id, s"($id) No data was written to Iceberg table by Spark.")
    }
    // add all summary entries except spark application id to metrics
    val icebergMetrics = (summary - "spark.app.id")
      // normalize names lowercase with underscore
      .map{case(k,v) => (k.replace("-","_"), Try(v.toLong).getOrElse(v))}
      // standardize naming
      // Unfortunately this is not possible yet for merge operation, as we only get added/deleted records. Added records contain inserted + updated rows, deleted records probably updated + deleted rows...
      .map{
        case ("added_records", v) if finalSaveMode != SDLSaveMode.Merge => ("rows_inserted", v)
        case (k,v) => (k,v)
      }

    // vacuum iceberg table
    vacuum

    // fix acls
    if (acl.isDefined) AclUtil.addACLs(acl.get, hadoopPath)(filesystem)

    // return
    sparkMetrics ++ icebergMetrics
  }

  private def writeToTempTable(df: DataFrame)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    // check if temp-table existing
    if(getIcebergCatalog.tableExists(getTableIdentifier)) {
      logger.error(s"($id) Temporary table ${tmpTable.fullName} for merge already exists! There might be a potential conflict with another job. It will be replaced.")
    }
    // write to temp-table
    df.write
      .format("iceberg")
      .option("path", hadoopPath.toString+"_sdltmp")
      .saveAsTable(tmpTable.fullName)
  }

  /**
   * Merges DataFrame with existing table data by writing DataFrame to a temp-table and using SQL Merge-statement.
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   * This all is done in one transaction.
   */
  def mergeDataFrameByPrimaryKey(df: DataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = context.sparkSession
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")

    try {
      // write data to temp table
      val metrics = SparkStageMetricsListener.execWithMetrics(this.id,
        writeToTempTable(df)
      )

      // update existing does not work with SQL merge stmt
      val updateExistingStatement = SQLUtil.createUpdateExistingStatement(table, df.columns.toSeq, tmpTable.fullName, saveModeOptions, SQLUtil.sparkQuoteCaseSensitiveColumn(_))
      updateExistingStatement.foreach{stmt =>
        logger.info(s"($id) executing update existing statement with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1 + "=" + e._2).mkString(" ")}")
        context.sparkSession.sql(stmt)
      }

      // override missing columns with null value, as Iceberg needs all target columns be included in insert statement
      val targetCols = session.table(table.fullName).schema.fieldNames
      val missingCols = targetCols.diff(df.columns)
      val saveModeOptionsExt = saveModeOptions.copy(
        insertValuesOverride = saveModeOptions.insertValuesOverride ++ missingCols.map(_ -> "null"),
        updateColumns = if (saveModeOptions.updateColumns.isEmpty) df.columns.diff(table.primaryKey.get) else saveModeOptions.updateColumns
      )
      // prepare SQL merge statement
      // note that we pass all target cols instead of new df columns as parameter, but with customized saveModeOptionsExt
      val mergeStmt = SQLUtil.createMergeStatement(table, targetCols, tmpTable.fullName, saveModeOptionsExt, SQLUtil.sparkQuoteCaseSensitiveColumn(_))
      // execute
      logger.info(s"($id) executing merge statement with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptionsExt).map(e => e._1+"="+e._2).mkString(" ")}")
      logger.debug(s"($id) merge statement: $mergeStmt")
      context.sparkSession.sql(mergeStmt)
      // return
      metrics
    } finally {
      // cleanup temp table
      val tmpTableIdentifier = TableIdentifier.of((getIdentifier.namespace :+ tmpTable.name):_*)
      getIcebergCatalog.dropTable(tmpTableIdentifier)
    }
  }

  def updateTableProperty(name: String, value: String, default: String)(implicit context: ActionPipelineContext) = {
    val currentValue = getIcebergTable.properties.asScala.getOrElse(name, default)
    if (currentValue != value) {
      getIcebergTable.updateProperties.set(name, value).commit
    }
    logger.info(s"($id) updated Iceberg table property $name to $value")
  }

  /**
   * Iceberg has a write option 'mergeSchema' (see also SparkWriteOptions.MERGE_SCHEMA),
   * but it doesnt work as there is another validation before that checks the schema (e.g. QueryCompilationErrors$.cannotWriteTooManyColumnsToTableError in the stack trace)
   * This code is therefore copied from SparkWriteBuilder.validateOrMergeWriteSchema:246ff
   */
  def evolveTableSchema(dsSchema: StructType)(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"($id) evolving Iceberg table schema")
    val table = getIcebergTable
    val caseSensitive = Environment.caseSensitive

    // convert the dataset schema and assign fresh ids for new fields
    val newSchema = SparkSchemaUtil.convertWithFreshIds(table.schema, dsSchema, caseSensitive)

    // update the table to get final id assignments and validate the changes
    val update = table.updateSchema.caseSensitive(caseSensitive).unionByNameWith(newSchema)
    val mergedSchema = update.apply

    // reconvert the dsSchema without assignment to use the ids assigned by UpdateSchema
    val writeSchema = SparkSchemaUtil.convert(mergedSchema, dsSchema, caseSensitive)

    TypeUtil.validateWriteSchema(mergedSchema, writeSchema, false, false)

    // if the validation passed, update the table schema
    update.commit()
  }

  def vacuum(implicit context: ActionPipelineContext): Unit = {
    historyRetentionPeriod.foreach { hours =>
      val (_, d) = PerformanceUtils.measureDuration {
        SparkActions.get(context.sparkSession)
          .expireSnapshots(getIcebergTable)
          .expireOlderThan(System.currentTimeMillis - hours * 60 * 60 * 1000)
          .execute
      }
      logger.info(s"($id) vacuum took $d")
    }
  }

  def getIcebergCatalog(implicit context: ActionPipelineContext): Catalog = {
    getSparkCatalog.icebergCatalog
  }
  def getSparkCatalog(implicit context: ActionPipelineContext): TableCatalog with SupportsNamespaces with HasIcebergCatalog = {
    getCatalogAndIdentifier.catalog match {
      case c: TableCatalog with HasIcebergCatalog with SupportsNamespaces => c
      case c => throw new IllegalStateException(s"($id) ${c.name}:${c.getClass.getSimpleName} is not a TableCatalog with SupportsNamespaces with HasIcebergCatalog implementation")
    }
  }
  def getIdentifier(implicit context: ActionPipelineContext): Identifier = {
    getCatalogAndIdentifier.identifier
  }
  def getTableIdentifier(implicit context: ActionPipelineContext): TableIdentifier = {
    convertToTableIdentifier(getIdentifier)
  }
  def convertToTableIdentifier(identifier: Identifier): TableIdentifier = {
    TableIdentifier.of(Namespace.of(identifier.namespace:_*), identifier.name)
  }
  private def getCatalogAndIdentifier(implicit context: ActionPipelineContext): CatalogAndIdentifier = {
    if (_catalogAndIdentifier.isEmpty) {
      _catalogAndIdentifier = Some(Spark3Util.catalogAndIdentifier(context.sparkSession, table.nameParts.asJava))
    }
    _catalogAndIdentifier.get
  }
  private var _catalogAndIdentifier: Option[CatalogAndIdentifier] = None

  def getIcebergTable(implicit context: ActionPipelineContext) = {
    // Note: loadTable is cached by default in Iceberg catalog
    getIcebergCatalog.loadTable(identifierToTableIdentifier(getIdentifier))
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    if (isPathBasedCatalog(getIcebergCatalog)) {
      // for hadoop catalog only table.db is relevant, table.catalog must be omitted
      getSparkCatalog.namespaceExists(Array(table.db.get))
    } else {
      getSparkCatalog.namespaceExists(table.nameParts.init.toArray)
    }
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    getIcebergCatalog.tableExists(identifierToTableIdentifier(getIdentifier))
  }


  /**
   * Configure whether [[io.smartdatalake.workflow.action.Action]]s should fail if the input file(s) are missing
   * on the file system.
   *
   * Default is false.
   */
  def failIfFilesMissing: Boolean = false

  /**
   * Check if the input files exist.
   *
   * @throws IllegalArgumentException if `failIfFilesMissing` = true and no files found at `path`.
   */
  protected def checkFilesExisting(implicit context: ActionPipelineContext): Boolean = {
    val hasFiles = filesystem.exists(hadoopPath.getParent) &&
      RemoteIteratorWrapper(filesystem.listFiles(hadoopPath, true)).exists(_.getPath.getName.matches(filetypePattern))
    if (!hasFiles) {
      logger.warn(s"($id) No files found at $hadoopPath. Can not import any data.")
      require(!failIfFilesMissing, s"($id) failIfFilesMissing is enabled and no files to process have been found in $hadoopPath.")
    }
    hasFiles
  }

  protected val separator: Char = Path.SEPARATOR_CHAR

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    if (isTableExisting) {
      val dfPartitions = context.sparkSession.table(s"${table.fullName}.partitions")
      val isPartitioned = dfPartitions.columns.contains("partition")
      if (partitions.nonEmpty && !isPartitioned) logger.warn(s"($id) partitions are defined but Iceberg table is not partitioned.")
      if (partitions.isEmpty && isPartitioned) logger.warn(s"($id) partitions are not defined but Iceberg table is partitioned.")
      if (partitions.nonEmpty && isPartitioned) {
        val dfPartitionsPartition = dfPartitions.select(col("partition.*"))
        val partitions = dfPartitionsPartition.collect().toSeq.map(r => r.getValuesMap[Any](dfPartitionsPartition.columns).mapValues(_.toString).toMap)
        partitions.map(PartitionValues(_))
      } else Seq()
    } else Seq()
  }

  /**
   * Note that Iceberg will not delete the whole partition but just the data of the partition because Iceberg keeps history
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    val deleteStmt = SQLUtil.createDeletePartitionStatement(table.fullName, partitionValues, SQLUtil.sparkQuoteCaseSensitiveColumn(_))
    context.sparkSession.sql(deleteStmt)
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    getIcebergCatalog.dropTable(getTableIdentifier, true) // purge
    HdfsUtil.deletePath(hadoopPath, false)(filesystem)
  }

  override def getStats(update: Boolean = false)(implicit context: ActionPipelineContext): Map[String, Any] = {
    try {
      val icebergTable = getIcebergTable
      val branches = icebergTable.refs().asScala.filter(_._2.isBranch).keys.toSeq.mkString(",")
      val oldestSnapshot = icebergTable.history().asScala.minBy(_.timestampMillis())
      val snapshot = icebergTable.currentSnapshot()
      val summary = snapshot.summary().asScala
      val lastModifiedAt = snapshot.timestampMillis()
      val oldestSnapshotTs = oldestSnapshot.timestampMillis()
      val icebergStats = Map(TableStatsType.LastModifiedAt.toString -> lastModifiedAt, TableStatsType.NumRows.toString -> summary("total-records").toLong, TableStatsType.NumDataFilesCurrent.toString -> summary("total-data-files").toInt, TableStatsType.Branches.toString -> branches, TableStatsType.OldestSnapshotTs.toString -> oldestSnapshotTs)
      val columnStats = getColumnStats(update, Some(lastModifiedAt))
      HdfsUtil.getPathStats(hadoopPath)(filesystem) ++ icebergStats ++ getPartitionStats + (TableStatsType.Columns.toString -> columnStats)
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get table stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map(TableStatsType.Info.toString -> e.getMessage)
    }
  }

  override def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])(implicit context: ActionPipelineContext): Map[String, Map[String,Any]] = {
    try {
      val session = context.sparkSession
      import session.implicits._
      val filesDf = context.sparkSession.table(s"${table.fullName}.files")
      val metricsRow = filesDf.select($"readable_metrics.*").head()
      val columns = metricsRow.schema.fieldNames
      columns.map {
        c =>
          val struct = metricsRow.getStruct(metricsRow.fieldIndex(c))
          c -> Map(
            ColumnStatsType.NullCount.toString -> struct.getAs[Long]("null_value_count"),
            ColumnStatsType.Min.toString -> struct.getAs[Any]("lower_bound"),
            ColumnStatsType.Max.toString -> struct.getAs[Any]("upper_bound")
          )
      }.toMap
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get column stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map()
    }
  }

  override def factory: FromConfigFactory[DataObject] = IcebergTableDataObject

  private var incrementalOutputExpr: Option[String] = None

  /**
   * To implement incremental processing this function is called to initialize the DataObject with its state from the last increment.
   * The state is just a string. It's semantics is internal to the DataObject.
   * Note that this method is called on initializiation of the SmartDataLakeBuilder job (init Phase) and for streaming execution after every execution of an Action involving this DataObject (postExec).
   *
   * @param state Internal state of last increment. If None then the first increment (may be a full increment) is delivered.
   */
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {

    incrementalOutputExpr = state.orElse(Some("0"))
  }

   /**
   * Return the state of the last increment or empty if no increment was processed.
   */
  override def getState: Option[String] = {

    incrementalOutputExpr

  }

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    sqlOpt.foreach(stmt => SparkQueryUtil.executeSqlStatementBasedOnTable(session, stmt, table))
  }

}

object IcebergTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): IcebergTableDataObject = {
    extract[IcebergTableDataObject](config)
  }
}

