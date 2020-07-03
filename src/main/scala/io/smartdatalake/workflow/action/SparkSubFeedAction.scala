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
package io.smartdatalake.workflow.action

import io.smartdatalake.definitions.{ExecutionMode, PartitionDiffMode}
import io.smartdatalake.util.misc.PerformanceUtils
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, InitSubFeed, SparkSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

abstract class SparkSubFeedAction extends SparkAction {

  /**
   * Input [[DataObject]] which can CanCreateDataFrame
   */
  def input: DataObject with CanCreateDataFrame

  /**
   * Output [[DataObject]] which can CanWriteDataFrame
   */
  def output:  DataObject with CanWriteDataFrame

  /**
   * Transform a [[SparkSubFeed]].
   * To be implemented by subclasses.
   *
   * @param subFeed [[SparkSubFeed]] to be transformed
   * @return transformed [[SparkSubFeed]]
   */
  def transform(subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed

  private def doTransform(subFeed: SubFeed, thisExecutionMode: Option[ExecutionMode])(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // convert subfeed to SparkSubFeed type or initialize if not yet existing
    var preparedSubFeed = SparkSubFeed.fromSubFeed(subFeed)
    // apply execution mode
    preparedSubFeed = thisExecutionMode match {
      case Some(mode) =>
        val newPartitionValues = ActionHelper.applyExecutionMode(mode, id, input, output, preparedSubFeed.partitionValues)
        preparedSubFeed.copy(partitionValues = newPartitionValues)
      case _ => preparedSubFeed
    }
    // prepare as input SubFeed
    preparedSubFeed = prepareInputSubFeed(preparedSubFeed, input)
    // enrich with fresh DataFrame if needed
    preparedSubFeed = enrichSubFeedDataFrame(input, preparedSubFeed, thisExecutionMode, context.phase)
    // transform
    val transformedSubFeed = transform(preparedSubFeed)
    // update partition values to output's partition columns and update dataObjectId
    validateAndUpdateSubFeedPartitionValues(output, transformedSubFeed).copy(dataObjectId = output.id)
  }

  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.prepare
  }

  /**
   * Action.init implementation
   */
  override final def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")})")
    outputs.collect{ case x: CanWriteDataFrame => x }.foreach(_.init())
    val subFeed = subFeeds.head
    val thisExecutionMode = runtimeExecutionMode(subFeed.isDAGStart)
    Seq(doTransform(subFeed, thisExecutionMode))
  }

  /**
   * Action.exec implementation
   */
  override final def exec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")})")
    val subFeed = subFeeds.head
    val thisExecutionMode = runtimeExecutionMode(subFeed.isDAGStart)
    // transform
    val transformedSubFeed = doTransform(subFeed, thisExecutionMode)
    // write output
    val msg = s"writing to ${output.id}" + (if (transformedSubFeed.partitionValues.nonEmpty) s", partitionValues ${transformedSubFeed.partitionValues.mkString(" ")}" else "")
    logger.info(s"($id) start " + msg)
    setSparkJobMetadata(Some(msg))
    val (noData,d) = PerformanceUtils.measureDuration {
      writeSubFeed(thisExecutionMode, transformedSubFeed, output)
    }
    setSparkJobMetadata()
    val metricsLog = if (noData) ", no data found"
    else getFinalMetrics(output.id).map(_.getMainInfos).map(" "+_.map( x => x._1+"="+x._2).mkString(" ")).getOrElse("")
    logger.info(s"($id) finished writing DataFrame to ${output.id}: duration=$d" + metricsLog)
    // return
    Seq(transformedSubFeed)
  }

  override final def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    assert(inputSubFeeds.size == 1, s"Only one inputSubFeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${inputSubFeeds.map(_.dataObjectId).mkString(",")})")
    assert(outputSubFeeds.size == 1, s"Only one outputSubFeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${outputSubFeeds.map(_.dataObjectId).mkString(",")})")
    postExecSubFeed(inputSubFeeds.head, outputSubFeeds.head)
  }

  def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit = Unit /* NOP */

}
