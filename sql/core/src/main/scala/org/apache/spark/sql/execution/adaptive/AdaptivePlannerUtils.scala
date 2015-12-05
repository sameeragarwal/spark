/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.adaptive

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SQLContext, execution}
import org.apache.spark.{MapOutputStatistics, ShuffleDependency, SimpleFutureAction}

object AdaptivePlannerUtils {
  def runSubtree(partiallyPlanned: LogicalPlan, sqlContext: SQLContext): LogicalPlan = {
    // Get all BoundaryLogicalPlans.
    val boundaries = partiallyPlanned.collect {
      case b: BoundaryLogicalPlan => b
    }

    // Run those BoundaryLogicalPlans.
    val boundaryToLogicalRDD = runFragment(boundaries.toArray, sqlContext)

    // Replace BoundaryLogicalPlans to LogicalRDDWithStatistics.
    partiallyPlanned transformUp {
      case b: BoundaryLogicalPlan => boundaryToLogicalRDD(b)
    }
  }

  def runFragment(
      boundaries: Array[BoundaryLogicalPlan],
      sqlContext: SQLContext): Map[BoundaryLogicalPlan, LogicalRDDWithStatistics] = {
    val exchanges = boundaries.map(_.exchange)
    exchanges.foreach(println)

    val shuffleDependencies =
      new Array[ShuffleDependency[Int, InternalRow, InternalRow]](exchanges.length)
    val submittedStageFutures =
      new Array[Option[SimpleFutureAction[MapOutputStatistics]]](exchanges.length)

    // Step 1: Get all shuffle dependencies and submit them to DAG scheduler.
    var i = 0
    while (i < exchanges.length) {
      val exchange = exchanges(i)
      val shuffleDependency = exchange.prepareShuffleDependency()
      shuffleDependencies(i) = shuffleDependency
      if (shuffleDependency.rdd.partitions.length != 0) {
        // submitMapStage does not accept RDD with 0 partition.
        // So, we will not submit this dependency.
        submittedStageFutures(i) =
          Some(exchange.sqlContext.sparkContext.submitMapStage(shuffleDependency))
      } else {
        submittedStageFutures(i) = None
      }
      i += 1
    }

    // Step 2: Wait for the finishes of those submitted map stages. Then, we will get
    // MaoOutputStatistics
    val mapOutputStatistics = new Array[Option[MapOutputStatistics]](exchanges.length)
    var j = 0
    while (j < exchanges.length) {
      // This call is a blocking call. If the stage has not finished, we will wait at here.
      mapOutputStatistics(j) = submittedStageFutures(j).map(_.get())
      j += 1
    }

    // Step 3: Wrap the shuffled RDD in a logical plan and then create a map from
    // an exchange to its corresponding logical plan.
    val newPostShuffleRDDs =
      new JHashMap[BoundaryLogicalPlan, LogicalRDDWithStatistics](exchanges.length)
    var k = 0
    while (k < exchanges.length) {
      val exchange = exchanges(k)
      val rdd =
        exchange.preparePostShuffleRDD(shuffleDependencies(k))
      val logicalPlan =
        LogicalRDDWithStatistics(
          exchange,
          mapOutputStatistics(k).map(_.bytesByPartitionId),
          rdd)
      newPostShuffleRDDs.put(boundaries(k), logicalPlan)

      k += 1
    }

    newPostShuffleRDDs.asScala.toMap
  }
}

/**
 * Logical plan node for scanning data from an RDD. It also contains per partition size
 * statistics of this RDD `bytesByPartitionId`. If `bytesByPartitionId` is not defined,
 * the total size of this RDD is 0 byte.
 */
private[sql] case class LogicalRDDWithStatistics(
    sparkPlan: SparkPlan,
    bytesByPartitionId: Option[Array[Long]],
    rdd: RDD[InternalRow])
  extends LeafNode with MultiInstanceRelation {

  override val output: Seq[Attribute] = sparkPlan.output

  // val outputsUnsafeRows: Boolean = sparkPlan.outputsUnsafeRows

  // val outputPartitioning: Partitioning = sparkPlan.outputPartitioning

  override def newInstance(): LogicalRDDWithStatistics.this.type =
    LogicalRDDWithStatistics(
      sparkPlan,
      bytesByPartitionId,
      rdd).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LogicalRDDWithStatistics(_, _, otherRDD) => rdd.id == otherRDD.id
    case _ => false
  }

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = bytesByPartitionId match {
      case Some(sizes) => BigInt(sizes.sum)
      case None => BigInt(0L)
    }
  )
}

/** Physical plan node for scanning data from an RDD. */
private[sql] case class PhysicalRDDWithPartitioning(
    sparkPlan: SparkPlan,
    rdd: RDD[InternalRow],
    extraInformation: String)
  extends execution.LeafNode {

  override val output: Seq[Attribute] = sparkPlan.output

  override val outputPartitioning: Partitioning = sparkPlan.outputPartitioning

  override val outputsUnsafeRows: Boolean = sparkPlan.outputsUnsafeRows

  protected override def doExecute(): RDD[InternalRow] = rdd

  override def simpleString: String = "Scan " + extraInformation + output.mkString("[", ",", "]")
}