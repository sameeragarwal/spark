package org.apache.spark.sql.online

import scala.collection.mutable

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

case class OpId(id: Long)

object StatefulOperator {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  def newOpId = OpId(curId.getAndIncrement)

  val state = new mutable.HashMap[(OpId, Long), Any]()
}

trait Stateful {
  self: SparkPlan =>

  val batchId: Int
  val opId: OpId

  type StateType

  def withNewBatchId(batchId: Int): SparkPlan

  def cachedState: Option[RDD[StateType]] = StatefulOperator.state.get(opId, batchId).map(_.asInstanceOf[RDD[StateType]])
}

case class SampledRelation(batchId: Int, numBatches: Int, child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def execute(): RDD[Row] =
    PartitionPruningRDD.create(child.execute(), _ % numBatches == batchId)
}