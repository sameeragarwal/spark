package org.apache.spark.sql.online

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.online.joins.{OnlineBroadcastHashJoin, OnlineShuffledHashJoin}

object BatchPlanner {

  def generate(plan: SparkPlan, batchId: Int): SparkPlan =
    plan transformUp {
      case SampledRelation(_, numBatches, child) => SampledRelation(batchId, numBatches, child)

      case aggregate: OnlineAggregate => aggregate.withNewBatchId(batchId)
    }

}
