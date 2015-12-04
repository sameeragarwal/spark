package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.{Strategy, SQLContext}
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.{EnsureRequirements, SparkPlan}

class AdaptivePlanner(
    sqlContext: SQLContext,
    maxIterations: Int = 100)
  extends RuleExecutor[LogicalPlan] {

  lazy val conf: CatalystConf = sqlContext.conf
  lazy val planner: QueryPlanner[SparkPlan] = sqlContext.planner
  lazy val batches: Seq[Batch] = Seq(
    Batch("Plan", Once, PlanNext)
  )

  object PlanNext extends Rule[LogicalPlan] {
    private[this] def shouldPlan(plan: LogicalPlan): Boolean = {
      plan.children.forall {
        case p: PipelineLogicalPlan => true
        case _ => false
      }
    }

    private[this] val ensureRequirements = EnsureRequirements(sqlContext)

    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case p if shouldPlan(p) =>
        // At here, we assume that planner can take out the sparkPlan
        // in a ExecutedLogicalPlan. So, we can just plan the given p.
        val planned = planner.plan(p).next()
        val hasRequiredDistribution = planned.requiredChildDistribution.exists {
          case UnspecifiedDistribution => false
          case _ => true
        }

        // If the SparkPlan "planned" supports adaptive execution.
        // TODO: We should not use true at here.
        val supportsAdaptiveExecution = true

        if (hasRequiredDistribution && supportsAdaptiveExecution) {
          // We create a single coordinator and use it to fill in the children
          // of the logicalPlan p.
          val coordinator = ExchangeCoordinator(planned.children)
          val newChildren = p.children.map(_ => coordinator)
          p.withNewChildren(newChildren)
        } else {
          val hasRequiredOrdering = planned.requiredChildOrdering.exists(_.nonEmpty)

          val withDataMovement = if (hasRequiredDistribution || hasRequiredOrdering) {
            // Add exchange/sort
            ensureRequirements.apply(planned)
          } else {
            planned
          }
          // Transform p to a PipelineLogicalPlan.
          PipelineLogicalPlan(withDataMovement)
        }
    }
  }
}

case class AdaptiveStrategy(planner: QueryPlanner[SparkPlan]) extends Strategy {
  private[this] def parentOfExchangeCoordinator(plan: LogicalPlan): Boolean = {
    val ret = plan.children.nonEmpty && plan.children.forall(_.isInstanceOf[ExchangeCoordinator])
    if (!ret) {
      assert(plan.children.exists(_.isInstanceOf[ExchangeCoordinator]))
    }

    ret
  }

  def apply(logicalPlan: LogicalPlan): Seq[SparkPlan] = logicalPlan match {
    case PipelineLogicalPlan(alreadyPlanned) => alreadyPlanned :: Nil
    case p if parentOfExchangeCoordinator(p) =>
      val coordinator = p.children.head.asInstanceOf[ExchangeCoordinator]
      planner.plan(p.withNewChildren(coordinator.expand())).toSeq
  }
}

// TODO: Probably we can avoid this by just add a flag in a logicalPlan
// so we can know if a logicalPlan has been visited by AdaptivePlanner.
private[sql] case class PipelineLogicalPlan(alreadyPlanned: SparkPlan)
  extends LeafNode {

  override def output: Seq[Attribute] = alreadyPlanned.output
}

case class ExchangeCoordinator(plansForExecution: Seq[SparkPlan]) extends LeafNode {
  override def output: Seq[Attribute] = Nil

  var executed: Boolean = false

  var executedPlan: Seq[SparkPlan] = Nil

  def expand(): Seq[PipelineLogicalPlan] = {
    if (executed) {
      executedPlan.map(PipelineLogicalPlan)
    } else {
      throw new IllegalStateException("expand should not be called when executed it false.")
    }
  }

  def execute(): Unit = {
    if (!executed) {
      // We need to first submit plansForExecution and then determine how to shuffle data.
      // Finally, we create Shuffled RDDs to fetch data at the reducer side,
      // and wrap these RDDs in PhysicalRDDs (these are stored in executedPlan).

      executed = true
    }
  }
}