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

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, InSet, Literal}
import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{SortBasedAggregate, TungstenAggregate}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoin, BuildLeft, BuildRight}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

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
    private[this] val prepareForExecution = sqlContext.prepareForExecution


    def apply(plan: LogicalPlan): LogicalPlan = {
      var shouldContinue = true
      plan transformUp {
        case p if shouldContinue =>
          // At here, we assume that planner can take out the sparkPlan
          // in a ExecutedLogicalPlan. So, we can just plan the given p.
          // TODO: need to reduce the number of times that we call plan.
          val planned = planner.plan(p).next()
          val executedPlan = prepareForExecution.execute(planned)
          assert(p.children.length == executedPlan.children.length)
          // TODO: this only works with join. It does not work with aggregation.
          val newChildren = p.children.zip(executedPlan.children).map {
            case (logicalChild, e: Exchange) =>
              BoundaryLogicalPlan(e)
            case (logicalChild, Sort(_, _, e: Exchange, _)) =>
              BoundaryLogicalPlan(e)
            case (logicalChild, e: BroadcastHashJoin) =>
              e.buildSide match {
                case BuildRight =>
                  val df = DataFrame(sqlContext, logicalChild.children(1))
                  val set = df.select(e.rightKeys.map(Column(_)): _*).collect()
                    .distinct.map(_.get(0)).map(Literal(_).value)
                  val condition = InSet(e.leftKeys.head, set.toSet)
                  if (!logicalChild.children.head.isInstanceOf[logical.Filter] ||
                    (logicalChild.children.head.isInstanceOf[logical.Filter] &&
                      logicalChild.children.head.asInstanceOf[logical.Filter].condition != condition)) {
                    val filterOp = logical.Filter(InSet(e.leftKeys.head, set.toSet),
                      logicalChild.children.head)
                    val childToReplace = logicalChild.children.head
                    logicalChild transformUp {
                      case i if i == childToReplace && childToReplace != filterOp =>
                        filterOp
                    }
                  } else {
                    logicalChild
                  }
                case BuildLeft =>
                  val df = DataFrame(sqlContext, logicalChild.children.head)
                  val set = df.select(e.rightKeys.map(Column(_)): _*).collect()
                    .distinct.map(_.get(0)).map(Literal(_).value)
                  val condition = InSet(e.rightKeys.head, set.toSet)
                  if (!logicalChild.children(1).isInstanceOf[logical.Filter] ||
                    (logicalChild.children(1).isInstanceOf[logical.Filter] &&
                      logicalChild.children(1).asInstanceOf[logical.Filter].condition != condition)) {
                    val filterOp = logical.Filter(InSet(e.rightKeys.head, set.toSet),
                      logicalChild.children(1))
                    val childToReplace = logicalChild.children(1)
                    logicalChild transformUp {
                      case i if i == childToReplace && childToReplace != filterOp =>
                        filterOp
                    }
                  } else {
                    logicalChild
                  }
              }
            case (logicalChild, agg: TungstenAggregate) =>
              sys.error("right now, aggregate does not work")
            case (logicalChild, agg: SortBasedAggregate) =>
              sys.error("right now, aggregate does not work")
            case (logicalChild, physicalChild) =>
              val hasExchange = physicalChild.find {
                case e: Exchange => true
                case _ => false
              }.isDefined
              assert(!hasExchange)
              logicalChild
          }

          if (newChildren.exists(_.isInstanceOf[BoundaryLogicalPlan])) {
            shouldContinue = false
          }

          p.withNewChildren(newChildren)
      }
    }
  }
}

private[sql] case class BoundaryLogicalPlan(exchange: Exchange)
  extends LeafNode {

  override def output: Seq[Attribute] = exchange.output
}
