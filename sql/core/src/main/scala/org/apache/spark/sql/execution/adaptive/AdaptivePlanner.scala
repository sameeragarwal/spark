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

import org.apache.spark.sql.{Strategy, SQLContext}
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.{Exchange, EnsureRequirements, SparkPlan}

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
    private[this] val ensureRequirements = EnsureRequirements(sqlContext)

    def apply(plan: LogicalPlan): LogicalPlan = {
      var shouldContinue = true
      plan transformUp {
        case p if shouldContinue =>
          // At here, we assume that planner can take out the sparkPlan
          // in a ExecutedLogicalPlan. So, we can just plan the given p.
          val planned = planner.plan(p).next()
          val withDataMovements = ensureRequirements.apply(planned)
          assert(p.children.length == withDataMovements.children.length)
          val newChildren = p.children.zip(withDataMovements.children).map {
            case (logicalChild, e: Exchange) =>
              BoundaryLogicalPlan(e)
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
