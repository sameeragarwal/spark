package org.apache.spark.sql.online

import java.util.{HashMap => JHashMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

case class OnlineAggregate(
    flag: Option[Attribute],
    lazyEvals: Option[Seq[Expression]],
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(
    val batchId: Int = 0,
    val opId: OpId = StatefulOperator.newOpId)
  extends UnaryNode with Stateful {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  private[this] val childOutput = child.output

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(
      unbound: AggregateExpression,
      aggregate: AggregateExpression,
      resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, childOutput),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction] = {
    val buffer = new Array[AggregateFunction](computedAggregates.length)
    var i = 0
    while (i < computedAggregates.length) {
      buffer(i) = computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  def cached(rdd: RDD[Row], flag: Option[Attribute]): RDD[Row] = flag match {
    case Some(f) =>
      rdd.mapPartitionsWithIndex { (index, iter) =>
        val predicate = InterpretedPredicate(f, child.output)
        new Iterator[Row] {
          override final def hasNext: Boolean = iter.hasNext
          override final def next(): Row = {
            val row = iter.next()
            if (predicate(row)) {
              // TODO: row.copy()
            }
            row
          }
        }
      }
    case None =>
      rdd.mapPartitions { iter =>
        new Iterator[Row] {
          override final def hasNext: Boolean = iter.hasNext
          override final def next(): Row = {
            val row = iter.next()
            // TODO: row.copy()
            row
          }
        }
      }
  }

  def refresh(rdd: RDD[Row], lazyEvals: Option[Seq[Expression]]): RDD[Row] = lazyEvals match {
    case Some(projectList) =>
      rdd.mapPartitions { iter =>
        // There should be very few tuples need to be refreshed.
        // No need to use generated codes.
        val projection = new InterpretedMutableProjection(projectList, child.output)
        iter.map(projection)
      }
    case None => rdd
  }

  override def execute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = 0
        while (i < buffer.length) {
          aggregateResults(i) = buffer(i).eval(EmptyRow)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new JHashMap[Row, Array[AggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[Row] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow4

          override final def hasNext: Boolean = hashTableIter.hasNext

          override final def next(): Row = {
            val currentEntry = hashTableIter.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = 0
            while (i < currentBuffer.length) {
              // Evaluating an aggregate buffer returns the result.  No row is required since we
              // already added all rows in the group using update.
              aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }

  type StateType = Row

  def withNewBatchId(bId: Int) =
    OnlineAggregate(flag, lazyEvals, partial, groupingExpressions, aggregateExpressions, child)(bId, opId)

  override protected final def otherCopyArgs = opId :: Nil
}