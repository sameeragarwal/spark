package org.apache.spark.sql.online

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.types._

// TODO: In general, we represent the multiplicity vector as an array of long.
// But in order to pack them tight together, we should note that the number of bits
// needs to represent a single multiplicity may grow. E.g., at the input, each multiplicity
// will probably fall within 0-4, but after a two-way join, the range may grow to 0-16, so on so forth.

// TODO: The seed should be deterministic. So how to pass in the seed parameter?
case class Poisson() extends LeafExpression {
  type EvaluatedType = Any

  def dataType = ByteType
  override def foldable = false
  def nullable = false
  override def toString = s"Poisson()"

  override def eval(input: Row): Any = ???
}

object ImmutableVectorize {
  val numBootstrapTrials = 100
}

case class ImmutableVectorize(
    evaluate: Expression,
    vectorChildren: Seq[Expression],
    scalarChildren: Seq[Expression])
  extends Expression {
  import ImmutableVectorize._

  override type EvaluatedType = Any

  @transient private[this] val vectors = vectorChildren.toArray
  @transient private[this] val v = new Array[Array[Any]](vectors.length)
  @transient private[this] val scalars = scalarChildren.toArray
  @transient private[this] val mutableRow: MutableRow = new GenericMutableRow(vectorChildren.length + scalarChildren.length)
  @transient private[this] val output = new Array[Any](numBootstrapTrials)

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: Row): Any = {
    var i: Int = 0
    while (i < scalars.length) {
      mutableRow(vectorChildren.length + i) = scalars(i).eval(input)
      i += 1
    }

    i = 0
    while (i < vectors.length) {
      v(i) = vectors(i).eval(input).asInstanceOf[Array[Any]]
    }

    i= 0
    while (i < numBootstrapTrials) {
      var j = 0
      while (j < v.length) {
        mutableRow(j) = v(j)(i)
        j += 1
      }
      output(i) = evaluate.eval(mutableRow)
      i += 1
    }
    output
  }

  override def nullable: Boolean = evaluate.nullable

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = ArrayType(evaluate.dataType, evaluate.nullable)

  override def children = vectorChildren ++ scalarChildren

  override def toString = s"Vec(${vectorChildren.mkString("[",",","]")}, ${scalarChildren.mkString("[",",","]")}, $evaluate)"
}

case class VectorSum(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  /**
   * Creates a new instance that can be used to compute this aggregate expression for a group
   * of input rows/
   */
  override def newInstance(): AggregateFunction = ???

  override def nullable: Boolean = false

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = child.dataType

  /**
   * Returns a [[SplitEvaluation]] that computes this aggregation using partial aggregation.
   */
  override def asPartial: SplitEvaluation = ???

  override def toString = s"VSUM($child)"
}

case class VectorSum01(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  /**
   * Creates a new instance that can be used to compute this aggregate expression for a group
   * of input rows/
   */
  override def newInstance(): AggregateFunction = ???

  override def nullable: Boolean = false

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = child.dataType

  /**
   * Returns a [[SplitEvaluation]] that computes this aggregation using partial aggregation.
   */
  override def asPartial: SplitEvaluation = ???

  override def toString = s"VSUM01($child)"
}

case class Exists(child: Expression) extends UnaryExpression with Predicate {
  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: Row): Any = ??? // TODO: implement similar to ImmutableVectorize

  override def foldable = child.foldable
  override def nullable: Boolean = child.nullable

  override def toString = s"Exists($child)"
}

case class ForAll(child: Expression) extends UnaryExpression with Predicate {
  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: Row): Any = ???

  override def foldable = child.foldable
  override def nullable: Boolean = child.nullable

  override def toString = s"ForAll($child)"
}