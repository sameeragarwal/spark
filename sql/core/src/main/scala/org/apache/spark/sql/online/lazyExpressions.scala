package org.apache.spark.sql.online

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.types.{DoubleType, DataType}

trait Tag {
  def symbol: String
  def simpleString = ""
}

trait TransformableTag extends Tag {
  def toTag: Tag
}

case class Bootstrap(scaleExponent: Int) extends TransformableTag {
  val symbol = ":"
  def toTag = this
}

case object Uncertain extends TransformableTag {
  val symbol = "~"
  def toTag = this
}

case object Flag extends TransformableTag {
  val symbol = ":"
  def toTag = this
}

trait GenerateLazyEvaluate {
  def lineage: Seq[NamedExpression]
  def lazyEval: Expression
}

trait LazyEvaluate {
  def lazyEval: Expression
  def lineage: Seq[Attribute] = lazyEval.references.toSeq
}

case class LazyAttribute(lazyEval: Expression) extends Tag with LazyEvaluate {
  val symbol = "`"
  override def simpleString = s" = $lazyEval"
}

case class LazyAggregate(lineage: Seq[NamedExpression], dataType: DataType, nullable: Boolean)
  extends TransformableTag with GenerateLazyEvaluate {
  val symbol = "`"
  def toTag = LazyAttribute(lazyEval)
  override def simpleString = s" = (${lineage.mkString("[",",","]")}, $lazyEval)"

  val lazyEval = Lookup(dataType, nullable, lineage.map(_.toAttribute))
}

case class LazyAlias(child: Expression) extends TransformableTag with GenerateLazyEvaluate {
  val symbol = "`"

  def toTag = LazyAttribute(lazyEval)

  override def simpleString = s" = (${lineage.mkString("[", ",", "]")}, $lazyEval)"

  val (lineage, lazyEval): (Seq[NamedExpression], Expression) = {
    import scala.collection.mutable.ArrayBuffer

    val certain = new ArrayBuffer[Expression]()
    val lineage = new ArrayBuffer[NamedExpression]()

    def isCertain(e: Expression) = certain.exists(_.fastEquals(e))

    // 1. Replace lazy evaluates
    // 2. mark certain nodes
    val replaced = child transformUp {
      case TaggedAttribute(lazyEval: LazyAttribute, _, _, _, _) =>
        lineage ++= lazyEval.lineage
        lazyEval.lazyEval
      case btCnt@TaggedAttribute(_: Bootstrap, _, _, _, _) =>
        lineage += btCnt
        btCnt
      case expr =>
        if (expr.children.forall(isCertain)) {
          certain += expr
        }
        expr
    }

    val lazyEval = replaced transformDown {
      case named: NamedExpression if isCertain(named) =>
        lineage += named
        named.toAttribute
      case literal: Literal => literal
      case expr if isCertain(expr) =>
        val named = Alias(expr, "_lineage")()
        lineage += named
        named.toAttribute
    }

    val optimized = lazyEval.transformUp {
      // Collapse vectorize
      case DevectorizePlaceholder(VectorizePlaceholder(expr), dataType, nullable)
        if expr.dataType == dataType && expr.nullable == nullable =>
        expr
    }.transformDown {
      // Collapse guard
      case guard: GuardPlaceholder =>
        guard transformChildrenDown {
          case GuardPlaceholder(_, trueVal, _) => trueVal
        }
    }

    val references = optimized.references.map(_.exprId).toSet
    val optLineage = lineage.filter(named => references.contains(named.exprId)).distinct.toSeq

    (optLineage, optimized)
  }
}

case class TaggedAttribute(
    tag: Tag,
    name: String,
    dataType: DataType,
    nullable: Boolean = true)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifiers: Seq[String] = Nil) extends Attribute with trees.LeafNode[Expression] {

  override def equals(other: Any) = other match {
    case ar: TaggedAttribute => tag == ar.tag && name == ar.name && exprId == ar.exprId && dataType == ar.dataType
    case _ => false
  }

  override def hashCode: Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + exprId.hashCode()
    h = h * 37 + dataType.hashCode()
    h
  }

  override def newInstance() =
    TaggedAttribute(tag, name, dataType, nullable)(qualifiers = qualifiers)

  /**
   * Returns a copy of this [[TaggedAttribute]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): TaggedAttribute = {
    if (nullable == newNullability) {
      this
    } else {
      TaggedAttribute(tag, name, dataType, newNullability)(exprId, qualifiers)
    }
  }

  /**
   * Returns a copy of this [[TaggedAttribute]] with new qualifiers.
   */
  override def withQualifiers(newQualifiers: Seq[String]) = {
    if (newQualifiers.toSet == qualifiers.toSet) {
      this
    } else {
      TaggedAttribute(tag, name, dataType, nullable)(exprId, newQualifiers)
    }
  }

  // Unresolved attributes are transient at compile time and don't get evaluated during execution.
  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString: String = s"${tag.symbol}$name#${exprId.id}$typeSuffix${tag.simpleString}"
}

case class TaggedAlias(tag: TransformableTag, child: Expression, name: String)
    (val exprId: ExprId = NamedExpression.newExprId, val qualifiers: Seq[String] = Nil)
  extends NamedExpression with trees.UnaryNode[Expression] {

  override type EvaluatedType = Any

  override def eval(input: Row) = child.eval(input)

  override def dataType = child.dataType
  override def nullable = child.nullable

  override def toAttribute = {
    if (resolved) {
      TaggedAttribute(
        tag.toTag, name, child.dataType, child.nullable)(exprId, qualifiers)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String =
    s"$child${tag.simpleString} AS ${tag.symbol}$name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs = exprId :: qualifiers :: Nil
}

// TODO
case class Lookup(dataType: DataType, nullable: Boolean, children: Seq[Expression]) extends Expression {
  type EvaluatedType = Any

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: Row): EvaluatedType = ??? // TODO

  override def toString = s"Lookup(${children.mkString("[",",","]")}):$dataType"
}

case class ScaleFactor(scale: Double, exponent: Int) extends LeafExpression {
  override def foldable = true
  def nullable = false
  def dataType = DoubleType

  val value = Double.box(scala.math.pow(scale, exponent))

  override def toString = s"$scale^$exponent"

  type EvaluatedType = Any
  override def eval(input: Row):Any = value
}