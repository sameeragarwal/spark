package org.apache.spark.sql.online

import java.io.File

import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.sql.catalyst.expressions.{Row, RowOrdering}
import org.apache.spark.sql.catalyst.util.benchmark
import org.apache.spark.sql.columnar.InMemoryColumnarTableScan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{BuildLeft, ShuffledHashJoin}
import org.apache.spark.sql.online.joins.OnlineShuffledHashJoin
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.{SQLConf, SchemaRDD}

import scala.collection.mutable.ArrayBuffer

case class LineItem(
    l_orderkey: Int = 0,
    l_partkey: Int = 0,
    l_suppkey: Int = 0,
    l_linenumber: Int = 0,
    l_quantity: Int = 0,
    l_extendedprice: Double = 0,
    l_discount: Double = 0,
    l_tax: Double = 0,
    l_returnflag: String = "",
    l_shipdate: String = "",
    l_commitdate: String = "",
    l_receiptdate: String = "",
    l_shipinstruct: String = "",
    l_shipmode: String = "",
    l_comment: String = "")

case class Part(
    p_partkey: Int = 0,
    p_name: String = "",
    p_mgfr: String = "",
    p_brand: String = "",
    p_type: String = "",
    p_size: Int = 0,
    p_container: String = "",
    p_retailprice: Double = 0,
    p_comment: String = "")


object TupleMemory {
  val rememberedResults = new scala.collection.mutable.HashMap[Int, ArrayBuffer[Row]]
}

case class TupleMemory(batchId: Int, includeBatches: Seq[Int], child: SparkPlan) extends SparkPlan {

  def output = child.output
  def children = child :: Nil

  def execute() = {
    child.execute().mapPartitions { iter =>
      val rememberedOutput = new ArrayBuffer[Row]
      TupleMemory.rememberedResults(batchId) = rememberedOutput

      val rememberingIterator = iter.map { row =>
        rememberedOutput += row
        row
      }

      includeBatches.flatMap(i => TupleMemory.rememberedResults(i)).iterator ++ rememberingIterator
    }
  }
}

object OnlineSQL {
  import org.apache.spark.sql.test.TestSQLContext._
  import org.apache.spark.sql.online.Growth._

  /**
   * Transforms the given query so that it only operates on a single partition of data, specified by
   * `batchId`.  The correctness of the answer is predicated on all previous batches having been
   * already run and their results memorized.
   */
  def withMemory(query: SchemaRDD, batchId: Int): SparkPlan = query.queryExecution.executedPlan transformUp {
    case a: Aggregate if a.partial =>
      TupleMemory(batchId, 0 until batchId, a)

    case data: InMemoryColumnarTableScan =>
      ExistingRdd(
        data.output,
        PartitionPruningRDD.create(data.execute(), _ == batchId))

    case ShuffledHashJoin(leftKeys, rightKeys, BuildLeft, left, right) =>
      OnlineShuffledHashJoin(leftKeys, rightKeys, BuildLeft, GrowthMode(AlmostFixed, Fixed), GrowthMode(Fixed, Fixed), 0, left, right)()
  }

  /**
   * Transforms a query to execute on partitions 0-`upTo`.  The correct answer is returned when
   * upTo = |partitions|.
   */
  def prunedQuery(query: SchemaRDD, upTo: Int) = query.queryExecution.executedPlan transformUp {
    case data: InMemoryColumnarTableScan =>
      ExistingRdd(
        data.output,
        PartitionPruningRDD.create(data.execute(), _ <= upTo))
  }

  def main(args: Array[String]): Unit = {
//    test1()
//    test2()
    Profiling.run()
  }

  def test1() = {
    setConf(SQLConf.SHUFFLE_PARTITIONS, "5")

    val lineFile = new File("lineitem.tbl")
    val partFile = new File("part.tbl")

    if (!lineFile.exists()) {
      println("Generating line items")
      sparkContext.parallelize(1 to 10, 10)
        .flatMap(i => 1 to 50000)
        .map(i => LineItem(l_partkey = i % 50, l_quantity = i % 10 + i))
        .saveAsParquetFile(lineFile.getCanonicalPath)
    }

    if (!partFile.exists()) {
      println("Generating parts")
      sparkContext.parallelize(1 to 50)
        .map(i => Part(p_partkey = i, p_brand = (i % 2).toString))
        .limit(50) // Force broadcast
        .saveAsParquetFile(partFile.getCanonicalPath)
    }

    parquetFile(lineFile.getCanonicalPath).registerTempTable("lineitem")
    parquetFile(partFile.getCanonicalPath).registerTempTable("part")

    benchmark {
      println("Rows: " + table("lineitem").count())
    }
    benchmark {
      println("Rows: " + table("part").count())
    }

//    val fullQuery = sql(
//      s"""
//             |select sum(price)
//             |from (select l_partkey as pkey, l_extendedprice as price, l_quantity as quantity from lineitem JOIN part where p_partkey = l_partkey and p_brand = "0") as a
//             |  JOIN (select l_partkey as key, avg(l_quantity) as avgQty from lineitem group by l_partkey) as b
//             |where pkey = key
//             |  and quantity < avgQty
//           """.stripMargin)

    val fullQuery = sql(
      s"""
         |select *
         |from lineitem JOIN part
         |where p_partkey = l_partkey
         |  and p_brand = "0"
       """.stripMargin)

//    val fullQuery = sql(
//      s"""
//         |select *
//         |from lineitem
//       """.stripMargin)

    println()
    println(fullQuery.queryExecution.executedPlan)

    println("=======================================")

    val planner = new OnlineSparkPlanner(Set("lineitem.tbl"), 10)(TestSQLContext)
    val planned = planner(fullQuery.queryExecution.executedPlan)

    println()
    println(planned)

//    (0 to 9).foldLeft(planned) { (p, batchId) =>
//      println(s"================ITERATION $batchId======================")
//      println()
//      val ret = BatchPlanner.generate(p, batchId)
//      println(ret)
//      ret
//    }

    (1 to 5).foreach { i =>
      println(s"== ITERATION $i ==")

      print("Full Query: ")
      benchmark {
        fullQuery.collect()
      }
      println()

      benchmark {
        (0 to 9).foldLeft(planned) { (p, batchId) =>
          print(s"Only batch $batchId, with memory: ")
          val ret = BatchPlanner.generate(p, batchId)
          benchmark {
            ret.execute().map(_.copy()).collect()
          }
          ret
        }

        print("Total time: ")
      }
      println()
    }
  }

  def test2() = {
    setConf(SQLConf.SHUFFLE_PARTITIONS, "5")
    setConf("spark.sql.inMemoryColumnarStorage.batchSize", "100000")

    sparkContext.parallelize(1 to 10, 10)
      .flatMap(i => 1 to 300)
      .map(i => LineItem(l_partkey = i % 50, l_quantity = i % 10 + i))
      .registerTempTable("lineitem")

    sparkContext.parallelize(1 to 50)
      .map(i => Part(p_partkey = i, p_brand = (i % 2).toString))
      .limit(50) // Force broadcast
      .registerTempTable("part")

    println("Caching data...")
    cacheTable("lineitem")
    benchmark {
      println("Rows: " + table("lineitem").count())
    }

//    val aggQuery: SchemaRDD = sql(
//      s"""
//         |select sum(price)
//         |from (select l_partkey as pkey, l_extendedprice as price, l_quantity as quantity from lineitem JOIN part where p_partkey = l_partkey and p_brand = "0") as a
//         |  JOIN (select l_partkey as key, avg(l_quantity) as avgQty from lineitem group by l_partkey) as b
//         |where pkey = key
//         |  and quantity < avgQty
//       """.stripMargin)

//    val aggQuery: SchemaRDD = sql(
//      s"""
//         |select l_linenumber, quantity
//         |from (select l_linenumber as number, l_partkey as pkey, l_extendedprice as price, l_quantity as quantity from lineitem JOIN part where p_partkey = l_partkey and p_brand = "0") as a
//         |  JOIN lineitem
//         |where pkey = l_partkey
//         |  and l_linenumber = number
//       """.stripMargin)


//        val aggQuery: SchemaRDD = sql(
//      s"""
//         |select *
//         |from (select l_partkey as pkey, l_extendedprice as price, l_quantity as quantity from lineitem JOIN part where p_partkey = l_partkey and p_brand = "0") as a
//         |  JOIN (select l_partkey as key, avg(l_quantity) as avgQty from lineitem group by l_partkey) as b
//         |where pkey = key
//         |  and quantity < avgQty
//       """.stripMargin)

//    val aggQuery: SchemaRDD = sql(
//      s"""
//         |select l_partkey as pkey, l_extendedprice as price, l_quantity as quantity from lineitem JOIN part where p_partkey = l_partkey and p_brand = "0"
//       """.stripMargin)

//      val aggQuery: SchemaRDD = sql(
//        s"""
//          |select a.l_partkey, b.l_partkey
//          |from lineitem a join lineitem b
//          |where a.l_orderkey = b.l_orderkey and a.l_linenumber = b.l_linenumber
//         """.stripMargin)

    val aggQuery: SchemaRDD = sql(
      s"""
         |select l_partkey as key, avg(l_quantity) as avgQty from lineitem group by l_partkey
       """.stripMargin)

    println(aggQuery.queryExecution.executedPlan)

    println("=====================")

    val planner = new OnlineSparkPlanner(Set("lineitem.tbl"), 10)(TestSQLContext)
    var planned = planner(aggQuery.queryExecution.executedPlan)
//    println(withMemory(aggQuery, 0))
    println(planned)

    (1 to 1).foreach { i =>
      println(s"== ITERATION $i ==")

      var gt = 0L

      print("Full Query: ")
      benchmark {
//        gt += aggQuery.queryExecution.executedPlan.execute().map(_.copy()).count()
      }
      println()

//      benchmark {
//        (0 to 9).foreach { i =>
//          print(s"Batches 0 to $i: ")
//          benchmark {
//            prunedQuery(aggQuery, i).execute().map(_.copy()).count()
//          }
//        }
//        print("TotalTime: ")
//      }
//      println()

      benchmark {
        (0 to 9).foreach { i =>
          print(s"Only batch $i, with memory: ")
          planned = BatchPlanner.generate(planned, i)
          benchmark {
//            withMemory(aggQuery, i).execute().map(_.copy()).collect()
            gt -= planned.execute().map(_.copy()).count()
          }
        }
        print("Total time: ")
      }

      println("Checking results... gt = " + gt)
//      assert(gt == 0)

      println()
    }

    implicit val ordering = new RowOrdering('a.int.at(0).asc :: Nil)

    //    println("== ORIG ==")
    //    aggQuery.collect().sorted.foreach(println)
    //
    //    println("== PRUNED, all ==")
    //    prunedQuery(aggQuery, 9).execute().map(_.copy()).collect().sorted.foreach(println)
    //
    //    println("==  WITH MEM  ==")
    //    withMemory(aggQuery, 9).execute().map(_.copy()).collect().sorted.foreach(println)
    //
//    println("Checking results...")
//
//    assert(prunedQuery(aggQuery, 9).execute().map(_.copy()).collect().toSet == aggQuery.collect().toSet,
//      "Pruned answers were wrong :(")
//
//    assert(withMemory(aggQuery, 9).execute().map(_.copy()).collect().toSet == aggQuery.collect().toSet,
//      "Online answers were wrong :(")
  }
}