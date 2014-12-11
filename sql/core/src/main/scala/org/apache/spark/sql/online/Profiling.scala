package org.apache.spark.sql.online

import java.util.{HashMap => JHashMap}

import scala.util.Random

import org.apache.spark.rdd.{ImmutableHashIndexedRDDPartition, PatriciaTreeIndexedRDDPartition}
import org.apache.spark.sql.catalyst.util.benchmark

object Profiling {
  val uri1: String = "C:\\Dropbox\\Public\\zk1"
  val uri2: String = "C:\\Dropbox\\Public\\zk2"

  def run(): Unit = {
    Profiling.batchConstruction()

    println()
    Profiling.pointLookup()

    println()
    Profiling.batchMerge()

    println()
    Profiling.serde()
  }

  def load(uri: String): Iterator[(Long, Vector[Int])] = {
    import scala.io.Source

    Source.fromFile(uri).getLines().map(_.split('\t')).map { segments =>
      (segments(0).toLong, segments(1).split(',').map(_.toInt).toVector)
    }
  }

  def toJavaMap(iter: Iterator[(Long, Vector[Int])]) = {
    val map = new JHashMap[Long, Vector[Int]]
    iter.foreach(e => map.put(e._1, e._2))
    map
  }

  def batchConstruction(): Unit = {
    val data = load(uri1).toArray

    println("Batch Construction:")
    (1 to 5).foreach { i =>
      println(s"==========ITERATION $i===============")

      benchmark {
        print("Java HashMap: ")
        toJavaMap(data.iterator)
      }

      benchmark {
        print("Scala HashMap: ")
        data.iterator.toMap
      }

      benchmark {
        print("Immutable HashMap IndexedRDD: ")
        ImmutableHashIndexedRDDPartition(data.iterator)
      }

      benchmark {
        print("Patricia Tree IndexedRDD: ")
        PatriciaTreeIndexedRDDPartition(data.iterator)
      }
    }
  }

  def pointLookup(): Unit = {
    val javaMap = toJavaMap(load(uri1))
    val scalaMap = load(uri1).toMap
    val immutableRDD = ImmutableHashIndexedRDDPartition(load(uri1))
    val patriciaRDD = PatriciaTreeIndexedRDDPartition(load(uri1))

    val keys = Random.shuffle(scalaMap.keys)

    println("Point Lookup:")
    (1 to 5).foreach { i =>
      println(s"==========ITERATION $i===============")

      benchmark {
        print("Java HashMap: ")
        keys.foreach { key =>
          javaMap.get(key)
        }
      }

      benchmark {
        print("Scala HashMap: ")
        keys.foreach { key =>
          scalaMap(key)
        }
      }

      benchmark {
        print("Immutable HashMap IndexedRDD: ")
        keys.foreach { key =>
          immutableRDD(key)
        }
      }

      benchmark {
        print("Patricia Tree IndexedRDD: ")
        keys.foreach { key =>
          patriciaRDD(key)
        }
      }
    }
  }

  def serde(): Unit = {
    val javaMap = toJavaMap(load(uri1))
    val scalaMap = load(uri1).toMap
    val immutableRDD = ImmutableHashIndexedRDDPartition(load(uri1))
    val patriciaRDD = PatriciaTreeIndexedRDDPartition(load(uri1))

    def toBytes(obj: Any): Array[Byte] = {
      import java.io.{ByteArrayOutputStream, ObjectOutputStream}

      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(obj)
      baos.toByteArray
    }

    def fromBytes(bytes: Array[Byte]): Any = {
      import java.io.{ByteArrayInputStream, ObjectInputStream}

      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      ois.readObject()
    }

    println("Serialization:")
    (1 to 5).foreach { i =>
      println(s"==========ITERATION $i===============")

      benchmark {
        print(s"Java HashMap: serializing ${toBytes(javaMap).length} bytes took ")
      }

      benchmark {
        print(s"Scala HashMap: serializing ${toBytes(scalaMap).length} bytes took ")
      }

      benchmark {
        print(s"Immutable HashMap IndexedRDD: serializing ${toBytes(immutableRDD).length} bytes took ")
      }

      benchmark {
        print(s"Patricia Tree IndexedRDD: serializing ${toBytes(patriciaRDD).length} bytes took ")
      }
    }
    println()

    val bytesOfJavaMap = toBytes(javaMap)
    val bytesOfScalaMap = toBytes(scalaMap)
    val bytesOfImmutableRDD = toBytes(immutableRDD)
    val bytesOfPatriciaRDD = toBytes(patriciaRDD)

    println("Deserialization:")
    (1 to 5).foreach { i =>
      println(s"==========ITERATION $i===============")

      benchmark {
        print("Java HashMap: ")
        fromBytes(bytesOfJavaMap)
      }

      benchmark {
        print("Scala HashMap: ")
        fromBytes(bytesOfScalaMap)
      }

      benchmark {
        print("Immutable HashMap IndexedRDD: ")
        fromBytes(bytesOfImmutableRDD)
      }

      benchmark {
        print("Patricia Tree IndexedRDD: ")
        fromBytes(bytesOfPatriciaRDD)
      }
    }
  }

  def batchMerge(): Unit = {
    val javaMap1 = toJavaMap(load(uri1))
    val javaMap2 = toJavaMap(load(uri2))

    val scalaMap1 = load(uri1).toMap
    val scalaMap2 = load(uri2).toMap

    //    val immutableRDD1 = ImmutableHashIndexedRDDPartition(load(uri1))
    //    val immutableRDD2 = ImmutableHashIndexedRDDPartition(load(uri2))

    val patriciaRDD1 = PatriciaTreeIndexedRDDPartition(load(uri1))
    val patriciaRDD2 = PatriciaTreeIndexedRDDPartition(load(uri2))

    println("Batch Merge:")
    (1 to 5).foreach { i =>
      println(s"==========ITERATION $i===============")

      benchmark {
        print("Java HashMap: ")
        val map = javaMap1.clone().asInstanceOf[JHashMap[Long, Vector[Int]]]
        val iter = javaMap2.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          map.put(entry.getKey, entry.getValue)
        }
      }

      benchmark {
        print("Scala HashMap: ")
        scalaMap1 ++ scalaMap2
      }

      //      benchmark {
      //        print("Immutable HashMap IndexedRDD: ")
      //        immutableRDD1.multiput(immutableRDD2.iterator, (id, vec1, vec2) => vec1 ++ vec2)
      //      }

      benchmark {
        print("Patricia Tree IndexedRDD: ")
        patriciaRDD1.multiput(patriciaRDD2.iterator, (id, vec1, vec2) => vec1 ++ vec2)
      }
    }
  }
}
