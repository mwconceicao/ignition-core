package ignition.core.jobs.utils

import scala.reflect._
import org.apache.spark.rdd.{PairRDDFunctions, CoGroupedRDD, RDD}
import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scalaz.{Success, Validation}

object RDDUtils {
  //TODO: try to make it work for any collection
  implicit class OptionRDDImprovements[V: ClassTag](rdd: RDD[Option[V]]) {
    def flatten: RDD[V] = {
      rdd.flatMap(x => x)
    }
  }

  implicit class SeqRDDImprovements[V: ClassTag](rdd: RDD[Seq[V]]) {
    def flatten: RDD[V] = {
      rdd.flatMap(x => x)
    }
  }

  implicit class ValidatedRDDImprovements[A: ClassTag, B: ClassTag](rdd: RDD[Validation[A, B]]) {

    def mapSuccess(f: B => Validation[A, B]): RDD[Validation[A, B]] = {
      rdd.map({
        case Success(v) => f(v)
        case failure => failure
      })
    }
  }

  implicit class ValidatedPairRDDImprovements[A: ClassTag, B: ClassTag, K: ClassTag](rdd: RDD[(K, Validation[A, B])]) {

    def mapValuesSuccess(f: B => Validation[A, B]): RDD[(K, Validation[A, B])] = {
      rdd.mapValues({
        case Success(v) => f(v)
        case failure => failure
      })
    }
  }

  implicit class RDDImprovements[V: ClassTag](rdd: RDD[V]) {
    def incrementCounter(acc: spark.Accumulator[Int]): RDD[V] = {
      rdd.map(x => { acc += 1; x })
    }

    def incrementCounterIf(cond: (V) => Boolean, acc: spark.Accumulator[Int]): RDD[V] = {
      rdd.map(x => { if (cond(x)) acc += 1; x })
    }
  }

  implicit class PairRDDImprovements[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def incrementCounter(acc: spark.Accumulator[Int]): RDD[(K, V)] = {
      rdd.mapValues(x => { acc += 1; x })
    }

    def incrementCounterIf(cond: (K, V) => Boolean, acc: spark.Accumulator[Int]): RDD[(K, V)] = {
      rdd.mapValuesWithKeys(x => { if(cond(x._1, x._2)) acc += 1; x._2 })
    }

    def flatMapValuesWithKeys[U: ClassTag](f: ((K, V)) => Seq[U]): RDD[(K, U)] = {
      rdd.mapPartitions[(K, U)](kvs => {
        kvs.flatMap[(K,U)](kv => Stream.continually(kv._1) zip f(kv))
      }, preservesPartitioning = true)
    }

    def mapValuesWithKeys[U: ClassTag](f: ((K, V)) => U): RDD[(K, U)] = {
      rdd.mapPartitions[(K, U)](kvs => {
        kvs.map[(K,U)](kv => (kv._1, f(kv)))
      }, preservesPartitioning = true)
    }

    // Adapted from source code to add more than 3 RDD groups:
    // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala
    // Changed the name because of implicit conversion issues
    // TODO: revise this code on new versions of Spark

    def cogroup4[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)], partitioner: Partitioner)
    : RDD[(K, (Seq[V], Seq[W1], Seq[W2], Seq[W3]))] = {
      val cg = new CoGroupedRDD[K](Seq(rdd, other1, other2, other3), partitioner)
      val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(classTag[K], classTag[Seq[Seq[_]]])
      prfs.mapValues { case Seq(vs, w1s, w2s, w3s) =>
        (vs.asInstanceOf[Seq[V]], w1s.asInstanceOf[Seq[W1]], w2s.asInstanceOf[Seq[W2]], w3s.asInstanceOf[Seq[W3]])
      }
    }

    def cogroup5[W1, W2, W3, W4](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)], other4: RDD[(K, W4)], partitioner: Partitioner)
    : RDD[(K, (Seq[V], Seq[W1], Seq[W2], Seq[W3], Seq[W4]))] = {
      val cg = new CoGroupedRDD[K](Seq(rdd, other1, other2, other3, other4), partitioner)
      val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(classTag[K], classTag[Seq[Seq[_]]])
      prfs.mapValues { case Seq(vs, w1s, w2s, w3s, w4s) =>
        (vs.asInstanceOf[Seq[V]], w1s.asInstanceOf[Seq[W1]], w2s.asInstanceOf[Seq[W2]], w3s.asInstanceOf[Seq[W3]], w3s.asInstanceOf[Seq[W4]])
      }
    }

    
  }
}
