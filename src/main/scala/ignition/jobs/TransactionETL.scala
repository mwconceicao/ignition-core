package ignition.jobs


import ignition.chaordic.pojo.Transaction
import ignition.jobs.AggregationLevel.AggregationValue
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write


object AggregationLevel {

  sealed trait AggregationValue {
    val pattern: String
  }

  case object SECONDS extends AggregationValue { val pattern = "yyyy-MM-dd'T'HH:mm:ss" }
  case object MINUTES extends AggregationValue { val pattern = "yyyy-MM-dd'T'HH:mm" }
  case object HOURS extends AggregationValue { val pattern = "yyyy-MM-dd'T'HH" }
  case object DAYS extends AggregationValue { val pattern = "yyyy-MM-dd" }

}


case class ETLTransaction(transaction: Transaction) {
  def cashFromTransaction: Double = {
    val itemPrices: Seq[Double] = for {
      item <- transaction.items
      quantity <- item.quantity
      price <- item.product.price
    } yield quantity * price
    itemPrices.sum
  }

  def aggregationKey(implicit aggregationPattern: AggregationValue): String = {
    transaction.date.toString(aggregationPattern.pattern)
  }


  def isSearchTransaction = transaction.info.contains("cssearch")
}

case class ResultPoint(key: String, value: Double)
case class ETLResult(searchRevenue: RDD[ResultPoint], participationRatio: RDD[ResultPoint])

object TransactionETL {

  def process(sc: SparkContext,
              transactions: RDD[Transaction])(implicit aggregationLevel: AggregationValue): ETLResult = {

    val etlTransactions: RDD[ETLTransaction] =
      transactions
      .map(transaction => ETLTransaction(transaction))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val searchTransactions: RDD[(String, Double)] =
      etlTransactions
        .filter(_.isSearchTransaction)
        .map(etlTransaction => (etlTransaction.aggregationKey(aggregationLevel), etlTransaction.cashFromTransaction))
        .reduceByKey(_ + _)

    val nonSearchTransactions: RDD[(String, Double)] =
      etlTransactions
        .filter(!_.isSearchTransaction)
        .map(etlTransaction => (etlTransaction.aggregationKey(aggregationLevel), etlTransaction.cashFromTransaction))
        .reduceByKey(_ + _)

    val groupedTransactions: RDD[(String, (Double, Double))] =
      searchTransactions.join(nonSearchTransactions)

    val participation: RDD[(String, Double)] =
      groupedTransactions.map {
        case (key: String, (searchValue: Double, nonSearchValue: Double)) =>
          (key, searchValue / (searchValue + nonSearchValue))
      }

    ETLResult(searchTransactions.map { p => ResultPoint(p._1, p._2) },
              participation.map { p => ResultPoint(p._1, p._2) })

  }

}
