package ignition.jobs


import ignition.chaordic.pojo.Transaction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


case class ResultPoint(key: String, apikey: String, value: Double)
case class ETLResult(searchRevenue: RDD[ResultPoint], participationRatio: RDD[ResultPoint])

object TransactionETL {

  type MetricByDate = (String, Double)
  val aggregationLevel = "yyyy-MM-dd"

  implicit class ETLTransaction(transaction: Transaction) {
    def cashFromTransaction: Double = {
      val itemPrices: Seq[Double] = for {
        item <- transaction.items
        quantity <- item.quantity
        price <- item.product.price
      } yield quantity * price
      itemPrices.sum
    }

    def aggregationKey = transaction.date.toString(aggregationLevel)

    def cashByDay = (aggregationKey, cashFromTransaction)

    def isSearchTransaction = transaction.info.contains("cssearch")
  }

  def calculateSearchTransactions(transactions: RDD[Transaction]): RDD[MetricByDate] =
    transactions
      .filter(_.isSearchTransaction)
      .map(_.cashByDay)
      .reduceByKey(_ + _)

  def calculateNonSearchTransactions(transactions: RDD[Transaction]): RDD[MetricByDate] =
    transactions
      .filter(!_.isSearchTransaction)
      .map(_.cashByDay)
      .reduceByKey(_ + _)

  def calculateParticipation(joinedTransactions: RDD[(String, (Double, Double))]) =
    joinedTransactions.map {
      case (key: String, (searchValue: Double, nonSearchValue: Double)) =>
        (key, searchValue / (searchValue + nonSearchValue))
    }

  def process(sc: SparkContext,
              transactions: RDD[Transaction], apiKey: String): ETLResult = {

    val searchTransactions = calculateSearchTransactions(transactions)

    val nonSearchTransactions: RDD[MetricByDate] = calculateNonSearchTransactions(transactions)

    val joinedTransactions: RDD[(String, (Double, Double))] =
      searchTransactions.join(nonSearchTransactions)

    val participation: RDD[MetricByDate] = calculateParticipation(joinedTransactions)

    ETLResult(searchTransactions.map { p => ResultPoint(p._1, apiKey, p._2) },
              participation.map { p => ResultPoint(p._1, apiKey, p._2) })

  }

}
