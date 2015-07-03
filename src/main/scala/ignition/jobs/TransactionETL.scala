package ignition.jobs

import ignition.chaordic.pojo.Transaction
import ignition.jobs.utils.DashboardAPI.ResultPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class ETLResult(salesSearch: RDD[ResultPoint], salesOverall: RDD[ResultPoint])


object TransactionETL extends SearchETL {

  type MetricByDate = ((String, String), Double)

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

    def cashByDay: MetricByDate = ((transaction.apiKey, aggregationKey), cashFromTransaction)

    def isSearchTransaction = transaction.info.contains("cssearch")
  }

  /**
   * Calculate the monetary value given by all transactions that search was involved aggregated by day.
   * @param transactions All transactions of a client.
   * @return RDD of (date, cash)
   */
  def calculateSearchSales(transactions: RDD[Transaction]): RDD[MetricByDate] =
    transactions
      .filter(_.isSearchTransaction)
      .map(_.cashByDay)
      .reduceByKey(_ + _)

  /**
   * Calculate the monetary value given by all transactions aggregated by day.
   * @param transactions All transactions of a client.
   * @return RDD of (date, cash)
   */
  def calculateOverallSales(transactions: RDD[Transaction]): RDD[MetricByDate] =
    transactions
      .map(_.cashByDay)
      .reduceByKey(_ + _)

  def process(sc: SparkContext, transactions: RDD[Transaction]): ETLResult = {
    val salesSearch = calculateSearchSales(transactions).map {
      case ((client: String, day: String), value: Double) => ResultPoint(client, day, value)
    }
    val salesOverall = calculateOverallSales(transactions).map {
      case ((client: String, day: String), value: Double) => ResultPoint(client, day, value)
    }

    ETLResult(salesSearch, salesOverall)
  }

}
