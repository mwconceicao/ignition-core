package ignition.jobs


import ignition.chaordic.pojo.Transaction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json.DefaultJsonProtocol


case class ResultPoint(day: String, client: String, value: Double)
case class ETLResult(searchRevenue: RDD[ResultPoint], participationRatio: RDD[ResultPoint])

object TransactionETLProtocol extends DefaultJsonProtocol {
  implicit val resultPointFormat = jsonFormat3(ResultPoint)
}


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

  /**
   * Calculate the monetary value given bt all transactions that search was involved aggregated by day.
   * @param transactions All transactions of a client.
   * @return RDD of (date, cash)
   */
  def calculateSearchTransactions(transactions: RDD[Transaction]): RDD[MetricByDate] =
    transactions
      .filter(_.isSearchTransaction)
      .map(_.cashByDay)
      .reduceByKey(_ + _)

  /**
   * Calculate the monetary value given bt all transactions that search was *NOT* involved aggregated by day.
   * @param transactions All transactions of a client.
   * @return RDD of (date, cash)
   */
  def calculateNonSearchTransactions(transactions: RDD[Transaction]): RDD[MetricByDate] =
    transactions
      .filter(!_.isSearchTransaction)
      .map(_.cashByDay)
      .reduceByKey(_ + _)

  /**
   * Given a joint RDD of Search and Non Search values calculate searchValue / (searchValue + nonSearchValue)
   * @param joinedTransactions (date, searchCash, nonSearchCash)
   * @return The participation Ratio
   */

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
