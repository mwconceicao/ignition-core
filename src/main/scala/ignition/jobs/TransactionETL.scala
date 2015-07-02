package ignition.jobs


import ignition.chaordic.pojo.Transaction
import ignition.jobs.TransactionETL.MetricByDate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json.DefaultJsonProtocol

case class ResultPoint(client: String, day: String, value: Double)
case class ETLResult(salesSearch: RDD[ResultPoint], salesOverall: RDD[ResultPoint])

object TransactionETLProtocol extends DefaultJsonProtocol {
  implicit val resultPointFormat = jsonFormat3(ResultPoint)
}

object TransactionETL {

  type MetricByDate = ((String, String), Double)
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

    def cashByDay: MetricByDate = ((transaction.apiKey, aggregationKey), cashFromTransaction)

    def isSearchTransaction = transaction.info.contains("cssearch")
  }

  /**
   * Calculate the monetary value given bt all transactions that search was involved aggregated by day.
   * @param transactions All transactions of a client.
   * @return RDD of (date, cash)
   */
  def calculateSearchSales(transactions: RDD[Transaction]): RDD[MetricByDate] =
    transactions
      .filter(_.isSearchTransaction)
      .map(_.cashByDay)
      .reduceByKey(_ + _)

  /**
   * Calculate the monetary value given bt all transactions aggregated by day.
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
