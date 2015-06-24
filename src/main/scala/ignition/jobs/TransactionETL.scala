package ignition.jobs


import ignition.chaordic.pojo.Transaction
import ignition.jobs.AggregationLevel.AggregationValue
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object AggregationLevel extends Enumeration {

  sealed trait AggregationValue {
    def value: Double
}


  case object SECONDS extends AggregationValue { val value = 1000.0 }
  case object MINUTES extends AggregationValue { val value = 60 * SECONDS.value }
  case object HOURS extends AggregationValue { val value = 60 * MINUTES.value }
  case object DAYS extends AggregationValue { val value = 24 * HOURS.value }
  case object WEEKS extends AggregationValue { val value = 7 * DAYS.value }

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

  def aggregationKey(implicit aggregationLevel: AggregationValue) = transaction.date.getMillis / aggregationLevel.value

  def isSearchTransaction = transaction.info.contains("cssearch")
}


object TransactionETL {

  def process(sc: SparkContext,
              transactions: RDD[Transaction])(implicit aggregationLevel: AggregationValue) : Unit = {

    val etlTransactions: RDD[ETLTransaction] =
      transactions
      .map(transaction => ETLTransaction(transaction))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val searchTransactions: RDD[(Double, Double)] =
      etlTransactions
        .filter(_.isSearchTransaction)
        .map(etlTransaction => (etlTransaction.aggregationKey(aggregationLevel), etlTransaction.cashFromTransaction))
        .reduceByKey(_ + _)

    val nonSearchTransactions: RDD[(Double, Double)] =
      etlTransactions
        .filter(!_.isSearchTransaction)
        .map(etlTransaction => (etlTransaction.aggregationKey(aggregationLevel), etlTransaction.cashFromTransaction))
        .reduceByKey(_ + _)

    val groupedTransactions: RDD[(Double, (Double, Double))] =
      searchTransactions.join(nonSearchTransactions)

    val participation: RDD[(Double, Double)] =
      groupedTransactions.map {
        case (key: Double, (searchValue: Double, nonSearchValue: Double)) =>
          (key, searchValue / (searchValue + nonSearchValue))
      }

    searchTransactions.saveAsTextFile("/tmp/test/vendas_captadas")
    nonSearchTransactions.saveAsTextFile("/tmp/test/nonSearch")
    participation.saveAsTextFile("/tmp/test/participacao_vendas")

  }

}
