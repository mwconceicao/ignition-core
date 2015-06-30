package ignition.jobs

import ignition.chaordic.pojo.ChaordicGenerators
import ignition.chaordic.pojo.ChaordicGenerators.TimeUnits
import ignition.core.testsupport.spark.SharedSparkContext
import ignition.core.utils.BetterTrace
import ignition.jobs.TransactionETL.ETLTransaction
import org.apache.spark.rdd.RDD
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, ShouldMatchers}

class TransactionETLTest extends FlatSpec with ShouldMatchers with SharedSparkContext
  with GeneratorDrivenPropertyChecks with BetterTrace {

  val transactionGenerator = ChaordicGenerators.transactionGenerator()
  val nonSearchTransactionsGenerator = Gen.nonEmptyListOf(transactionGenerator).map(sc.parallelize(_))
  val searchTransactionsGenerator = Gen.nonEmptyListOf(
    ChaordicGenerators.transactionGenerator(gInfo = Gen.const(Map("cssearch" -> "")))
  ).map(sc.parallelize(_))

  "ETLTransaction" should "calculate how much money it have" in {

    forAll(transactionGenerator) {
      transaction => {
        val cash = {
          for {
            item <- transaction.items
            quantity <- item.quantity
            price <- item.product.price
          } yield quantity * price
        }.sum

        transaction.cashFromTransaction should be (cash)
      }
    }

  }

  it should "sum search transactions on method calculateSearchTransactions on the same day" in {
    forAll(searchTransactionsGenerator) {
      transactions =>
        val sumOfRDD: Double = TransactionETL.calculateSearchTransactions(transactions).map(_._2).sum()

        sumOfRDD should be (transactions.map(_.cashFromTransaction).sum)
    }
  }

  it should "not sum search transactions on method calculateSearchTransactions on the same day" in {
    forAll(nonSearchTransactionsGenerator) {
      transactions =>
        val sumOfRDD = TransactionETL.calculateSearchTransactions(transactions)

        sumOfRDD.isEmpty() should be (true)
    }
  }

  it should "generate the same number of keys as we have days in our RDD" in {
    val timedTransactions = Gen.nonEmptyListOf(
      ChaordicGenerators.transactionGenerator(
        gDate = ChaordicGenerators.dateGenerator(TimeUnits.DAYS),
        gInfo = Gen.const(Map("cssearch" -> ""))
      )
    ).map(sc.parallelize(_))

    forAll(timedTransactions) {
      transactions =>
        val numberOfDays: Long = TransactionETL.calculateSearchTransactions(transactions).keys.count()

        numberOfDays should be (transactions.map(_.aggregationKey).distinct().count())
    }

  }

  it should "sum search transactions on method calculateNonSearchTransactions on the same day" in {
    forAll(nonSearchTransactionsGenerator) {
      transactions =>
        val sumOfRDD: Double = TransactionETL.calculateNonSearchTransactions(transactions).map(_._2).sum()

        sumOfRDD should be (transactions.map(_.cashFromTransaction).sum)
    }
  }

  it should "not sum search transactions on method calculateNonSearchTransactions on the same day" in {
    forAll(searchTransactionsGenerator) {
      transactions =>
        val sumOfRDD = TransactionETL.calculateNonSearchTransactions(transactions)

        sumOfRDD.isEmpty() should be (true)
    }
  }

  it should "calculate the participation" in {
    forAll(nonSearchTransactionsGenerator, searchTransactionsGenerator) {
      (nonSearchTransactions, searchTransactions) =>

        val joinedTransactions: RDD[(String, (Double, Double))] =
          TransactionETL.calculateSearchTransactions(searchTransactions)
            .join(TransactionETL.calculateNonSearchTransactions(nonSearchTransactions))

        TransactionETL.calculateParticipation(joinedTransactions).collect() should be (joinedTransactions.map {
          case (key: String, (searchValue: Double, nonSearchValue: Double)) =>
            (key, searchValue / (searchValue + nonSearchValue))
        }.collect())

    }
  }

}
