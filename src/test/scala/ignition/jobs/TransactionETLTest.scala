package ignition.jobs

import ignition.chaordic.pojo.ChaordicGenerators
import ignition.core.testsupport.spark.SharedSparkContext
import ignition.core.utils.BetterTrace
import ignition.jobs.TransactionETL.ETLTransaction
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, ShouldMatchers}
import org.scalautils.TolerantNumerics
import scala.language.postfixOps

class TransactionETLTest extends FlatSpec with ShouldMatchers with SharedSparkContext
  with GeneratorDrivenPropertyChecks with BetterTrace {

  val transactionGenerator = ChaordicGenerators.transactionGenerator()
  val nonSearchTransactionsGenerator = Gen.nonEmptyListOf(transactionGenerator).map(sc.parallelize(_))
  val searchTransactionsGenerator = Gen.nonEmptyListOf(
    ChaordicGenerators.transactionGenerator(gInfo = Gen.const(Map("cssearch" -> "")))
  ).map(sc.parallelize(_))

  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.001)

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

        transaction.cashFromTransaction === cash
      }
    }
  }

  it should "sum search transactions on method calculateSearchSales" in {
    forAll(searchTransactionsGenerator) {
      transactions =>
        val sumOfRDD = TransactionETL.calculateSearchSales(transactions).map(_._2).sum()
        val sumOfRaw = transactions.map(_.cashFromTransaction).sum()

        sumOfRDD === sumOfRaw
    }
  }

  it should "not sum search transactions on method calculateSearchTransactions on the same day" in {
    forAll(nonSearchTransactionsGenerator) {
      transactions =>
        val sumOfRDD = TransactionETL.calculateSearchSales(transactions)

        sumOfRDD.isEmpty() shouldBe true
    }
  }

  it should "generate the same number of keys as we have client/days in our RDD" in {
    val transactions = Gen.nonEmptyListOf(
      ChaordicGenerators.transactionGenerator(
        gDate = ChaordicGenerators.dateGenerator(TimeUnits.DAYS),
        gInfo = Gen.const(Map("cssearch" -> ""))
      )
    ).map(sc.parallelize(_))

    forAll(transactions) {
      transactions =>
        val actualAmountOfMetrics = TransactionETL.calculateSearchSales(transactions).keys.count()
        val expectedAmountOfMetrics = transactions.map(_.cashByDay).keys.distinct().count()

        actualAmountOfMetrics shouldBe expectedAmountOfMetrics
    }
  }

  it should "sum search transactions on method calculateNonSearchTransactions on the same day" in {
    forAll(searchTransactionsGenerator) {
      transactions =>
        val sumOfRDD = TransactionETL.calculateOverallSales(transactions).map(_._2).sum()
        val sumOfRaw = transactions.map(_.cashFromTransaction).sum()

        sumOfRDD === sumOfRaw
    }
  }

}
