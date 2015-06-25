package ignition.jobs.setups

import ignition.chaordic.Chaordic
import ignition.chaordic.pojo.Parsers.TransactionParser
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.{AggregationLevel, TransactionETL}
import org.json4s.native.Serialization.write

import scala.util.Success

object TransactionETLSetup {
  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext

    val transactions = sc.textFile("/tmp/transactions/*").map { transaction =>
      Chaordic.parseWith(transaction, new TransactionParser)
    }.collect{ case Success(t) => t }


    implicit val aggregationValue = AggregationLevel.HOURS

    val results = TransactionETL.process(sc, transactions)

    results.searchRevenue.map {
      x =>
        implicit val formats = org.json4s.DefaultFormats
        write(x)
    }.saveAsTextFile("/tmp/test/searchRevenue")
    results.participationRatio.map {
      x =>
        implicit val formats = org.json4s.DefaultFormats
        write(x)
    }.saveAsTextFile("/tmp/test/participationRatio")

  }
}
