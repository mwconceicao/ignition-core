package ignition.jobs.setups

import ignition.chaordic.Chaordic
import ignition.chaordic.pojo.Parsers.TransactionParser
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.{AggregationLevel, TransactionETL}

import scala.util.Success

object TransactionETLSetup {
  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val now = runnerContext.config.date

    val transactions = sc.textFile("/Users/flavio/git/search-ignition/saraiva-v5.gz").map { transaction =>
      Chaordic.parseWith(transaction, new TransactionParser)
    }.collect{ case Success(t) => t }


    implicit val aggregationValue = AggregationLevel.MINUTES

    TransactionETL.process(sc, transactions)

  }
}
