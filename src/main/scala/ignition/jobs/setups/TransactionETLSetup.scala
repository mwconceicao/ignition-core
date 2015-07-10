package ignition.jobs.setups

import java.util.concurrent.Executors

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.setups.SitemapXMLSetup._
import ignition.jobs.utils.SearchApi
import ignition.jobs.{SearchETL, TransactionETL}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
 * This Job Calculate our Transaction based metrics.
 *
 * It consist of two basic metrics:
 *  - searchSales: Monetary value of all the transactions that search was involved (done by checking the
 *                 `cssearch` field on info, that tagged by Onsite).
 *  - overallSales: Monetary value of all captured transactions.
 */

object TransactionETLSetup extends SearchETL {

  lazy val logger: Logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  def run(runnerContext: RunnerContext) {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config

    val start = parseDateOrElse(config.additionalArgs.get("start"), config.date.minusDays(1).withTimeAtStartOfDay())
    val end = parseDateOrElse(config.additionalArgs.get("end"), config.date.minusDays(1).withTime(23, 59, 59, 999))

    val timeoutSaveOperation: FiniteDuration = 30 minutes

    logger.info(s"Starting TransactionETLSetup for start=$start, end=$end")

    val allClients = executeRetrying(SearchApi.getClients())
    logger.info(s"With clients: $allClients")

    val transactions = parseTransactions(config.setupName, sc, start, end, allClients).persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(s"Starting ETLTransaction")
    val results = TransactionETL.process(sc, transactions)

    val saveSalesSearch = saveMetrics("sales_search", start, end, results.salesSearch.collect().toSeq)
    val saveSalesOverall = saveMetrics("sales_overall", start, end, results.salesOverall.collect().toSeq)
    val saveToDashBoard = saveSalesSearch.zip(saveSalesOverall)

    saveToDashBoard.onComplete {
      case Success(_) =>
        logger.info("TransactionETL - GREAT SUCCESS")
      case Failure(exception) =>
        logger.error("Error on saving metrics to dashboard API", exception)
        throw exception
    }

    try {
      Await.ready(saveToDashBoard, timeoutSaveOperation)
    } catch {
      case NonFatal(exception) =>
        logger.error("Error on saving metrics to dashboard API", exception)
        throw exception
    }
  }

}
