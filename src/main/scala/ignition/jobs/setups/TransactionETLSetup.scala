package ignition.jobs.setups

import java.util.concurrent.{Executors, TimeUnit}

import ignition.chaordic.pojo.Parsers.TransactionParser
import ignition.chaordic.pojo.Transaction
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.chaordic.{Chaordic, ParsingReporter}
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.setups.SitemapXMLSetup._
import ignition.jobs.utils.DashboardAPI.ResultPoint
import ignition.jobs.utils.{DashboardAPI, SearchApi}
import ignition.jobs.TransactionETL
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, Interval}
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/**
 * This Job Calculate our Transaction based metrics.
 *
 * It consist of two basic metrics:
 *  - searchSales: Monetary value of all the transactions that search was involved (done by checking the
 *                 `cssearch` field on info, that tagged by Onsite).
 *  - overallSales: Monetary value of all captured transactions.
 */

object TransactionETLSetup {

  private lazy val logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  def run(runnerContext: RunnerContext) {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config

    val start = parseDateOrElse(config.additionalArgs.get("start"), config.date.minusDays(1).withTimeAtStartOfDay())
    val end = parseDateOrElse(config.additionalArgs.get("end"), config.date.withTime(23, 59, 59, 999))

    val timeoutSaveOperation = Duration(30, TimeUnit.MINUTES)

    logger.info(s"Starting TransactionETLSetup for start=$start, end=$end")

    val allClients = executeRetrying(SearchApi.getClients())
    logger.info(s"With clients: $allClients")

    val transactions = parseTransactions(sc, start, end, allClients).persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(s"Starting ETLTransaction")
    val results = TransactionETL.process(sc, transactions)

    val saveSalesSearch = saveMetrics("sales_search", start, end, results.salesSearch.collect().toSeq)
    val saveSalesOverall = saveMetrics("sales_overall", start, end, results.salesOverall.collect().toSeq)
    val saveToDashBoard = saveSalesSearch.zip(saveSalesOverall)

    saveToDashBoard.onComplete {
      case Success(_) => logger.info("TransactionETL - GREAT SUCCESS")
      case Failure(exception) => logger.error("Error on saving metrics to dashboard API", exception)
    }

    try {
      Await.ready(saveToDashBoard, timeoutSaveOperation)
    } catch {
      case NonFatal(exception) =>
        logger.error("Error on saving metrics to dashboard API", exception)
    }
  }

  def saveMetrics(kpi: String, start: DateTime, end: DateTime, points: Seq[ResultPoint]): Future[Unit] = {
    DashboardAPI.deleteDailyFact("search", kpi, new Interval(start, end)).flatMap { response =>
      logger.info(s"Cleanup for kpi = $kpi, start = $start, end = $end")
      Future.sequence(points.map(point => saveResultPoint(kpi, point))).map { _ =>
        logger.info(s"All kpi $kpi saved")
      }
    }
  }

  def saveResultPoint(kpi: String, resultPoint: ResultPoint): Future[Unit] = {
    DashboardAPI.dailyFact("search", kpi, resultPoint).map { _ =>
      logger.info(s"Kpi $kpi for client ${resultPoint.client} at day ${resultPoint.day} with value ${resultPoint.value} saved.")
    }
  }

  def parseTransactions(context: SparkContext, start: DateTime, end: DateTime, clients: Set[String]): RDD[Transaction] =
    context.filterAndGetTextFiles(s"s3n://platform-dumps-virginia/buyOrders/*/{${clients.mkString(",")}}.gz",
      endDate = Option(end), startDate = Option(start)).map {
      json => Chaordic.parseWith(json, parser = new TransactionParser, reporter = reporterFor("transaction"))
    }.collect { case Success(parsed) => parsed }

  def parseDateOrElse(param: Option[String], default: DateTime) = param.map(DateTime.parse).getOrElse(default)

  def reporterFor(entityName: String) = new ParsingReporter {
    override def reportError(message: String, jsonStr: String): Unit =
      logger.trace("Failed to parse '{}' with error '{}' for entity {}", jsonStr, message, entityName)

    override def reportSuccess(jsonStr: String): Unit =
      // using Seq to solve scala ambiguous reference to overloaded definition
      logger.trace("Parsed successfully json '{}' to entity {}", Seq(jsonStr, entityName))
  }

}
