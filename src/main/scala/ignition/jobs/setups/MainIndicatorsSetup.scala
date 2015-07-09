package ignition.jobs.setups

import java.util.concurrent.Executors

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.MainIndicators.MainIndicatorKey
import ignition.jobs.utils.DashboardAPI.FeaturedResultPoint
import ignition.jobs.{MainIndicators, _}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object MainIndicatorsSetup extends SearchETL {

  lazy val logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  def run(runnerContext: RunnerContext) {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config

    val start = parseDateOrElse(config.additionalArgs.get("start"), config.date.minusDays(6).withTimeAtStartOfDay())
    val end = parseDateOrElse(config.additionalArgs.get("end"), config.date.withTime(23, 59, 59, 999))

    val timeoutSaveOperation: FiniteDuration = 30 minutes

    logger.info(s"Starting MainIndicatorsETL for start=$start, end=$end")

    logger.info(s"Parsing Search logs...")
    val searchLogs = parseSearchLogs(sc, start, end).persist()
    logger.info(s"Parsing AutoComplete logs...")
    val autoCompleteLogs = parseAutoCompleteLogs(sc, start, end).persist()
    logger.info(s"Parsing Click logs...")
    val clickLogs = parseClickLogs(sc, start, end).persist()

    val results = MainIndicators.process(searchLogs, autoCompleteLogs, clickLogs)

    def collectAndMap(rdd: RDD[(MainIndicatorKey, Int)]): Seq[FeaturedResultPoint] =
      rdd.collect().map {
        case (key, value) => key.toFeaturedResultPoint(value)
      }.toSeq

    val f1: Future[Unit] = saveMetrics("searches", start, end, collectAndMap(results.searchMetrics))
    val f2 = saveMetrics("unique_searches", start, end, collectAndMap(results.searchUniqueMetrics))
    val f3 = saveMetrics("search_clicks", start, end, collectAndMap(results.searchClickMetrics))
    val f4 = saveMetrics("unique_search_clicks", start, end, collectAndMap(results.searchClickUniqueMetrics))
    val f5 = saveMetrics("autocomplete_count", start, end, collectAndMap(results.autoCompleteMetrics))
    val f6 = saveMetrics("autocomplete_unique", start, end, collectAndMap(results.autoCompleteUniqueMetrics))
    val f7 = saveMetrics("autocomplete_clicks", start, end, collectAndMap(results.autoCompleteClickMetrics))
    val f8 = saveMetrics("unique_autocomplete_clicks", start, end, collectAndMap(results.autoCompleteUniqueClickMetrics))

    val saveToDashBoard = Future.sequence(List(f1,f2,f3,f4,f5,f6,f7,f8))

    saveToDashBoard.onComplete {
      case Success(_) => logger.info("MainIndicators - GREAT SUCCESS")
      case Failure(exception) => logger.error("Error on saving metrics to dashboard API", exception)
    }

    try {
      Await.ready(saveToDashBoard, timeoutSaveOperation)
    } catch {
      case NonFatal(exception) =>
        logger.error("Error on saving metrics to dashboard API", exception)
    }
  }

}
