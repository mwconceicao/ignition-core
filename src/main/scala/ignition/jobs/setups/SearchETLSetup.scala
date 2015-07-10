package ignition.jobs.setups

import java.util.concurrent.Executors

import ignition.chaordic.utils.Json
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.MainIndicators.MainIndicatorKey
import ignition.jobs.TopQueriesJob.TopQueries
import ignition.jobs.setups.MainIndicatorsSetup._
import ignition.jobs.setups.SitemapXMLSetup.logger
import ignition.jobs.setups.SitemapXMLSetup.logger
import ignition.jobs.setups.TransactionETLSetup._
import ignition.jobs.setups.TransactionETLSetup.logger
import ignition.jobs.utils.DashboardAPI.{DashPoint, ResultPoint}
import ignition.jobs.{TopQueriesJob, MainIndicators, TransactionETL, SearchETL}
import ignition.jobs.setups.SitemapXMLSetup._
import ignition.jobs.utils.SearchApi
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object SearchETLSetup extends SearchETL {

  lazy val logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  def run(runnerContext: RunnerContext) {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config

    val start = parseDateOrElse(config.additionalArgs.get("start"), config.date.minusDays(2).withTimeAtStartOfDay())
    val end = parseDateOrElse(config.additionalArgs.get("end"), config.date.minusDays(1).withTime(23, 59, 59, 999))

    val timeoutForSaveOperation: FiniteDuration = 30 minutes

    logger.info(s"Starting SearchETL for start=$start, end=$end")

    val allClients = executeRetrying(SearchApi.getClients())
    logger.info(s"With clients: $allClients")

    val s3Prefix = s"s3n://chaordic-search-ignition-history/${config.setupName}/${config.user}/${config.tag}"

    logger.info(s"Starting ETLTransaction")

    logger.info(s"Parsing Transactions...")
    val transactions = parseTransactions(config.setupName, sc, start, end, allClients).persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(s"Parsing Search logs...")
    val searchLogs = parseSearchLogs(config.setupName, sc, start, end).persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(s"Parsing AutoComplete logs...")
    val autoCompleteLogs = parseAutoCompleteLogs(config.setupName, sc, start, end).persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(s"Parsing Click logs...")
    val clickLogs = parseClickLogs(config.setupName, sc, start, end).persist(StorageLevel.MEMORY_AND_DISK)

    val topQueriesResults = TopQueriesJob.execute(searchLogs)
    val fSaveTopQueries = saveTopQueries(topQueriesResults, s3Prefix)

    val transactionsResults = TransactionETL.process(transactions)
    val mainIndicatorsResults = MainIndicators.process(searchLogs, autoCompleteLogs, clickLogs)

    val kpis = sc.union(
      toKpi("sales_search", start, end, transactionsResults.salesSearch),
      toKpi("sales_search", start, end, transactionsResults.salesSearch),
      toKpi("sales_overall", start, end, transactionsResults.salesSearch),
      toKpi("searches", start, end, mainIndicatorsResults.searchMetrics),
      toKpi("unique_searches", start, end, mainIndicatorsResults.searchUniqueMetrics),
      toKpi("search_clicks", start, end, mainIndicatorsResults.searchClickMetrics),
      toKpi("unique_search_clicks", start, end, mainIndicatorsResults.searchClickUniqueMetrics),
      toKpi("autocomplete_count", start, end, mainIndicatorsResults.autoCompleteMetrics),
      toKpi("autocomplete_unique", start, end, mainIndicatorsResults.autoCompleteUniqueMetrics),
      toKpi("autocomplete_clicks", start, end, mainIndicatorsResults.autoCompleteClickMetrics),
      toKpi("unique_autocomplete_clicks", start, end, mainIndicatorsResults.autoCompleteUniqueClickMetrics)
    ).repartition(numPartitions = 1).persist(StorageLevel.MEMORY_AND_DISK)

    kpis.saveAsTextFile(s"$s3Prefix/kpis")

    val fSaveToDashBoard = saveDashPoints(kpis, s3Prefix)
    val fSaveOperation = fSaveToDashBoard.zip(fSaveTopQueries)

    fSaveOperation.onComplete {
      case Success(_) =>
        logger.info("ETL GREAT SUCCESS =]")
      case Failure(exception) =>
        logger.error("Error on saving metrics", exception)
        throw exception
    }

    Await.ready(fSaveOperation, timeoutForSaveOperation)
  }

  case class KpiWithDashPoint(kpi: String, start: DateTime, end: DateTime, point: DashPoint)

  def toKpi[T <: DashPoint](kpi: String, start: DateTime, end: DateTime, points: RDD[T]): RDD[KpiWithDashPoint] =
    points.map(point => KpiWithDashPoint(kpi, start, end, point))

  def saveTopQueries(topQueries: RDD[TopQueries], s3Prefix: String): Future[Unit] = {
    val transformed = topQueries.map(TopQueriesSetup.transformToJsonString)
    transformed.saveAsTextFile(s"$s3Prefix/top-queries", classOf[GzipCodec])
    Future.successful() // TODO save results to Elastic Search
  }

  def saveDashPoints(dashPoints: RDD[KpiWithDashPoint], s3Prefix: String): Future[Unit] = {
    val requests = dashPoints.keyBy(data => (data.kpi, data.start, data.end)).groupByKey().map {
      case ((kpi, start, end), points) => saveMetrics(kpi, start, end, points.map(_.point).toSeq)
    }
    Future.sequence(requests).map(_ => logger.info("All kpi's saved!"))
  }

}
