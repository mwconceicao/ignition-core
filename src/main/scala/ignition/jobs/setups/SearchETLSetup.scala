package ignition.jobs.setups

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import ignition.chaordic.utils.Json
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.MainIndicators.MainIndicatorKey
import ignition.jobs.SearchETL.KpiWithDashPoint
import ignition.jobs.setups.SitemapXMLSetup._
import ignition.jobs.utils.DashboardAPI.DashPoint
import ignition.jobs.utils.{ElasticSearchClient, SearchApi}
import ignition.jobs._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext}
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success}

object SearchETLSetup extends SearchETL {

  lazy val logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  implicit lazy val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  implicit lazy val actorSystem = ActorSystem("SearchETLSetup")
  lazy val elasticSearch = new ElasticSearchClient(Configuration.elasticSearchReport, Configuration.elasticSearchPort)

  def run(runnerContext: RunnerContext) {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config

    val start = parseDateOrElse(config.additionalArgs.get("start"), config.date.minusDays(1).withTimeAtStartOfDay())
    val end = parseDateOrElse(config.additionalArgs.get("end"), config.date.minusDays(1).withTime(23, 59, 59, 999))

    implicit val timeoutForSaveOperation: FiniteDuration = 30 minutes

    logger.info(s"Starting SearchETL for start=$start, end=$end")

    val allClients = executeRetrying(SearchApi.getClients())
    logger.info(s"With clients: $allClients")

    val s3KPIsPath = buildS3Prefix(config) + "/kpis"
    val s3TopQueriesPath = buildS3Prefix(config) + "/top-queries"

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

    val transactionsResults = TransactionETL.process(transactions)
    val mainIndicatorsResults = MainIndicators.process(searchLogs, autoCompleteLogs, clickLogs)

    def toDashPoint(tuple: (MainIndicatorKey, Int)): DashPoint = tuple match {
      case (indicator, value) => indicator.toFeaturedResultPoint(value)
    }

    def toKpi[T <: DashPoint](kpi: String, start: DateTime, end: DateTime, points: RDD[T]): RDD[KpiWithDashPoint] =
      points.map(point => KpiWithDashPoint(kpi, start, end, point))

    val kpis = sc.union(
      toKpi("sales_search", start, end, transactionsResults.salesSearch),
      toKpi("sales_overall", start, end, transactionsResults.salesOverall),
      toKpi("searches", start, end, mainIndicatorsResults.searchMetrics.map(toDashPoint)),
      toKpi("unique_searches", start, end, mainIndicatorsResults.searchUniqueMetrics.map(toDashPoint)),
      toKpi("search_clicks", start, end, mainIndicatorsResults.searchClickMetrics.map(toDashPoint)),
      toKpi("unique_search_clicks", start, end, mainIndicatorsResults.searchClickUniqueMetrics.map(toDashPoint)),
      toKpi("autocomplete_count", start, end, mainIndicatorsResults.autoCompleteMetrics.map(toDashPoint)),
      toKpi("autocomplete_unique", start, end, mainIndicatorsResults.autoCompleteUniqueMetrics.map(toDashPoint)),
      toKpi("autocomplete_clicks", start, end, mainIndicatorsResults.autoCompleteClickMetrics.map(toDashPoint)),
      toKpi("unique_autocomplete_clicks", start, end, mainIndicatorsResults.autoCompleteUniqueClickMetrics.map(toDashPoint)))
      .repartition(numPartitions = 1).persist(StorageLevel.MEMORY_AND_DISK)

    kpis.map(Json.toJson4sString).saveAsTextFile(s3KPIsPath)
    logger.info(s"Kpis saved to s3, path = $s3KPIsPath")

    topQueriesResults
      .map(_.toRaw.toJson)
      .repartition(numPartitions = 1)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .saveAsTextFile(s3TopQueriesPath)
    logger.info(s"TopQueries saved to s3, path = $s3TopQueriesPath")

    val defaultIndexConfig = Source.fromURL(getClass.getResource("/etl-top-queries-template.json")).mkString
    val indexOperation = elasticSearch.saveTopQueries(topQueriesResults.collect().toIterator, defaultIndexConfig, bulkSize = 50)
    indexOperation match {
      case Success(_) => logger.info(s"Top-queries saved to elasticsearch!")
      case Failure(ex) =>
        logger.error(s"Fail to save top-queries", ex)
        throw ex
    }

    val fSaveKpis = saveKpisToDashBoard(kpis.collect().toSeq).map { _ =>
      logger.info("Kpis saved to dashboard!")
    }

    fSaveKpis.onComplete {
      case Success(_) =>
        logger.info("ETL GREAT SUCCESS =]")
      case Failure(exception) =>
        logger.error("Error on saving metrics", exception)
        throw exception
    }

    Await.ready(fSaveKpis, timeoutForSaveOperation)
  }

}
