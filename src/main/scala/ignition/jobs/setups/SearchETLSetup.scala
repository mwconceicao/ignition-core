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
  lazy val elasticSearch = new ElasticSearchClient(Configuration.elasticSearchHost, Configuration.elasticSearchPort)

  def run(runnerContext: RunnerContext) {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config

    val start = parseDateOrElse(config.additionalArgs.get("start"), config.date.minusDays(1).withTimeAtStartOfDay())
    val end = parseDateOrElse(config.additionalArgs.get("end"), config.date.minusDays(1).withTime(23, 59, 59, 999))

    implicit val timeoutForSaveOperation: FiniteDuration = 30 minutes

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

    val transactionsResults = TransactionETL.process(transactions)
    val mainIndicatorsResults = MainIndicators.process(searchLogs, autoCompleteLogs, clickLogs)

    def toDashPoint(tuple: (MainIndicatorKey, Int)): DashPoint = tuple match {
      case (indicator, value) => indicator.toFeaturedResultPoint(value)
    }

    def toKpi[T <: DashPoint](kpi: String, start: DateTime, end: DateTime, points: RDD[T]): RDD[KpiWithDashPoint] =
      points.map(point => KpiWithDashPoint(kpi, start, end, point))

    val kpis = sc.union(toKpi("sales_search", start, end, transactionsResults.salesSearch),
      toKpi("sales_search", start, end, transactionsResults.salesSearch),
      toKpi("sales_overall", start, end, transactionsResults.salesSearch),
      toKpi("searches", start, end, mainIndicatorsResults.searchMetrics.map(toDashPoint)),
      toKpi("unique_searches", start, end, mainIndicatorsResults.searchUniqueMetrics.map(toDashPoint)),
      toKpi("search_clicks", start, end, mainIndicatorsResults.searchClickMetrics.map(toDashPoint)),
      toKpi("unique_search_clicks", start, end, mainIndicatorsResults.searchClickUniqueMetrics.map(toDashPoint)),
      toKpi("autocomplete_count", start, end, mainIndicatorsResults.autoCompleteMetrics.map(toDashPoint)),
      toKpi("autocomplete_unique", start, end, mainIndicatorsResults.autoCompleteUniqueMetrics.map(toDashPoint)),
      toKpi("autocomplete_clicks", start, end, mainIndicatorsResults.autoCompleteClickMetrics.map(toDashPoint)),
      toKpi("unique_autocomplete_clicks", start, end, mainIndicatorsResults.autoCompleteUniqueClickMetrics.map(toDashPoint)))
      .repartition(numPartitions = 1).persist(StorageLevel.MEMORY_AND_DISK)

    kpis.map(Json.toJson4sString).saveAsTextFile(s"$s3Prefix/kpis")
    logger.info(s"Kpis saved to s3, path = $s3Prefix/kpis")

    topQueriesResults
      .repartition(numPartitions = 1)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .saveAsTextFile(s"$s3Prefix/top-queries")
    logger.info(s"TopQueries saved to s3, path = $s3Prefix/top-queries")

    val fSaveKpis = saveKpisToDashBoard(kpis.collect().toSeq).map { _ =>
      logger.info("Kpis saved to dashboard!")
    }

    val defaultIndexConfig = Source.fromURL(getClass.getResource("/etl-top-queries-template.json")).mkString
    val fSaveTopQueries = elasticSearch.saveTopQueries(topQueriesResults.collect().toIterator, defaultIndexConfig, bulkSize = 50)
    fSaveTopQueries.onComplete {
      case Success(result) =>
        logger.info(s"Top-queries saved to elastic-search! Details: $result")
      case Failure(exception) =>
        logger.error("Fail to bulk index top queries", exception)
        throw exception
    }

    val fSaveOperation = fSaveKpis.zip(fSaveTopQueries)
    fSaveOperation.onComplete {
      case Success(_) =>
        logger.info("ETL GREAT SUCCESS =]")
      case Failure(exception) =>
        logger.error("Error on saving metrics", exception)
        throw exception
    }

    Await.ready(fSaveOperation, timeoutForSaveOperation)
  }

}
