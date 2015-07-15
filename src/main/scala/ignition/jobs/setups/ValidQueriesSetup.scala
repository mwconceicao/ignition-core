package ignition.jobs.setups

import ignition.chaordic.utils.Json
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.ValidQueriesJob.ValidQueryFinal
import ignition.jobs.{SearchETL, ValidQueriesJob}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}


object ValidQueriesSetup extends SearchETL {


  lazy val logger: Logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  def run(runnerContext: RunnerContext) {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config
    val now = runnerContext.config.date

    val start = parseDateOrElse(config.additionalArgs.get("start"), config.date.minusDays(180).withTime(23, 59, 59, 999))
    val end = parseDateOrElse(config.additionalArgs.get("end"), config.date.minusDays(1).withTimeAtStartOfDay())

    logger.info(s"Starting ValidQueries for start=$start, end=$end")

    val parsedSearchLogs = parseSearchLogs(config.setupName, sc, start = start, end = now).persist(StorageLevel.MEMORY_AND_DISK)
    val parsedClickLogs = parseClickLogs(config.setupName, sc, start = start, end = now).persist(StorageLevel.MEMORY_AND_DISK)

    ValidQueriesJob.process(parsedSearchLogs, parsedClickLogs)
      .map(transformToJsonString)
      .coalesce(20)
      .saveAsTextFile(s"s3n://chaordic-search-ignition-history/valid-queries/${config.tag}")
  }

  def transformToJsonString(validQuery: ValidQueryFinal): String = {
    val validQueryAsMap = Map(
      "top_query" -> validQuery.topQuery,
      "apiKey" -> validQuery.apiKey,
      "raw_ctr" -> validQuery.rawCtr,
      "tokens" -> validQuery.tokens,
      "latest_search_log_results" -> validQuery.latestSearchLogResults,
      "searchs" -> validQuery.searches,
      "active" -> validQuery.active,
      "average_results" -> validQuery.averageResults,
      "latest_search_log" -> validQuery.latestSearchLog.toString("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
      "queries" -> validQuery.queries.map(vq => Map(
        "raw_ctr" -> vq.rawCtr,
        "apiKey" -> vq.apiKey,
        "average_results" -> vq.averageResults,
        "latest_search_log_results" -> vq.latestSearchLogResults,
        "latest_search_log" -> vq.latestSearchLog.toString("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        "latest_search_log_feature" -> vq.latestSearchLogFeature,
        "searchs" -> vq.searches,
        "query" -> vq.query,
        "clicks" -> vq.clicks)),
      "clicks" ->  validQuery.clicks)
    Json.toJsonString(validQueryAsMap)
  }

}
