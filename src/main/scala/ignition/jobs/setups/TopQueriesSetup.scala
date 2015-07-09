package ignition.jobs.setups

import ignition.chaordic.pojo.TopQueries
import ignition.chaordic.utils.Json
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.{SearchETL, TopQueriesJob}
import org.slf4j.LoggerFactory

object TopQueriesSetup extends SearchETL  {

  lazy val logger = LoggerFactory.getLogger("ignition.TopQueriesSetup")

  def run(runnerContext: RunnerContext): Unit = {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config
    val now = runnerContext.config.date
    val start = now.minusDays(1).withTimeAtStartOfDay()

    logger.info(s"Starting TopQueriesJob for start = $start, end $now")

    val parsedSearchLogs = parseSearchLogs(sc, start = start, end = now)
    TopQueriesJob.execute(parsedSearchLogs)
      .repartition(numPartitions = 1)
      .map(transformToJsonString)
      .saveAsTextFile(s"s3://chaordic-search-ignition-history/top-queries/${config.tag}")

    logger.info(s"TopQueriesJob done.")
  }

  def transformToJsonString(topQueries: TopQueries): String = {
    val topQueriesAsMap = Map(
      "apiKey" -> topQueries.apiKey,
      "datetime" -> topQueries.day,
      "queries_has_results" -> topQueries.hasResult,
      "event" -> "top_queries",
      "top_queries" -> topQueries.topQueries.map(query =>
        Map("query" -> query.query, "count" -> query.count)))
    Json.toJsonString(topQueriesAsMap)
  }

}
