package ignition.jobs.setups

import ignition.chaordic.pojo.Parsers.SearchLogParser
import ignition.chaordic.pojo.TopQueries
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.chaordic.utils.Json
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.ExecutionRetry
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.TopQueriesJob
import org.apache.spark.rdd.RDD

object TopQueriesSetup extends ExecutionRetry  {

  def run(runnerContext: RunnerContext): Unit = {

    val sc = runnerContext.sparkContext
    val config = runnerContext.config
    val now = runnerContext.config.date

    // "s3n://chaordic-search-logs/searchlog/*"
    val parsedSearchLogs = SearchLogParser.parseSearchLogs(sc.filterAndGetTextFiles("/home/fparisotto/Temp/search-ignition/s3_cache/*",
      startDate = Option(now.minusDays(1).withTimeAtStartOfDay()), endDate = Option(now)))

    val topQueries: RDD[TopQueries] = TopQueriesJob.execute(parsedSearchLogs)

    topQueries
      .map(Json.toJsonString(_))
      .saveAsTextFile(s"/tmp/TopQueriesSetup/${now.toString}")
  }

}
