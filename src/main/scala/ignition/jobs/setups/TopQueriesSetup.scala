package ignition.jobs.setups

import ignition.chaordic.Chaordic
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.chaordic.utils.Json
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.ExecutionRetry
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.TopQueriesJob
import ignition.jobs.TopQueriesJob.TopQueries
import ignition.jobs.pojo._
import org.apache.spark.rdd.RDD

import scala.util.Success

object TopQueriesSetup extends ExecutionRetry  {

  def parseSearchLogs(rdd: RDD[String]): RDD[SearchLog] = rdd.map { line =>
    Chaordic.parseWith(line, new Parsers.SearchLogParser)
  }.collect { case Success(v) => v }

  def run(runnerContext: RunnerContext): Unit = {

    val sc = runnerContext.sparkContext
    val config = runnerContext.config
    val now = runnerContext.config.date

    // "s3n://chaordic-search-logs/searchlog/*"
    val parsedSearchLogs = parseSearchLogs(sc.filterAndGetTextFiles("/home/fparisotto/Temp/search-ignition/s3_cache/*",
      startDate = Option(now.minusDays(1).withTimeAtStartOfDay()), endDate = Option(now)))

    val topQueries: RDD[TopQueries] = TopQueriesJob.execute(parsedSearchLogs)

    topQueries
      .map(Json.toJsonString(_))
      .saveAsTextFile(s"/tmp/TopQueriesSetup/${now.toString}")
  }

}
