package ignition.jobs.setups

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.utils.uploader.Uploader
import ignition.jobs.{Configuration, SearchETL, TopQueriesJob}
import org.slf4j.LoggerFactory

import scala.io.Source

object TopQueriesSetup extends SearchETL  {

  lazy val logger = LoggerFactory.getLogger("ignition.TopQueriesSetup")

  def run(runnerContext: RunnerContext): Unit = {
    val sc = runnerContext.sparkContext
    val config = runnerContext.config
    val now = runnerContext.config.date
    val start = now.minusDays(1).withTimeAtStartOfDay()

    logger.info(s"Starting TopQueriesJob for start = $start, end $now")

    val s3Path = buildS3Prefix(runnerContext.config)

    val parsedSearchLogs = parseSearchLogs(config.setupName, sc, start = start, end = now)
    TopQueriesJob.execute(parsedSearchLogs)
      .repartition(numPartitions = 1)
      .map(_.toRaw.toJson)
      .saveAsTextFile(s3Path)

    val indexJsonConfig = Option(Source.fromURL(getClass.getResource("/etl-top-queries-template.json")).mkString)
    Uploader.runTopQueries(s3Path, Configuration.elasticSearchReport, Configuration.elasticSearchPort,
      Configuration.elasticSearchTimeoutInMinutes, Configuration.elasticSearchBulk, indexJsonConfig)

    logger.info(s"TopQueriesJob done.")
  }

}
