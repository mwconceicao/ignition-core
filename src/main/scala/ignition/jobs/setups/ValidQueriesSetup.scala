package ignition.jobs.setups

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.jobs.utils.uploader.Uploader
import ignition.jobs.{Configuration, SearchETL, ValidQueriesJob}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

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

    val s3NPath = buildS3Prefix(config)

    val s3Path = s3NPath.replace("s3n://", "s3://")

    ValidQueriesJob.process(parsedSearchLogs, parsedClickLogs)
      .map(_.toRaw.toJson)
      .coalesce(20)
      .saveAsTextFile(s3NPath)

    val indexJsonConfig = Option(Source.fromURL(getClass.getResource("/valid_queries_index_configuration.json")).mkString)
    Uploader.runValidQueries(s3Path, Configuration.elasticSearchAPI, Configuration.elasticSearchPort,
      Configuration.elasticSearchTimeoutInMinutes, Configuration.elasticSearchBulk, indexJsonConfig)
  }

}
