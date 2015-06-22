package ignition.jobs.setups

import ignition.chaordic.Chaordic
import ignition.chaordic.pojo.Parsers.{ProductV1Parser, ProductV2Parser}
import ignition.chaordic.pojo.Product
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.chaordic.utils.JobConfiguration
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.ExecutionRetry
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.core.utils.DateUtils._
import ignition.core.utils.S3Client
import ignition.jobs._
import ignition.jobs.utils.SearchApi
import ignition.jobs.utils.SearchApi.SitemapConfig
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
import scala.util.{Failure, Try, Success}
import scala.xml.Elem


object SitemapXMLSetup extends ExecutionRetry {

  private lazy val logger = LoggerFactory.getLogger("ignition.SitemapXMLSetup")


  def parseProducts(rdd: RDD[String]): RDD[Product] = rdd.map { line =>
    Chaordic.parseWith(line, new ProductV1Parser, new ProductV2Parser)
  }.collect { case Success(v) => v.fold(identity, identity) }

  def parseSearchLogs(rdd: RDD[String]): RDD[SearchLog] = rdd.map { line =>
    Chaordic.parseWith(line, SearchLogParser)
  }.collect { case Success(v) => v }

  def parseSearchClickLogs(rdd: RDD[String]): RDD[SearchClickLog] = rdd.map { line =>
    Chaordic.parseWith(line, SearchClickLogParser)
  }.collect { case Success(v) => v }

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val config = runnerContext.config
    val now = runnerContext.config.date

    val productionUser = "root"
    val jobOutputBucket = "chaordic-search-ignition-history"
    val finalBucket = "chaordic-search-ignition-latest"
    val finalBucketPrefix = "sitemaps"


    logger.info("Getting clients list")
    val allClients = executeRetrying(SearchApi.getClients())
    logger.debug(s"Got clients: $allClients")


    val configurations = allClients.flatMap { client =>
      logger.info(s"Getting sitemap configuration for $client")
      val result = Try { executeRetrying(SearchApi.getSitemapConfig(client)) }
      result match {
        case Success(sitemapConfig) =>
          logger.debug(s"Got configuration $sitemapConfig for $client")
          client -> sitemapConfig :: Nil
        case Failure(t) =>
          logger.warn(s"Failed to get sitemap configuration for $client, message: ${t.getMessage}, ignoring client...")
          Nil
      }
    }.toMap

    val willNeedSearch = configurations.values.exists(_.generateSearch)

    logger.info(s"Do we need search logs? $willNeedSearch")
    val parsedClickLogs = if (willNeedSearch)
      parseSearchClickLogs(sc.filterAndGetTextFiles("s3n://chaordic-search-logs/clicklog/*",
        endDate = Option(now), startDate = Option(now.minusDays(30).withTimeAtStartOfDay()))).persist(StorageLevel.MEMORY_AND_DISK)
    else
      sc.emptyRDD[SearchClickLog]

    val parsedSearchLogs = if (willNeedSearch)
      parseSearchLogs(sc.filterAndGetTextFiles("s3n://chaordic-search-logs/searchlog/*",
        endDate = Option(now), startDate = Option(now.minusDays(30).withTimeAtStartOfDay()))).persist(StorageLevel.MEMORY_AND_DISK)
    else
      sc.emptyRDD[SearchLog]

    logger.info(s"Will process clients ${configurations.keySet}")
    configurations.foreach { case (apiKey, conf) =>
      try {
        logger.info(s"Starting processing for client $apiKey")
        val pageUrls = if (conf.generatePages) {
          val products = parseProducts(sc.filterAndGetTextFiles(s"s3n://platform-dumps-virginia/products/*/$apiKey.gz",
            endDate = Option(now), lastN = Option(1)).repartition(500))
          SitemapXMLPagesJob.generateUrlXMLs(sc, now, products, conf)
        } else {
          sc.emptyRDD[String]
        }

        val searchUrls = if (conf.generateSearch) {
          val searchClickLogs = parsedClickLogs.filter(_.apiKey == apiKey)
          val searchLogs = parsedSearchLogs.filter(_.apiKey == apiKey)
          SitemapXMLSearchJob.generateSearchUrlXMLs(sc, now, searchLogs, searchClickLogs, conf)
        } else {
          sc.emptyRDD[String]
        }

        val urls = pageUrls ++ searchUrls
        val jobOutputPart = s"tmp/${config.setupName}/$apiKey/${config.tag}"
        val jobOutputPath = s"s3n://$jobOutputBucket/$jobOutputPart"

        val fullXMLsPerPartition = SitemapXMLJob.generateUrlSetPerPartition(urls.repartition(conf.numberPartFiles))
        fullXMLsPerPartition.saveAsTextFile(jobOutputPath, classOf[GzipCodec])
        if (config.user == productionUser) {
          copyAndGenerateSitemapIndex(now, conf, jobOutputBucket, jobOutputPart, finalBucket, s"$finalBucketPrefix/$apiKey", ".*part-.*")
        }
        logger.info(s"Finished processing $apiKey")
      } catch {
        case NonFatal(e) =>
          logger.error(s"Got exception processing $apiKey, skipping client")
      }
    }
  }

  /*
    destBucket: APIKEY IS INSIDE.
   */
  def copyAndGenerateSitemapIndex(now: DateTime, conf: SitemapConfig,
                                  sourceBucket: String, sourceKey: String,
                                  destBucket: String, destPath: String,
                                  glob: String): Unit = {
    val contentType = "application/xml"
    val s3 = new S3Client
    val srcFiles: List[String] = s3
      .list(sourceBucket, sourceKey)
      .map(file => file.getKey)
      .filter(path => path matches glob)
      .toList

    def getFileName(path: String) = path.split("/").last

    srcFiles.foreach {
      filePath =>
        val contentEncoding = if (filePath.endsWith(".gz"))
          Option("gzip")
        else
          None
        executeRetrying(s3.copyFile(sourceBucket,
          filePath,
          destBucket,
          s"$destPath/${getFileName(filePath)}",
          destContentType = Option(contentType), destContentEncoding = contentEncoding))
    }

    def sitemap(filename: String): Elem =
      <sitemap>
        <loc>{s"${conf.normalizedHost}/sitemaps/$filename"}</loc>
        <lastmod>{now.toIsoString}</lastmod>
      </sitemap>

    def generateSitemap(filenames: List[String]) =
      s"""<?xml version="1.0" encoding="UTF-8"?>
        |<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        |${filenames.map(filename => sitemap(filename).toString()).mkString("\n")}
        |</sitemapindex>
        |""".stripMargin

    executeRetrying(s3.writeContent(destBucket, s"$destPath/sitemap.xml", generateSitemap(srcFiles.map(getFileName)), contentType = contentType))
  }
}
