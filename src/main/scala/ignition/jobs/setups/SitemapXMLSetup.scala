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
import ignition.jobs.SitemapXMLJob.Config
import ignition.jobs._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.util.Success
import scala.xml.Elem


object SitemapXMLSetup extends ExecutionRetry {

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
    val jobOutputBucket = "bucket"
    val finalBucket = "bucketlatest"
    val finalBucketPrefix = "sitemaps"


    val clients = Set("saraiva-v5")
    // Config(baseHost: String, generatePages: Boolean, generateSearch: Boolean,
    //pagesBaseHost: String = "", detailsKeys: Set[String] = Set.empty)
    val configurations = new JobConfiguration(Config(),
      Map("saraiva-v5" -> Config(baseHost="http://busca.saraiva.com.br",
                                 generatePages=true, generateSearch=false,
                                 detailsKeys=Set("ratings", "publisher", "brand", "ano", "produtoDigital"))))

    val numberOutputs = 10

    val willNeedSearch = configurations.values.exists(_.generateSearch)
    val parsedClickLogs = if (willNeedSearch) parseSearchClickLogs(sc.textFile("search/clicklog")).persist() else sc.emptyRDD[SearchClickLog]
    val parsedSearchLogs = if (willNeedSearch) parseSearchLogs(sc.textFile("search/2015-06-01")).persist() else sc.emptyRDD[SearchLog]

    clients.foreach { apiKey =>
      val conf = configurations.getFor(apiKey)
      val pageUrls = if (conf.generatePages) {
      val products = parseProducts(sc.filterAndGetTextFiles(s"s3n://platform-dumps-virginia/products/*/$apiKey.gz", endDate = Option(now), lastN = Option(1)))
        SitemapXMLPagesJob.generateUrlXMLs(sc, now, products, conf)
      } else {
        sc.emptyRDD[String]
      }

      val searchUrls = if (conf.generateSearch) {
        val searchClickLogs = parsedClickLogs.filter(_.apiKey == apiKey)
        val searchLogs = parsedSearchLogs.filter(_.apiKey == apiKey)
        SitemapXMLSearchJob.generateSearchURLXMLs(sc, now, searchLogs, searchClickLogs, conf)
      } else{
        sc.emptyRDD[String]
      }

      val urls = pageUrls ++ searchUrls
      val jobOutputPart = s"tmp/${config.setupName}/$apiKey/${config.tag}"
      val jobOutputPath = s"s3n://$jobOutputBucket/$jobOutputPart"

      val fullXMLsPerPartition = SitemapXMLJob.generateUrlSetPerPartition(urls.repartition(numberOutputs))
      fullXMLsPerPartition.saveAsTextFile(jobOutputPath, classOf[GzipCodec])
      if (config.user == productionUser) {
        copyAndGenerateSitemapIndex(conf, now, jobOutputBucket, jobOutputPart, finalBucket, s"$finalBucketPrefix/$apiKey", ".*part-.*")
      }
    }
  }

  /*
    destBucket: APIKEY IS INSIDE.
   */
  def copyAndGenerateSitemapIndex(conf: Config, now: DateTime,
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
        <loc>{s"${conf.baseHost}/$filename"}</loc>
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
