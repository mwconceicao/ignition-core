package ignition.jobs.setups

import ignition.chaordic.Chaordic
import ignition.chaordic.pojo.Parsers.{ProductV1Parser, ProductV2Parser}
import ignition.chaordic.pojo.Product
import ignition.chaordic.utils.JobConfiguration
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.ExecutionRetry
import ignition.core.utils.S3Client
import ignition.jobs.SitemapXMLJob.Config
import ignition.jobs._
import ignition.core.jobs.utils.SparkContextUtils._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import ignition.chaordic.utils.ChaordicPathDateExtractor._

import scala.util.Success


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

    val jobOutputBucket = s"s3n://bucket/tmp/${config.setupName}"

    val clients = Set("saraiva-v5")
    // Config(baseHost: String, generatePages: Boolean, generateSearch: Boolean,
    //pagesBaseHost: String = "", detailsKeys: Set[String] = Set.empty)
    val configurations = new JobConfiguration(Config(),
      Map("saraiva-v5" -> Config(baseHost="http://busca.saraiva.com.br",
                                 generatePages=true, generateSearch=false,
                                 detailsKeys=Set("ratings", "publisher", "brand", "ano", "produtoDigital"))))

    val numberOutputs = 1

    val willNeedSearch = configurations.values.exists(_.generateSearch)
    val parsedClickLogs = if (willNeedSearch) parseSearchClickLogs(sc.textFile("search/clicklog")).persist() else sc.emptyRDD[SearchClickLog]
    val parsedSearchLogs = if (willNeedSearch) parseSearchLogs(sc.textFile("search/2015-06-01")).persist() else sc.emptyRDD[SearchLog]

    clients.map { apiKey =>
      val conf = configurations.getFor(apiKey)
      val pageUrls = if (conf.generatePages) {
      val products = parseProducts(sc.filterAndGetTextFiles(s"s3n://platform-dumps-virginia/products/*/$apiKey.gz", endDate = Option(now), lastN = Option(1)))
//      val products = parseProducts(sc.textFile(s"/tmp/products/$apiKey"))
//        val products = parseProducts(sc.textFile(s"/mnt/tmp/products/$apiKey"))
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

      val jobOutputPath = s"s3n://$jobOutputBucket/tmp/${config.setupName}/$apiKey/${config.tag}"

      val fullXMLsPerPartition = SitemapXMLJob.generateUrlSetPerPartition(urls.repartition(numberOutputs))
      fullXMLsPerPartition.saveAsTextFile(jobOutputPath, classOf[GzipCodec])
//      fullXMLsPerPartition.saveAsTextFile(s"s3n://mail-ignition/tmp/sitexmls/${config.tag}/$apiKey", classOf[GzipCodec])
//      fullXMLsPerPartition.saveAsTextFile(s"sitexmls/$apiKey", classOf[GzipCodec])
    }
  }

  def copyAndGenerateSitemapIndex(destBucket: String, destPath: String, sourceBucket: String, sourceKey: String, glob: String = "part-*"): Unit = {
    val s3 = new S3Client
    val srcFiles = s3.li
//    s3.list(sourceBucket, sourceKey)(0).getKey
  }
}
