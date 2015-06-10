package ignition.jobs.setups

import ignition.chaordic.Chaordic
import ignition.chaordic.pojo.Product
import ignition.chaordic.pojo.Parsers.{ProductV2Parser, ProductV1Parser}
import ignition.chaordic.utils.JobConfiguration
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.ExecutionRetry
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.SitemapXMLJob
import ignition.jobs.SitemapXMLJob.Config
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.util.Success

// filterAndGetTextFiles requires a date extractor:
import ignition.core.jobs.utils.SimplePathDateExtractor.default

// This job is also used as a Sanity Check for cluster initialization
object SitemapXMLSetup extends ExecutionRetry {

  def parseProducts(rdd: RDD[String]): RDD[Product] = rdd.map { line =>
    Chaordic.parseWith(line, new ProductV1Parser, new ProductV2Parser)
  }.collect { case Success(v) => v.fold(identity, identity) }

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val config = runnerContext.config
    val now = runnerContext.config.date

    val clients = Set("saraiva-v5")
    val configurations = new JobConfiguration(Config(),
      Map("saraiva-v5" -> Config("http://busca.saraiva.com.br/pages", Set("ratings", "publisher", "brand", "ano", "produtoDigital"))))

    val numberOutputs = 1000
    clients.map { apiKey =>
      val conf = configurations.getFor(apiKey)
//      val products = parseProducts(sc.filterAndGetTextFiles(s"s3n://platform-dumps-virginia/products/*/$apiKey.gz", endDate = Option(now), lastN = Option(1)))
      val products = parseProducts(sc.textFile(s"/mnt/tmp/products/$apiKey"))
      val urls = SitemapXMLJob.generateUrlXMLs(sc, now, products, conf)

      val fullXMLsPerPartition = SitemapXMLJob.generateUrlSetPerPartition(urls.repartition(numberOutputs))
      fullXMLsPerPartition.saveAsTextFile(s"s3n://mail-ignition/tmp/sitexmls/${config.tag}/$apiKey", classOf[GzipCodec])
    }

  }
}
