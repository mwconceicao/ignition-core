package ignition.jobs.setups

import ignition.chaordic.Chaordic
import ignition.chaordic.pojo.Parsers.TransactionParser
import ignition.chaordic.pojo.Transaction
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.TransactionETL
import ignition.jobs.setups.SitemapXMLSetup._
import ignition.jobs.utils.SearchApi
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.json4s.native.Serialization.write
import org.slf4j.LoggerFactory

import scala.util.Success

object TransactionETLSetup {

  private lazy val logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val config = runnerContext.config
    val now = runnerContext.config.date

    val productionUser = "root"
    val jobOutputBucket = "chaordic-search-ignition-history"
    val finalBucket = "chaordic-search-ignition-latest"

    logger.info("Getting clients list")
    val allClients: Set[String] = executeRetrying(SearchApi.getClients())
//    val allClients = Set("saraiva-v5")
    logger.debug(s"Got clients: $allClients")



    allClients.foreach {
      client => {

        logger.info(s"Starting ETLTransaction for client $client")

        val transactions: RDD[Transaction] =
          sc.filterAndGetTextFiles(s"s3n://platform-dumps-virginia/buyOrders/*/$client",
            endDate = Option(now), startDate = Option(now.minusDays(30).withTimeAtStartOfDay())).map {
              rawTransaction => Chaordic.parseWith(rawTransaction, new TransactionParser)
          }.collect{ case Success(t) => t }.persist(StorageLevel.MEMORY_AND_DISK)

        logger.info(s"Parsed all transactions for client $client")

        val results = TransactionETL.process(sc, transactions)

        results.searchRevenue.map {
          x =>
            implicit val formats = org.json4s.DefaultFormats
            write(x)
        }.saveAsTextFile("/tmp/test/searchRevenue")

        logger.info(s"Saving search transactions for client $client")
        //fullXMLsPerPartition.saveAsTextFile(jobOutputPath, classOf[GzipCodec])

        results.participationRatio.map {
          x =>
            implicit val formats = org.json4s.DefaultFormats
            write(x)
        }.saveAsTextFile("/tmp/test/participationRatio")

        logger.info(s"Saving search participations for client $client")
      }
    }




  }
}
