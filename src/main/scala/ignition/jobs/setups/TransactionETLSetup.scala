package ignition.jobs.setups

import ignition.chaordic.Chaordic
import ignition.chaordic.pojo.Parsers.TransactionParser
import ignition.chaordic.pojo.Transaction
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.chaordic.utils.Json
import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.TransactionETL
import ignition.jobs.setups.SitemapXMLSetup._
import ignition.jobs.utils.SearchApi
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object TransactionETLSetup {

  private lazy val logger = LoggerFactory.getLogger("ignition.TransactionETLSetup")

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val now: DateTime = runnerContext.config.date

    val productionUser = runnerContext.config.user
    val config = runnerContext.config
    val jobOutputBucket = "chaordic-search-ignition-history"
    val jobPath = s"$jobOutputBucket/${config.setupName}/$productionUser"

    logger.info("Getting clients list")
    val allClients: Set[String] = executeRetrying(SearchApi.getClients())
    logger.info(s"Got clients: $allClients")

    allClients
      .map(client => getTransactions(runnerContext, client))
      .collect { case Success((client, transactions)) => (client, transactions)}
      .foreach {
        case (client, transactions) =>
          logger.info(s"Starting ETLTransaction for client $client")
          val results = TransactionETL.process(sc, transactions, client)

          logger.info(s"Saving search transactions for client $client")
          results.searchRevenue.map(Json.toJsonString(_))
            .saveAsTextFile(s"s3://$jobPath/searchRevenue/${config.tag}/$client")

          logger.info(s"Saving search participations for client $client")
          results.participationRatio.map(Json.toJsonString(_))
            .saveAsTextFile(s"s3://$jobPath/participationRatio/${config.tag}/$client")

          logger.info(s"ETLTransaction finished for client $client")
    }

    logger.info("TransactionETL - GREAT SUCCESS")
  }

  def getTransactions(rc: RunnerContext, client: String): Try[(String, RDD[Transaction])] = {
    val now = rc.config.date
    Try {
      client -> rc.sparkContext.filterAndGetTextFiles(s"s3n://platform-dumps-virginia/buyOrders/2015-06*/$client.gz",
        endDate = Option(now), startDate = Option(now.minusDays(1).withTimeAtStartOfDay())).map {
        rawTransaction => Chaordic.parseWith(rawTransaction, new TransactionParser)
      }.collect { case Success(t) => t }.persist(StorageLevel.MEMORY_AND_DISK)
    } recoverWith {
      case NonFatal(exception) =>
        logger.error(s"Could not parse BuyOrders for client $client", exception)
        Failure(exception)
    }
  }
}

