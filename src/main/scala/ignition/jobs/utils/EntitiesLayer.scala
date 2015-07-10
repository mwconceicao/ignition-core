package ignition.jobs.utils

import ignition.chaordic.pojo.{Transaction, SearchClickLog, Parsers, SearchLog}
import ignition.chaordic.{Chaordic, ParsingReporter}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.Success

object EntitiesLayer {

  private lazy val logger = LoggerFactory.getLogger("ignition.search.EntitiesLayer")

  private lazy val jsonApiKeyRegex = """apiKey" *: *"([\p{Alnum}-]+)"""".r
  private lazy val jsonClientRegex = """client" *: *"([\p{Alnum}-]+)"""".r

  def reporterFor(setupName: String, entityName: String) = new ParsingReporter {
    override def reportError(message: String, jsonStr: String) {
      val guessedApiKey = jsonApiKeyRegex.findFirstMatchIn(jsonStr) orElse jsonClientRegex.findFirstMatchIn(jsonStr) match {
        case Some(m) if m.groupCount > 0 => m.group(1)
        case _ => "unknown"
      }
      Telemetry.errorDetected(s"parsing.failed.$setupName.$entityName.$guessedApiKey.source")
      logger.warn("Failed to parse '{}' with error '{}' for entity {} on {}", jsonStr, message, entityName, setupName)
    }

    override def reportSuccess(jsonStr: String): Unit = {
      logger.trace("Parsed successfully json '{}' to entity {} for setup {}", jsonStr, entityName, setupName)
    }
  }

  def parseSearchLogs(rdd: RDD[String], setupName: String): RDD[SearchLog] = rdd
    .map { line => Chaordic.parseWith(line, new Parsers.SearchLogParser, reporterFor("searchLog", setupName)) }
    .collect { case Success(log) => log }

  def parseSearchClickLogs(rdd: RDD[String], setupName: String): RDD[SearchClickLog] = rdd
    .map { line => Chaordic.parseWith(line, new Parsers.SearchClickLogParser, reporterFor("searchClickLog", setupName)) }
    .collect { case Success(log) => log }

  def parseTransactions(rdd: RDD[String], setupName: String): RDD[Transaction] = rdd
    .map { line => Chaordic.parseWith(line, new Parsers.TransactionParser, reporterFor("transaction", setupName)) }
    .collect { case Success(transaction) => transaction }

}
