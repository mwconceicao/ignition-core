package ignition.jobs

import ignition.chaordic.pojo.Parsers.{SearchLogParser, SearchClickLogParser, TransactionParser}
import ignition.chaordic.pojo.{SearchLog, SearchClickLog, Transaction}
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.chaordic.{Chaordic, ParsingReporter}
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.utils.DashboardAPI
import ignition.jobs.utils.DashboardAPI.{DashPoint, ResultPoint}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Interval}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait SearchETL {

  lazy val logger = LoggerFactory.getLogger("ignition.SearchETL")

  val aggregationLevel = "yyyy-MM-dd"

  def saveMetrics(kpi: String, start: DateTime, end: DateTime, points: Seq[DashPoint])(implicit ec: ExecutionContext): Future[Unit] = {
    DashboardAPI.deleteDailyFact("search", kpi, new Interval(start, end)).flatMap { response =>
      logger.info(s"Cleanup for kpi = $kpi, start = $start, end = $end")
      Future.sequence(points.map(point => saveResultPoint(kpi, point))).map { _ =>
        logger.info(s"All kpi $kpi saved")
      }
    }
  }

  def saveResultPoint(kpi: String, resultPoint: DashPoint)(implicit ec: ExecutionContext): Future[Unit] = {
    DashboardAPI.dailyFact("search", kpi, resultPoint).map { _ =>
      logger.info(s"Kpi $kpi for client ${resultPoint.client} at day ${resultPoint.day} with value ${resultPoint.value} saved.")
    }
  }

  def parseAutoCompleteLogs(context: SparkContext, start: DateTime, end: DateTime): RDD[SearchLog] =
    //SearchLogParser.parseSearchLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/autocompletelog/*/*.gz",
    SearchLogParser.parseSearchLogs(context.filterAndGetTextFiles("/Users/flavio/git/search-ignition/events/autocomplete/*/*.gz",
      startDate = Option(start), endDate = Option(end)))

  def parseSearchLogs(context: SparkContext, start: DateTime, end: DateTime): RDD[SearchLog] =
    //SearchLogParser.parseSearchLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/searchlog/*/*.gz",
    SearchLogParser.parseSearchLogs(context.filterAndGetTextFiles("/Users/flavio/git/search-ignition/events/searchlog/*/*.gz",
      startDate = Option(start), endDate = Option(end)))

  def parseClickLogs(context: SparkContext, start: DateTime, end: DateTime): RDD[SearchClickLog] =
    //SearchClickLogParser.parseSearchClickLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/clicklog/*/*.gz",
    SearchClickLogParser.parseSearchClickLogs(context.filterAndGetTextFiles("/Users/flavio/git/search-ignition/events/clicklog/*/*.gz",
      startDate = Option(start), endDate = Option(end)))

  def parseTransactions(context: SparkContext, start: DateTime, end: DateTime, clients: Set[String]): RDD[Transaction] =
    context.filterAndGetTextFiles(s"s3n://platform-dumps-virginia/buyOrders/*/{${clients.mkString(",")}}.gz",
      endDate = Option(end), startDate = Option(start)).map {
      json => Chaordic.parseWith(json, parser = new TransactionParser, reporter = reporterFor("transaction"))
    }.collect { case Success(parsed) => parsed }

  def parseDateOrElse(param: Option[String], default: DateTime) = param.map(DateTime.parse).getOrElse(default)

  def reporterFor(entityName: String) = new ParsingReporter {
    override def reportError(message: String, jsonStr: String): Unit =
      logger.trace("Failed to parse '{}' with error '{}' for entity {}", jsonStr, message, entityName)

    override def reportSuccess(jsonStr: String): Unit =
    // using Seq to solve scala ambiguous reference to overloaded definition
      logger.trace("Parsed successfully json '{}' to entity {}", Seq(jsonStr, entityName))
  }
}

