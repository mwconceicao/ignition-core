package ignition.jobs

import ignition.chaordic.pojo.Parsers.{SearchLogParser, SearchClickLogParser, TransactionParser}
import ignition.chaordic.pojo.{SearchLog, SearchClickLog, Transaction}
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.chaordic.Chaordic
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.utils.DashboardAPI
import ignition.jobs.utils.DashboardAPI.ResultPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{Days, DateTime, Interval}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait SearchETL {

  private lazy val logger = LoggerFactory.getLogger("ignition.SearchETL")

  val aggregationLevel = "yyyy-MM-dd"

  def saveMetrics(kpi: String, start: DateTime, end: DateTime, points: Seq[ResultPoint])(implicit ec: ExecutionContext): Future[Unit] = {
    DashboardAPI.deleteDailyFact("search", kpi, new Interval(start, end)).flatMap { response =>
      logger.info(s"Cleanup for kpi = $kpi, start = $start, end = $end")
      Future.sequence(points.map(point => saveResultPoint(kpi, point))).map { _ =>
        logger.info(s"All kpi $kpi saved")
      }
    }
  }

  def saveResultPoint(kpi: String, resultPoint: ResultPoint)(implicit ec: ExecutionContext): Future[Unit] = {
    DashboardAPI.dailyFact("search", kpi, resultPoint).map { _ =>
      logger.info(s"Kpi $kpi for client ${resultPoint.client} at day ${resultPoint.day} with value ${resultPoint.value} saved.")
    }
  }

  def parseAutoCompleteLogs(context: SparkContext, start: DateTime, end: DateTime): RDD[SearchLog] =
    SearchLogParser.parseSearchLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/autocompletelog/*/*.gz",
      startDate = Option(start), endDate = Option(end)))

  def parseSearchLogs(context: SparkContext, start: DateTime, end: DateTime): RDD[SearchLog] =
    SearchLogParser.parseSearchLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/searchlog/*/*.gz",
      startDate = Option(start), endDate = Option(end)))

  def parseClickLogs(context: SparkContext, start: DateTime, end: DateTime): RDD[SearchClickLog] =
    SearchClickLogParser.parseSearchClickLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/clicklog/*/*.gz",
      startDate = Option(start), endDate = Option(end)))

  def parseTransactions(context: SparkContext, start: DateTime, end: DateTime, clients: Set[String]): RDD[Transaction] = {
    require(start.isBefore(end), s"Start = $start must be before end = $end")
    val paths = for { date <- dateRangeByDay(start, end) } yield {
      s"s3n://platform-dumps-virginia/buyOrders/${date.toString("yyyy-MM-dd")}/*.gz"
    }
    context.getTextFiles(paths)
      .map(json => Chaordic.parseWith(json, parser = new TransactionParser))
      .collect { case Success(parsed) => parsed }
      .filter(transaction => clients.contains(transaction.apiKey))
  }

  def dateRangeByDay(start: DateTime, end: DateTime): Seq[DateTime] = {
    val interval = Days.daysBetween(start, end).getDays
    (0 to interval).map(start.withTimeAtStartOfDay.plusDays)
  }

  def parseDateOrElse(param: Option[String], default: DateTime) = param.map(DateTime.parse).getOrElse(default)

}

