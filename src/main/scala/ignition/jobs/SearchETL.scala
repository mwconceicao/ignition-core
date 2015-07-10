package ignition.jobs

import ignition.chaordic.pojo.Parsers.{SearchClickLogParser, SearchLogParser, TransactionParser}
import ignition.chaordic.pojo.{SearchClickLog, SearchLog, Transaction}
import ignition.chaordic.utils.ChaordicPathDateExtractor._
import ignition.chaordic.Chaordic
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.utils.{EntitiesLayer, DashboardAPI}
import ignition.jobs.utils.DashboardAPI.DashPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{Days, DateTime, Interval}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait SearchETL {

  private lazy val logger = LoggerFactory.getLogger("ignition.SearchETL")

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

  def parseAutoCompleteLogs(setupName: String, context: SparkContext, start: DateTime, end: DateTime): RDD[SearchLog] =
    EntitiesLayer.parseSearchLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/autocompletelog/*/*.gz",
      startDate = Option(start), endDate = Option(end)), setupName)

  def parseSearchLogs(setupName: String, context: SparkContext, start: DateTime, end: DateTime): RDD[SearchLog] =
    EntitiesLayer.parseSearchLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/searchlog/*/*.gz",
      startDate = Option(start), endDate = Option(end)), setupName)

  def parseClickLogs(setupName: String, context: SparkContext, start: DateTime, end: DateTime): RDD[SearchClickLog] =
    EntitiesLayer.parseSearchClickLogs(context.filterAndGetTextFiles("s3n://chaordic-search-logs/clicklog/*/*.gz",
      startDate = Option(start), endDate = Option(end)), setupName)

  def parseTransactions(setupName: String, context: SparkContext, start: DateTime, end: DateTime, clients: Set[String]): RDD[Transaction] = {
    require(start.isBefore(end), s"Start = $start must be before end = $end")
    val paths = for { date <- dateRangeByDay(start, end) } yield {
      s"s3n://platform-dumps-virginia/buyOrders/${date.toString("yyyy-MM-dd")}/*.gz"
    }
    EntitiesLayer.parseTransactions(context.getTextFiles(paths), setupName)
  }

  def dateRangeByDay(start: DateTime, end: DateTime): Seq[DateTime] = {
    val interval = Days.daysBetween(start, end).getDays
    (0 to interval).map(start.withTimeAtStartOfDay.plusDays)
  }

  def parseDateOrElse(param: Option[String], default: DateTime) = param.map(DateTime.parse).getOrElse(default)

}

