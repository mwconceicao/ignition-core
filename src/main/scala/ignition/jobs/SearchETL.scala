package ignition.jobs

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.DocumentSource
import ignition.chaordic.pojo.{SearchClickLog, SearchLog, Transaction}
import ignition.chaordic.utils.Json
import ignition.core.jobs.utils.SparkContextUtils._
import ignition.jobs.TopQueriesJob.TopQueries
import ignition.jobs.utils.DashboardAPI.DashPoint
import ignition.jobs.utils.{DashboardAPI, EntitiesLayer}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.common.settings.ImmutableSettings
import org.joda.time.{DateTime, Days, Interval}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object SearchETL extends SearchETL

trait SearchETL {

  private lazy val logger = LoggerFactory.getLogger("ignition.SearchETL")

  implicit lazy val defaultClient = ElasticClient.remote(
    settings = ImmutableSettings.settingsBuilder().put("cluster.name", Configuration.elasticSearchClusterName).build(),
    addresses = (Configuration.elasticSearchHost, Configuration.elasticSearchPort))

  val aggregationLevel = "yyyy-MM-dd"

  case class KpiWithDashPoint(kpi: String, start: DateTime, end: DateTime, point: DashPoint)

  def saveMetrics(kpi: String, start: DateTime, end: DateTime, points: Seq[DashPoint])(implicit ec: ExecutionContext): Future[Unit] = {
    DashboardAPI.deleteDailyFact("search", kpi, new Interval(start, end)).flatMap { response =>
      logger.info(s"Cleanup for kpi = $kpi, start = $start, end = $end")
      Future.sequence(points.map(point => saveResultPoint(kpi, point))).map { _ =>
        logger.info(s"All kpi $kpi saved")
      }
    }
  }

  private implicit class TopQueriesElasticSearchUtils(topQueries: TopQueries) {
    def indexName: String = s"etl-top-queries-${topQueries.datetime}"
    def documentSource = new DocumentSource {
      override def json: String = Json.toJsonString(topQueries)
    }
  }

  def executeSaveElasticSearch(topQueries: Seq[TopQueries])
                              (implicit ec: ExecutionContext, client: ElasticClient): Future[BulkResponse] =
    client.execute {
      bulk(topQueries.map { event => index into event.indexName doc event.documentSource }:_*)
    }

  def serialBulkSaveToElasticSearch(topQueries: Seq[TopQueries], bulkSize: Int = 50)
                                   (implicit ec: ExecutionContext, timeout: Duration, client: ElasticClient): Future[Iterator[BulkResponse]] =
    Future {
      topQueries.grouped(bulkSize).map { bulkQueries =>
        val fRequest = executeSaveElasticSearch(bulkQueries)(ec, client)
        Await.result(fRequest, timeout)
      }
    }

  def  saveKpisToDashBoard(kpis: Seq[KpiWithDashPoint])(implicit ec: ExecutionContext): Future[Unit] = {
    val requests = kpis.groupBy(kpi => (kpi.kpi, kpi.start, kpi.end)).map {
      case ((kpi, start, end), points) => saveMetrics(kpi, start, end, points.map(_.point))
    }
    Future.sequence(requests).map(_ => ())
  }

  def saveResultPoint(kpi: String, resultPoint: DashPoint)(implicit ec: ExecutionContext): Future[Unit] =
    DashboardAPI.dailyFact("search", kpi, resultPoint).map { _ =>
      logger.info(s"Kpi $kpi for client ${resultPoint.client} at day ${resultPoint.day} with value ${resultPoint.value} saved.")
    }

  def parseAutoCompleteLogs(setupName: String, context: SparkContext, start: DateTime, end: DateTime): RDD[SearchLog] = {
    require(start.isBefore(end), s"Start = $start must be before end = $end")
    val paths = for { date <- dateRangeByDay(start, end) } yield {
      s"s3n://chaordic-search-logs/autocompletelog/${date.toString("yyyy-MM-dd")}/*.gz"
    }
    EntitiesLayer.parseSearchLogs(context.getTextFiles(paths), setupName)
  }

  def parseSearchLogs(setupName: String, context: SparkContext, start: DateTime, end: DateTime): RDD[SearchLog] = {
    require(start.isBefore(end), s"Start = $start must be before end = $end")
    val paths = for { date <- dateRangeByDay(start, end) } yield {
      s"s3n://chaordic-search-logs/searchlog/${date.toString("yyyy-MM-dd")}/*.gz"
    }
    EntitiesLayer.parseSearchLogs(context.getTextFiles(paths), setupName)
  }

  def parseClickLogs(setupName: String, context: SparkContext, start: DateTime, end: DateTime): RDD[SearchClickLog] = {
    require(start.isBefore(end), s"Start = $start must be before end = $end")
    val paths = for { date <- dateRangeByDay(start, end) } yield {
      s"s3n://chaordic-search-logs/clicklog/${date.toString("yyyy-MM-dd")}/*.gz"
    }
    EntitiesLayer.parseSearchClickLogs(context.getTextFiles(paths), setupName)
  }

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

