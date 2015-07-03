package ignition.jobs.utils

import akka.actor.ActorSystem
import ignition.jobs.Configuration
import ignition.jobs.utils.DashboardAPI.{DashPoint, FeaturedResultPoint, ResultPoint}
import org.joda.time.Interval
import spray.client.pipelining.{SendReceive, _}
import spray.http._
import spray.http.ContentTypes.`application/json`
import spray.httpx.SprayJsonSupport._
import spray.json.{JsValue, RootJsonFormat, DefaultJsonProtocol}

import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps}


object DashboardAPIProtocol extends DefaultJsonProtocol {
  implicit val resultPointFormat = jsonFormat3(ResultPoint)
  implicit val resultPointWithFeatureFormat = jsonFormat4(FeaturedResultPoint)
}

object DashboardAPI {

  import DashboardAPIProtocol._

  sealed trait DashPoint {
    val client: String
    val day: String
    val value: Double
  }

  case class ResultPoint(client: String, day: String, value: Double) extends DashPoint
  case class FeaturedResultPoint(client: String, day: String, value: Double,
                                 feature: String) extends DashPoint

  implicit object DashPointFormat extends RootJsonFormat[DashPoint] {
    import spray.json._
    override def read(json: JsValue): DashPoint = json.asJsObject.getFields("feature") match {
      case seq if seq.size == 1 => json.convertTo[FeaturedResultPoint]
      case _ => json.convertTo[ResultPoint]
    }

    override def write(obj: DashPoint): JsValue = obj match {
      case point: ResultPoint => point.toJson
      case point: FeaturedResultPoint => point.toJson
    }
  }

  implicit val system = ActorSystem("dashboard-api")
  import system.dispatcher

  val user = Configuration.dashboardApiUser
  val password = Configuration.dashboardApiPassword
  val baseHref = s"${Configuration.dashboardApiUrl}/v2/kpi"

  /**
   * Send a Daily Fact to DashboardAPI
   *
   * @param product Product identification on dashboard
   * @param kpi KPI to send metric
   * @param resultPoint Metric to send
   *
   * @return a future with the result of this operation.
   */
  def dailyFact(product: String, kpi: String, resultPoint: DashPoint): Future[Unit] = {
    dashboardPipeline(Post(s"$baseHref/$product/$kpi", resultPoint)).flatMap { response =>
      if (response.status.isSuccess)
        Future.successful()
      else
        Future.failed(new RuntimeException(s"Fail to save product = $product, kpi = $kpi, resultPoint = $resultPoint"))
    }
  }

  /**
   * Delete multiple metrics from DashboardAPI
   *
   * @param product Product identification on dashboard
   * @param kpi KPI to delete metric
   * @param interval Interval to delete
   * @return a future with the result of this operation
   */

  def deleteDailyFact(product: String, kpi: String, interval: Interval): Future[Unit] = {
    val url = s"""$baseHref/$product/$kpi?from=${interval.getStart.toString("yyyy-MM-dd")}&to=${interval.getEnd.toString("yyyy-MM-dd")}"""
    dashboardPipeline(Delete(url)).flatMap { response =>
      if (response.status.isSuccess)
        Future.successful()
      else
        Future.failed(new RuntimeException(s"Fail to cleanup metrics for product = $product, kpi = $kpi, interval = $interval"))
    }
  }

  val dashboardPipeline: SendReceive = (
    addHeader(HttpHeaders.`Content-Type`(`application/json`)) ~>
    addCredentials(BasicHttpCredentials(user, password))
      ~> sendReceive
    )

}
