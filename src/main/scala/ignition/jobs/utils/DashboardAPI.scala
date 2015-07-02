package ignition.jobs.utils

import akka.actor.ActorSystem
import ignition.jobs.ResultPoint
import org.joda.time.Interval
import spray.client.pipelining.{SendReceive, _}
import spray.http._
import spray.http.ContentTypes.`application/json`
import spray.httpx.SprayJsonSupport._

import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps}

object DashboardAPI {

  import ignition.jobs.TransactionETLProtocol._

  implicit val system = ActorSystem("dashboard-api")
  import system.dispatcher

  // TODO externalize this
  val dashboardId = "mail"
  val passwd = "#sc0rsese!"
  val baseHref = "https://dashboard-api-test.chaordicsystems.com/v2/kpi"

    /**
     * Send a Daily Fact to DashboardAPI
     *
     * @param product Product identification on dashboard
     * @param kpi KPI to send metric
     * @param resultPoint Metric to send
     *
     * @return a future with the result of this operation.
     */
  def dailyFact(product: String, kpi: String, resultPoint: ResultPoint): Future[Unit] = {
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
    addCredentials(BasicHttpCredentials(dashboardId, passwd))
      ~> sendReceive
    )

}
