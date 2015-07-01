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

  val dashboardId = ""
  val passwd = ""

  val baseHref = "https://dashboard-api-test.chaordicsystems.com/v2/kpi"

  //search/kpi_test
    /**
     * Send a Daily Fact to DashboardAPI
     *
     * @param product Product identification on dashboard
     * @param kpi KPI to send metric
     * @param resultPoint Metric to send
     *
     * @return a future with the result of this operation.
     */
  def dailyFact(product: String, kpi: String, resultPoint: ResultPoint): Future[HttpResponse] = {
    // TODO: need to change key to day or create a case class for sending it
    dashboardPipeline(Post(s"$baseHref/$product/$kpi", resultPoint))
  }

  /**
   * Delete multiple metrics from DashboardAPI
   *
   * @param product Product identification on dashboard
   * @param kpi KPI to delete metric
   * @param clients Set of clients
   * @param interval Interval to delete
   * @return a future with the result of this operation
   */

  def deleteDailyFact(product: String, kpi: String, clients: Set[String],
                       interval: Interval): Future[HttpResponse] = {
    dashboardPipeline(
      Delete(s"""$baseHref/$product/$kpi?from=${interval.getStart}&to=${interval.getEnd}&client=${clients.mkString(",")}"""))
  }

  val dashboardPipeline: SendReceive = (
    addHeader(HttpHeaders.`Content-Type`(`application/json`)) ~>
    addCredentials(BasicHttpCredentials(dashboardId, passwd))
      ~> sendReceive
    )

}

