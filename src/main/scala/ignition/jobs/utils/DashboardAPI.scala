package ignition.jobs.utils

import akka.actor.ActorSystem
import ignition.jobs.ResultPoint
import spray.client.pipelining.{SendReceive, _}
import spray.http.BasicHttpCredentials
import spray.httpx.SprayJsonSupport._

import scala.language.{implicitConversions, postfixOps}

object DashboardAPI {

  import ignition.jobs.TransactionETLProtocol._

  implicit val system = ActorSystem("dashboard-api")
  import system.dispatcher

  val dashboardId = ""
  val passwd = ""

  val baseHref = "https://dashboard-api-test.chaordicsystems.com/v2/kpi"

  //search/kpi_test
  def dailyFact(product: String, kpi: String, resultPoint: ResultPoint) = {
    // TODO: need to change key to day or create a case class for sending it
    dashboardPipeline(Post(s"$baseHref/$product/$kpi", resultPoint))
  }

  val dashboardPipeline: SendReceive = (
    addCredentials(BasicHttpCredentials(dashboardId, passwd))
      ~> sendReceive
    )
}

