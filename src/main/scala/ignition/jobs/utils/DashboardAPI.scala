package ignition.jobs.utils

import ignition.jobs.ResultPoint
import spray.client.pipelining.{SendReceive, _}
import spray.http.BasicHttpCredentials

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.{implicitConversions, postfixOps}


object DashboardAPI {

  val dashboardId = ""
  val passwd = ""

  val baseHref = "https://dashboard-api-test.chaordicsystems.com/v2/kpi"

  //search/kpi_test
  def dailyFact(product: String, kpi: String, resultPoint: ResultPoint) = {
    dashboardPipeline(Post(s"$baseHref/$product/$kpi", resultPoint))
  }

  val dashboardPipeline: SendReceive = (
    addCredentials(BasicHttpCredentials(dashboardId, passwd))
      ~> sendReceive
    )
}

