package ignition.jobs.utils

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors

import ignition.chaordic.utils.Json
import ignition.jobs.{Configuration, SearchETL}
import ignition.jobs.TopQueriesJob.TopQueries
import ignition.jobs.utils.DashboardAPI.DashPoint
import org.elasticsearch.common.settings.ImmutableSettings
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.DocumentSource

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

object Uploader extends SearchETL {

  private lazy val logger = LoggerFactory.getLogger("ignition.search.Uploader")

  private implicit lazy val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  private lazy val defaultClient = buildClient(Configuration.elasticSearchHost,
    Configuration.elasticSearchClusterName, Configuration.elasticSearchPort)

  private case class UploaderConfig(eventType: Option[String] = None,
                                    path: Option[String] = None,
                                    server: Option[String] = None,
                                    clusterName: Option[String] = None,
                                    port: Option[Int] = None)

  def main (args: Array[String]) {
    val parser = new scopt.OptionParser[UploaderConfig]("Uploader") {
      help("help").text("prints this usage text")
      arg[String]("eventType ( kpi | top-queries )") required() action { (x, c) =>
        c.copy(eventType = Option(x))
      }
      arg[String]("path") required() action { (x, c) =>
        c.copy(path = Option(x))
      }
      opt[String]('s', "es-server") action { (x, c) =>
        c.copy(server = Option(x))
      }
      opt[String]('n', "es-cluster-name") action { (x, c) =>
        c.copy(server = Option(x))
      }
      opt[Int]('p', "es-port") action { (x, c) =>
        c.copy(port = Option(x))
      }
    }

    parser.parse(args, UploaderConfig()).map {
      case UploaderConfig(Some("top-queries"), path, server, clusterName, port) =>
        val client = buildClient(server = server.getOrElse(Configuration.elasticSearchHost),
          clusterName = clusterName.getOrElse(Configuration.elasticSearchClusterName),
          port = port.getOrElse(Configuration.elasticSearchPort))
        executeSaveToElasticSearch(parseFromFiles[TopQueries](path.get), client)

      case UploaderConfig(Some("kpi"), Some(path), _, _, _) =>
        saveKpisToDashBoard(parseFromFiles[KpiWithDashPoint](path))

      case _ => throw new RuntimeException("Invalid parameters")
    }
  }

  private def buildClient(server: String, clusterName: String, port: Int) =
    ElasticClient.remote(ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build(), server, port)

  private def executeSaveToElasticSearch(topQueries: Seq[TopQueries], client: ElasticClient = defaultClient)
                                        (implicit ec: ExecutionContext): Future[Unit] = {
    val requests = topQueries.map { event =>
//      client.execute { index into event.indexName doc event.documentSource }
      Future { logger.info("mock executeSaveToElasticSearch") }
    }
    Future.sequence(requests).map(_ => ())
  }

  private def parseFromFiles[T](path: String)(implicit mf: Manifest[T]): Seq[T] =
    getFiles(path).flatMap { file =>
      Source.fromFile(file).getLines().map(line => Json.parseRaw[T](line))
    }

  private def getFiles(pathParam: String): Seq[File] = {
    val path = Paths.get(pathParam)
    if (Files.isDirectory(path)) path.toFile.listFiles()
    else if (Files.isRegularFile(path)) Seq(path.toFile)
    else throw new RuntimeException(s"Invalid file path: $path")
  }

  private implicit class TopQueriesElasticSearchUtils(topQueries: TopQueries) {
    def indexName: String = s"etl-top-queries-${topQueries.day}"
    def documentSource = new DocumentSource {
      override def json: String = Json.toJsonString(topQueries)
    }
  }

  case class KpiWithDashPoint(kpi: String, start: DateTime, end: DateTime, point: DashPoint)

  def saveTopQueriesToElasticSearch(topQueries: Seq[TopQueries])(implicit ec: ExecutionContext): Future[Unit] =
    executeSaveToElasticSearch(topQueries, defaultClient)

  def saveKpisToDashBoard(kpis: Seq[KpiWithDashPoint])(implicit ec: ExecutionContext): Future[Unit] = {
    val requests = kpis.groupBy(kpi => (kpi.kpi, kpi.start, kpi.end)).map {
      case ((kpi, start, end), points) =>
        Future { logger.info("mock saveKpisToDashBoard") }
//        saveMetrics(kpi, start, end, points.map(_.point))

    }
    Future.sequence(requests).map(_ => ())
  }

}
