package ignition.jobs.utils

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.GZIPInputStream

import com.sksamuel.elastic4s.ElasticClient
import ignition.core.utils.S3Client
import ignition.jobs.TopQueriesJob.{QueryCount, TopQueries}
import ignition.jobs.{Configuration, SearchETL}
import org.elasticsearch.common.settings.ImmutableSettings
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.slf4j.LoggerFactory
import spray.httpx.SprayJsonSupport
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.io.{Codec, Source}
import scala.language.postfixOps
import scala.util.control.NonFatal

object Uploader extends SearchETL {

  private object UploaderSprayJsonFormatter extends DefaultJsonProtocol with SprayJsonSupport {

    implicit object DateTimeJsonFormat extends JsonFormat[DateTime] {
      def write(x: DateTime) = {
        require(x ne null)
        JsString(ISODateTimeFormat.dateTimeNoMillis.withZoneUTC.print(x))
      }
      def read(value: JsValue) = value match {
        case JsString(s) => try {
          ISODateTimeFormat.dateTimeParser.withZoneUTC.parseDateTime(s)
        } catch {
          case ex: IllegalArgumentException => deserializationError("Fail to parse DateTime string: " + s, ex)
        }
        case s => deserializationError("Expected JsString, but got " + s)
      }
    }

    implicit val kpiWithDashPointFormat = jsonFormat4(KpiWithDashPoint)
    implicit val rawQueryCountFormat = jsonFormat2(RawQueryCount)
    implicit val rawTopQueriesFormat = jsonFormat5(RawTopQueries)

  }

  import UploaderSprayJsonFormatter._
  import spray.json._

  private implicit lazy val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  private lazy val s3Client = new S3Client
  private lazy val logger = LoggerFactory.getLogger("ignition.UploaderCLI")

  private case class RawQueryCount(query: String, count: Int)

  private case class RawTopQueries(apiKey: String,
                                   datetime: String,
                                   queries_has_results: Boolean,
                                   event: String,
                                   top_queries: Seq[RawQueryCount]) {
    def convert = TopQueries(
      apiKey = apiKey,
      datetime = datetime,
      hasResult = queries_has_results,
      topQueries = top_queries.map(e => QueryCount(query = e.query, count = e.count)))
  }

  private case class UploaderConfig(eventType: String = "",
                                    path: String = "",
                                    server: String = Configuration.elasticSearchHost,
                                    clusterName: String = Configuration.elasticSearchClusterName,
                                    port: Int = Configuration.elasticSearchPort,
                                    bulkTimeoutInMinutes: Long = 30,
                                    dashboardSaveTimeoutInMinutes: Long = 20,
                                    bulkSize: Int = 50)

  def main (args: Array[String]) {
    val parser = new scopt.OptionParser[UploaderConfig]("Uploader") {
      help("help").text("prints this usage text")
      arg[String]("eventType") required() action { (x, c) => c.copy(eventType = x) } text "kpi | top-queries"
      arg[String]("path") required() action { (x, c) => c.copy(path = x) }  text "can be dir or file from 's3://<bucket>/<key>' or '/local/file/system'"
      opt[String]('s', "es-server") action { (x, c) => c.copy(server = x) }
      opt[String]('n', "es-cluster-name") action { (x, c) => c.copy(server = x) }
      opt[Int]('p', "es-port") action { (x, c) => c.copy(port = x) }
      opt[Long]('t', "es-save-timeout-minutes-per-bulk") action { (x, c) => c.copy(bulkTimeoutInMinutes = x) }
      opt[Int]('b', "es-bulk-size") action { (x, c) => c.copy(bulkSize = x) }
    }

    sys.addShutdownHook {
      ec.shutdown()
    }

    try {
      parser.parse(args, UploaderConfig()).map {
        case UploaderConfig("top-queries", path, server, clusterName, port, timeoutInMinutes, _, bulkSize) if path.nonEmpty =>
          val client = ElasticClient.remote(
            settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build(),
            addresses = (server, port))
          val timeout = Duration(timeoutInMinutes, TimeUnit.MINUTES)
          val topQueries = parseTopQueries(getLines(path))
          logger.info(s"Executing top-queries uploads to Elastic Search server: $clusterName, address: $server:$port")
          serialBulkSaveToElasticSearch(topQueries, bulkSize)(ec, timeout, client).map { bulks =>
            val message = bulks.filter(_.hasFailures).map(_.buildFailureMessage()).mkString("\\n")
            logger.warn("Save operation has some errors: \\n{}", message)
          }

        case UploaderConfig("kpi", path, _, _, _, _, dashboardSaveTimeoutInMinutes, _) if path.nonEmpty =>
          val kpis = parseKpis(getLines(path))
          logger.info("Executing kpi uploads to Dashboard API")
          Await.result(saveKpisToDashBoard(kpis), Duration(dashboardSaveTimeoutInMinutes, TimeUnit.MINUTES))

        case _ => throw new IllegalArgumentException(s"Invalid parameters: $parser")
      }
    } catch {
      case ex: IllegalArgumentException =>
        logger.error(ex.getMessage)
        sys.exit(1)

      case NonFatal(ex) =>
        logger.error("Unknown error", ex)
        sys.exit(1)
    }

    logger.info("done!")
    sys.exit()
  }

  private def parseTopQueries(lines: Seq[String]): Seq[TopQueries] = lines.map { line =>
    try { line.parseJson.convertTo[RawTopQueries].convert }
    catch {
      case ex: JsonParser.ParsingException =>
        logger.error("Parsing error: json = '', message: '{}'", line, ex.getMessage)
        sys.exit(1)
    }
  }

  private def parseKpis(lines: Seq[String]): Seq[KpiWithDashPoint] = lines.map { line =>
    try { line.parseJson.convertTo[KpiWithDashPoint] }
    catch {
      case ex: JsonParser.ParsingException =>
        logger.error("Parsing error: json = '', message: '{}'", line, ex.getMessage)
        sys.exit(1)
    }
  }

  private def getLines(path: String) = {
    implicit val codec = Codec.UTF8

    val isS3Path = path.startsWith("s3://")

    def getFiles(pathParam: String): Seq[File] = {
      val path = Paths.get(pathParam)
      if (Files.isDirectory(path)) path.toFile.listFiles()
      else if (Files.isRegularFile(path)) Seq(path.toFile)
      else throw new IllegalArgumentException(s"Invalid file path: '$path'")
    }

    def linesFromFiles(path: String): Seq[String] = {
      logger.info("Loading data from local file system, path: {}", path)
      getFiles(path).flatMap { file =>
        logger.info("File: {}", file.getAbsolutePath)
        val inputStream = if (file.getAbsolutePath.endsWith("gz"))
          new GZIPInputStream(new FileInputStream(file))
        else
          new FileInputStream(file)
        Source.fromInputStream(inputStream)(codec).getLines()
      }
    }

    val s3Pattern = "(s3?://)?([a-zA-Z0-9\\-]+)/(.*)".r

    def linesFromS3(path: String): Seq[String] = path match {
      case s3Pattern(_, bucket, key) => {
        logger.info("Loading data from S3, path: {}", path)
        s3Client.list(bucket, key).flatMap { unload =>
          logger.info("S3 object: {}", unload.getKey)
          val s3Object = s3Client.readContent(bucket, unload.getKey)
          val inputStream = if (s3Object.getKey.endsWith("gz"))
            new GZIPInputStream(s3Object.getDataInputStream)
          else
            s3Object.getDataInputStream
          Source.fromInputStream(inputStream)(codec).getLines()
        }
      }
      case _ => throw new IllegalArgumentException(s"Invalid S3 path = '$path'")
    }

    if (isS3Path) linesFromS3(path) else linesFromFiles(path)
  }

}
