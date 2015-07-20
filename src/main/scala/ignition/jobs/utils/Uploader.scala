package ignition.jobs.utils

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import ignition.chaordic.Chaordic
import ignition.core.utils.S3Client
import ignition.jobs.SearchETL.KpiWithDashPoint
import ignition.jobs.TopQueriesJob.{QueryCount, TopQueries}
import ignition.jobs.ValidQueriesJob.{ValidQuery, ValidQueryFinal}
import ignition.jobs.{Configuration, SearchETL}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.slf4j.LoggerFactory
import spray.httpx.SprayJsonSupport
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.io.{Codec, Source}
import scala.language.postfixOps
import scala.util.{Failure, Success}
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
    implicit val rawValidQueryFormat = jsonFormat9(RawValidQuery)
    implicit val rawValidQueriesFormat = jsonFormat11(RawValidQueries)

  }

  import UploaderSprayJsonFormatter._
  import spray.json._

  private lazy val s3Client = new S3Client
  private lazy val logger = LoggerFactory.getLogger("ignition.UploaderCLI")

  implicit lazy val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  implicit lazy val actorSystem = ActorSystem("uploader")

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

  private case class RawValidQuery(raw_ctr: Double,
                                   apiKey: String,
                                   average_results: Long,
                                   latest_search_log_results: Int,
                                   latest_search_log: String,
                                   latest_search_log_feature: String,
                                   searchs: Long,
                                   query: String,
                                   clicks: Long) {
    def convert: ValidQuery = ValidQuery(
      apiKey = apiKey,
      query = query,
      searches = searchs,
      clicks = clicks,
      rawCtr = raw_ctr,
      latestSearchLog = Chaordic.parseDate(latest_search_log),
      latestSearchLogResults = latest_search_log_results,
      latestSearchLogFeature = latest_search_log_feature,
      sumResults = 0, // not used
      averageResults = average_results)
  }

  private case class RawValidQueries(top_query: String,
                                     apiKey: String,
                                     raw_ctr: Int,
                                     tokens: Seq[String],
                                     latest_search_log_results: Int,
                                     searchs: Long,
                                     active: Boolean,
                                     average_results: Long,
                                     latest_search_log: String,
                                     queries: Seq[RawValidQuery],
                                     clicks: Long) {
    def convert: ValidQueryFinal = ValidQueryFinal(
      apiKey = apiKey,
      tokens = tokens,
      topQuery = top_query,
      searches = searchs,
      clicks = clicks,
      rawCtr = raw_ctr,
      latestSearchLog = Chaordic.parseDate(latest_search_log),
      latestSearchLogResults = latest_search_log_results,
      averageResults = average_results,
      queries = queries.map(_.convert),
      active = active)
  }

  private case class UploaderConfig(eventType: String = "",
                                    path: String = "",
                                    server: String = Configuration.elasticSearchHost,
                                    port: Int = Configuration.elasticSearchPort,
                                    bulkTimeoutInMinutes: Int = 5,
                                    bulkSize: Int = 50,
                                    validQueriesIndexConfigPath: Option[String] = None)

  def main (args: Array[String]) {
    val parser = new scopt.OptionParser[UploaderConfig]("Uploader") {
      help("help").text("prints this usage text")
      arg[String]("eventType") required() action { (x, c) => c.copy(eventType = x) } text "(kpi | top-queries | valid-queries)"
      arg[String]("path") required() action { (x, c) => c.copy(path = x) }  text "can be dir or file from 's3://<bucket>/<key>' or '/local/file/system'"
      opt[String]('j', "valid-queries-json-config") action { (x, c) => c.copy(validQueriesIndexConfigPath = Option(x)) }
      opt[String]('s', "es-server") action { (x, c) => c.copy(server = x) }
      opt[String]('n', "es-cluster-name") action { (x, c) => c.copy(server = x) }
      opt[Int]('p', "es-port") action { (x, c) => c.copy(port = x) }
      opt[Int]('t', "es-save-timeout-minutes-per-bulk") action { (x, c) => c.copy(bulkTimeoutInMinutes = x) }
      opt[Int]('b', "es-bulk-size") action { (x, c) => c.copy(bulkSize = x) }
    }

    sys.addShutdownHook {
      ec.shutdown()
    }

    try {
      parser.parse(args, UploaderConfig()).foreach {
        case UploaderConfig("top-queries", path, server, port, timeoutInMinutes, bulkSize, _) if path.nonEmpty =>
          runTopQueries(path, server, port, timeoutInMinutes, bulkSize)

        case UploaderConfig("valid-queries", path, server, port, timeoutInMinutes, bulkSize, config) if path.nonEmpty =>
          runValidQueries(path, server, port, timeoutInMinutes, bulkSize, config)

        case UploaderConfig("kpi", path, _, _, _, timeoutInMinutes, _) if path.nonEmpty =>
          runKpis(path, timeoutInMinutes)

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

  private def runKpis(path: String, timeoutInMinutes: Int): Unit = {
    val kpis = parseLines[KpiWithDashPoint](getLines(path))
    logger.info("Executing kpi uploads to Dashboard API")
    Await.result(saveKpisToDashBoard(kpis), Duration(timeoutInMinutes, TimeUnit.MINUTES))
  }

  private def runTopQueries(path: String, server: String, port: Int, timeoutInMinutes: Int, bulkSize: Int): Unit = {
    val timeout = Duration(timeoutInMinutes, TimeUnit.MINUTES)
    val topQueries = parseLines[RawTopQueries](getLines(path))
    logger.info(s"Executing top-queries uploads to Elastic Search server: $server:$port")
    val client = new ElasticSearchClient(server, port)
    val indexOperation = client.serialSaveTopQueries(topQueries.map(_.convert), bulkSize)(timeout)
    indexOperation.onComplete {
      case Success(result) =>
        logger.info(s"Top-queries saved to elastic-search! Details: $result")
      case Failure(ex) =>
        logger.error("Fail to bulk index top queries", ex)
        sys.exit(1)
    }
    Await.result(indexOperation, timeout * topQueries.size)
  }

  private def runValidQueries(path: String, server: String, port: Int, timeoutInMinutes: Int, bulkSize: Int, jsonIndexConfig: Option[String]): Unit = {
    val timeout = Duration(timeoutInMinutes, TimeUnit.MINUTES)
    val validQueries = parseLines[RawValidQueries](getLines(path))
    logger.info(s"Executing valid-queries uploads to Elastic Search server: $server:$port")
    val client = new ElasticSearchClient(server, port)
    val configContent = jsonIndexConfig.map(getClass.getResource).getOrElse(getClass.getResource("/valid_queries_index_configuration.json"))
    val indexOperation = client.saveValidQueries(validQueries.map(_.convert), Source.fromURL(configContent).mkString, bulk = bulkSize)(timeout)
    indexOperation.onComplete {
      case Success(result) =>
        logger.info(s"Top-queries saved to elastic-search! Details: $result")
      case Failure(ex) =>
        logger.error("Fail to bulk index top queries", ex)
        sys.exit(1)
    }
    Await.result(indexOperation, timeout * validQueries.size)
  }

  private def parseLines[T](lines: Seq[String])(implicit reader: JsonReader[T]): Seq[T] = lines.map { line =>
    try { line.parseJson.convertTo[T] }
    catch {
      case ex: JsonParser.ParsingException =>
        logger.error("Parsing error: json = '', message: '{}'", line, ex.getMessage)
        sys.exit(1)
    }
  }

  private def getLines(path: String): Seq[String] = {
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
      case s3Pattern(_, bucket, key) =>
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
      case _ => throw new IllegalArgumentException(s"Invalid S3 path = '$path'")
    }

    if (isS3Path) linesFromS3(path) else linesFromFiles(path)
  }

}
