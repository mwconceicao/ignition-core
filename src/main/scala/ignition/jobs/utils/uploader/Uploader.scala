package ignition.jobs.utils.uploader

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import ignition.core.utils.S3Client
import ignition.jobs.SearchETL.KpiWithDashPoint
import ignition.jobs.pojo.{RawTopQueries, RawValidQueries, TopQueriesSprayJsonFormatter, ValidQueriesSprayJsonFormatter}
import ignition.jobs.utils.ElasticSearchClient
import ignition.jobs.{DashboardAPISprayJsonFormatter, SearchETL}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.io.{Codec, Source}
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object Uploader extends SearchETL {



  import DashboardAPISprayJsonFormatter._
  import TopQueriesSprayJsonFormatter._
  import ValidQueriesSprayJsonFormatter._
  import spray.json._

  private lazy val s3Client = new S3Client
  private lazy val logger = LoggerFactory.getLogger("ignition.Uploader")

  implicit lazy val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  implicit lazy val actorSystem = ActorSystem("uploader")

  private case class UploaderConfig(eventType: String = "",
                                    path: String = "",
                                    server: String = "",
                                    port: Int = 9200,
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
      opt[Int]('p', "es-port") action { (x, c) => c.copy(port = x) }
      opt[Int]('t', "es-save-timeout-minutes-per-bulk") action { (x, c) => c.copy(bulkTimeoutInMinutes = x) }
      opt[Int]('b', "es-bulk-size") action { (x, c) => c.copy(bulkSize = x) }
    }

    try {
      parser.parse(args, UploaderConfig()).foreach {
        case UploaderConfig("top-queries", path, server, port, timeoutInMinutes, bulkSize, config) if path.nonEmpty && server.nonEmpty =>
          runTopQueries(path, server, port, timeoutInMinutes, bulkSize, config)

        case UploaderConfig("valid-queries", path, server, port, timeoutInMinutes, bulkSize, config) if path.nonEmpty && server.nonEmpty =>
          runValidQueries(path, server, port, timeoutInMinutes, bulkSize, config)

        case UploaderConfig("kpi", path, _, _, _, timeoutInMinutes, _) if path.nonEmpty =>
          runKpis(path, timeoutInMinutes)

        case _ => throw new IllegalArgumentException(s"Invalid parameters: $parser")
      }
    } catch {
      case ex: IllegalArgumentException =>
        logger.error(ex.getMessage)

      case NonFatal(ex) =>
        logger.error("Error", ex)
    }

    actorSystem.shutdown()
    ec.shutdown()
    logger.info("done!")
  }

  private def runKpis(path: String, timeoutInMinutes: Int): Unit = {
    val kpis = parseLines[KpiWithDashPoint](getLines(path))
    logger.info("Executing kpi uploads to Dashboard API")
    Await.result(saveKpisToDashBoard(kpis.toSeq), Duration(timeoutInMinutes, TimeUnit.MINUTES))
  }

  def runTopQueries(path: String, server: String, port: Int, timeoutInMinutes: Int, bulkSize: Int, jsonIndexConfig: Option[String]): Unit = {
    logger.info(s"Executing top-queries uploads to Elastic Search server: $server:$port")
    val timeout = Duration(timeoutInMinutes, TimeUnit.MINUTES)
    val topQueries = parseLines[RawTopQueries](getLines(path)).map(_.convert)
    val client = new ElasticSearchClient(server, port)
    val configContent = jsonIndexConfig.map(getClass.getResource).getOrElse(getClass.getResource("/etl-top-queries-template.json"))

    val indexOperation = client.saveTopQueries(topQueries, Source.fromURL(configContent).mkString, bulkSize)(timeout)
    indexOperation match {
      case Success(result) =>
        logger.info(s"Top-queries saved to elasticsearch!")
      case Failure(ex) =>
        logger.error("Fail to bulk index top queries", ex)
        throw ex
    }
  }

  def runValidQueries(path: String, server: String, port: Int, timeoutInMinutes: Int, bulkSize: Int, jsonIndexConfig: Option[String]): Unit = {
    logger.info(s"Executing valid-queries uploads to Elastic Search server: $server:$port")
    val timeout = Duration(timeoutInMinutes, TimeUnit.MINUTES)
    val validQueries = parseLines[RawValidQueries](getLines(path)).map(_.convert)
    val client = new ElasticSearchClient(server, port)
    val configContent = jsonIndexConfig.map(getClass.getResource).getOrElse(getClass.getResource("/valid_queries_index_configuration.json"))
    val indexOperation = client.saveValidQueries(validQueries, Source.fromURL(configContent).mkString, bulk = bulkSize)(timeout)
    indexOperation match {
      case Success(result) =>
        logger.info(s"Valid-queries saved to elasticsearch!")
      case Failure(ex) =>
        logger.error("Fail to bulk index valid queries", ex)
        throw ex
    }
  }

  private def parseLines[T](lines: Iterator[String])(implicit reader: JsonReader[T]): Iterator[T] = lines.map { line =>
    try { line.parseJson.convertTo[T] }
    catch {
      case ex: JsonParser.ParsingException =>
        logger.error("Parsing error: json = '', message: '{}'", line, ex.getMessage)
        throw ex
    }
  }

  private def getLines(path: String): Iterator[String] = {
    val codec = Codec.UTF8

    val isS3Path = path.startsWith("s3://")

    def getFiles(pathParam: String): Iterator[File] = {
      val path = Paths.get(pathParam)
      if (Files.isDirectory(path)) path.toFile.listFiles().toIterator
      else if (Files.isRegularFile(path)) Iterator(path.toFile)
      else throw new IllegalArgumentException(s"Invalid file path: '$path'")
    }

    def linesFromFiles(path: String): Iterator[String] = {
      logger.info("Loading data from local file system, path: {}", path)
      getFiles(path).map { file =>
        logger.info("File: {}", file.getAbsolutePath)
        val inputStream = if (file.getAbsolutePath.endsWith("gz"))
          new GZIPInputStream(new FileInputStream(file))
        else
          new FileInputStream(file)
        Source.fromInputStream(inputStream)(codec).getLines()
      }.filter(_.nonEmpty).flatten
    }

    val s3Pattern = "s3://([a-zA-Z0-9\\-]+)/(.*)".r

    def linesFromS3(path: String): Iterator[String] = path match {
      case s3Pattern(bucket, key) =>
        logger.info("Loading data from S3, path: {}", path)
        val validObjects = s3Client.list(bucket, key)
          .filter(_.getContentLength > 0)
          .filterNot(_.getKey contains "_temporary").toIterator

        validObjects.flatMap { unload =>
          logger.info("Loading S3 object: {}", unload.getKey)
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
