package ignition.jobs.utils

import akka.actor.ActorRefFactory
import ignition.chaordic.Chaordic
import ignition.chaordic.utils.Json
import ignition.jobs.pojo.{TopQueries, ValidQueries}
import org.apache.commons.codec.digest.DigestUtils
import org.joda.time.DateTime
import org.slf4j.{LoggerFactory, Logger}
import spray.client.pipelining.{SendReceive, _}
import spray.http.ContentTypes._
import spray.http._

import scala.collection.generic.CanBuildFrom
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Codec
import scala.language.{higherKinds, implicitConversions, postfixOps}
import scala.util.{Success, Try}
import scala.util.control.NonFatal

trait ElasticSearchApi {

  implicit def actorFactory: ActorRefFactory
  implicit def executionContext: ExecutionContext

  def elasticSearchHost: String
  def elasticSearchPort: Int

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val elasticSearchUrl = s"$elasticSearchHost:$elasticSearchPort"

  private lazy val pipeLine: SendReceive = sendReceive

  private def syncExecute(request: HttpRequest)(implicit timeout: Duration): Try[String] = {
    val fResponse = pipeLine(request).flatMap { response =>
      if (response.status.isFailure)
        Future.failed(new RuntimeException(response.entity.asString))
      else
        Future.successful(response.entity.asString)
    }
    logger.debug("Executing http operation: {}", request)
    Try(Await.result(fResponse, timeout))
  }

  private case class Document(id: String, documentType: String, json: String) {
    def indexAction(indexName: String): String =
      s"""{ "index": { "_index": "$indexName", "_type": "$documentType", "_id": "$id" } }\n$json\n"""
  }

  private case class IndexInfo(name: String, aliases: Set[String])

  private type RawIndexesInfo = Map[String, Map[String, Map[String, Any]]]

  private def post(path: String, json: String)(implicit timeout: Duration): Try[String] =
    syncExecute(Post(s"$elasticSearchUrl$path", HttpEntity(`application/json`, json)))

  private def get(path: String)(implicit timeout: Duration): Try[String] =
    syncExecute(Get(s"$elasticSearchUrl$path"))

  private def head(path: String)(implicit timeout: Duration): Try[String] =
    syncExecute(Head(s"$elasticSearchUrl$path"))

  private def delete(path: String)(implicit timeout: Duration): Try[String] =
    syncExecute(Delete(s"$elasticSearchUrl$path"))

  private def toDocument(data: TopQueries): Document = {
    val id = DigestUtils.md5Hex(s"${data.apiKey}${data.datetime}${data.hasResult}".getBytes(Codec.UTF8.charSet))
    val json = data.toRaw.toJson
    Document(id, data.apiKey, json)
  }

  private def toDocument(data: ValidQueries): Document = {
    val id = DigestUtils.md5Hex(s"${data.apiKey},${data.topQuery}".getBytes(Codec.UTF8.charSet))
    val json = data.toRaw.toJson
    Document(id, "query", json)
  }

  // copy from Future.sequence
  object TryExtension {

    def sequence[A, M[_] <: TraversableOnce[_]](in: M[Try[A]])(implicit cbf: CanBuildFrom[M[Try[A]], A, M[A]], executor: ExecutionContext): Try[M[A]] = {
      in.foldLeft(Try(cbf(in))) {
        (fr, fa) => for (r <- fr; a <- fa.asInstanceOf[Try[A]]) yield r += a
      } map (_.result)
    }

  }

  def indexExists(indexName: String)(implicit timeout: Duration): Boolean = head(s"/$indexName").isSuccess

  def saveBulkTopQueries(jsonIndexConfig: String,  bulkSize: Int, datetime: String, bulk: Seq[TopQueries])(implicit timeout: Duration): Try[Unit] = {
    val indexName = s"etl-top_queries-$datetime"
    ensureIndex(indexName, jsonIndexConfig).flatMap { _ =>
      val bulks = bulk.grouped(bulkSize)
      val indexOperations = bulks.map(bulk => bulkIndex(indexName, bulk.map(toDocument)))
      TryExtension.sequence(indexOperations).map(_ => ())
    }
  }

  def saveTopQueries(topQueries: Iterator[TopQueries], jsonIndexConfig: String, bulkSize: Int)
                    (implicit timeout: Duration): Try[Unit] = {
    logger.info("Uploading top queries, bulk size {}", bulkSize)
    val topQueriesByDatetime = topQueries.toSeq
      .groupBy(topQuery => Chaordic.parseDate(topQuery.datetime).toString("yyyy-MM"))
      .filter { case (_, grouped) => grouped.nonEmpty }

    val bulks = topQueriesByDatetime.map {
      case (datetime, groupedTopQueries) => saveBulkTopQueries(jsonIndexConfig, bulkSize, datetime, groupedTopQueries)
    }

    TryExtension.sequence(bulks).map(_ => ())
  }

  def serialSaveValidQueries(indexName: String, validQueries: Iterator[ValidQueries], bulkSize: Int)
                            (implicit timeout: Duration): Try[Unit] = {
    TryExtension.sequence(validQueries.map(toDocument).grouped(bulkSize).map { bulk =>
      bulkIndex(indexName, bulk)
    }).map(_ => ())
  }

  private def setTemplate(templateConfig: String)(implicit timeout:Duration): Try[String] = {
    logger.info("Setting template: {}", templateConfig)
    val templateName = "top_query_template"

    if (!templateExists(templateName))
      post(s"/_template/$templateName", templateConfig)
    else
      Success("yey")
  }

  private def createNewIndex(indexName: String, jsonIndexConfig: String)(implicit timeout: Duration): Try[String] = {
    logger.info("Creating new index {} with config '{}'", indexName, jsonIndexConfig)
    post(s"/$indexName", jsonIndexConfig)
  }

  private def templateExists(templateName: String)(implicit timeout: Duration): Boolean = {
    head(s"/_template/$templateName").isSuccess
  }

  private def ensureIndex(indexName: String, jsonIndexConfig: String)(implicit timeout: Duration): Try[String] = {
    logger.info("Ensuring index {}", indexName)
    if (indexExists(indexName))
      Success(indexName)
    else
      setTemplate(jsonIndexConfig)
  }

  private def indexesInfo()(implicit timeout: Duration): Try[Seq[IndexInfo]] = get("/_aliases").map { response =>
    Json.parseRaw[RawIndexesInfo](response).map {
      case (indexName, aliases) =>
        IndexInfo(indexName, aliases.get("aliases").map(_.keys.toSet).getOrElse(Set.empty))
    }.toSeq
  }

  def saveValidQueries(validQueries: Iterator[ValidQueries],
                       jsonIndexConfig: String,
                       now: DateTime = DateTime.now,
                       aliasName: String  = "valid_queries",
                       bulk: Int = 1000)
                      (implicit bulkTimeout: Duration): Try[Unit] = {
    val indexName = s"${aliasName}_${now.toString("yyyyMMdd-HHmmss")}"
    logger.info("Uploading valid queries to index {}, reference date {}, alias name {}, bulk size {}", indexName, now.toString, aliasName, bulk.toString)
    createNewIndex(indexName, jsonIndexConfig)
      .flatMap { _ => serialSaveValidQueries(indexName, validQueries, bulk) }
      .flatMap { _ => updateAliases(aliasName, indexName) }
  }

  private def updateAliases(aliasName: String, newIndexName: String)(implicit timeout: Duration): Try[Unit] = {
    indexesInfo().flatMap { indexes =>
      logger.info("Updating aliases to alias name '{}', new index: {}")
      val oldIndexes = indexes.filter(index => index.aliases.contains(aliasName))
      val remove = oldIndexes.map(index => Map("remove" -> Map("index" -> index.name, "alias" -> aliasName)))
      val add = Map("add" -> Map("index" -> newIndexName, "alias" -> aliasName))
      val actions = Map("actions" -> (remove :+ add))
      val json = Json.toJsonString(actions)
      logger.debug("Updating alias json request: {}", json)
      post("/_aliases", json).flatMap { aliasResponse =>
        logger.debug("Creating alias response: {}", aliasResponse)
        TryExtension.sequence(oldIndexes.map { index =>
          delete(s"/${index.name}").map(deleteResponse => logger.debug("Delete index {} response: {}", index, deleteResponse))
        }).map(_ => ())
      }
    }
  }

  private def bulkIndex(indexName: String, documents: Seq[Document])(implicit timeout: Duration): Try[String] = {
    val bulkJson = documents.map(_.indexAction(indexName)).mkString("\n")
    logger.info("Execution bulk for index {}", indexName)
    post("/_bulk", bulkJson)
  }

}
