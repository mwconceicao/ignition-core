package ignition.jobs.utils

import akka.actor.ActorRefFactory
import ignition.chaordic.utils.Json
import ignition.jobs.Configuration
import ignition.jobs.TopQueriesJob.TopQueries
import ignition.jobs.ValidQueriesJob.ValidQueryFinal
import ignition.jobs.setups.{TopQueriesSetup, ValidQueriesSetup}
import org.apache.commons.codec.digest.DigestUtils
import org.joda.time.DateTime
import spray.client.pipelining.SendReceive
import spray.http.ContentTypes._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.{Source, Codec}

import spray.http._
import spray.client.pipelining._

import scala.language.implicitConversions

trait ElasticSearchApi {

  implicit def actorFactory: ActorRefFactory
  implicit def executionContext: ExecutionContext

  def elasticSearchHost: String
  def elasticSearchPort: Int

  private lazy val elasticSearchUrl = s"$elasticSearchHost:$elasticSearchPort"

  private lazy val pipeLine: SendReceive = sendReceive

  private def serialExecution[E, R](elements: Seq[E], f: Seq[E] => Future[R], bulkSize: Int)
                             (implicit bulkTimeout: Duration): Future[Iterator[R]] =
    Future.sequence(elements.grouped(bulkSize).map { bulk =>
      val result = f(bulk)
      Await.ready(result, bulkTimeout)
    })

  private def execute(request: HttpRequest): Future[String] =
    pipeLine(request).flatMap { response =>
      if (response.status.isFailure)
        Future.failed(new RuntimeException(response.entity.asString))
      else
        Future.successful(response.entity.asString)
    }

  private case class Document(id: String, json: String)

  private case class IndexInfo(name: String, aliases: Set[String])

  private type RawIndexesInfo = Map[String, Map[String, Map[String, Any]]]

  private def post(path: String, json: String): Future[String] =
    execute(Post(s"$elasticSearchUrl$path", HttpEntity(`application/json`, json)))

  private def get(path: String): Future[String] = execute(Get(s"$elasticSearchUrl$path"))

  private def delete(path: String): Future[String] = execute(Delete(s"$elasticSearchUrl$path"))

  private def toDocument(data: TopQueries): Document = {
    val id = DigestUtils.md5Hex(s"${data.apiKey}${data.datetime}${data.hasResult}".getBytes(Codec.UTF8.charSet))
    val json = TopQueriesSetup.transformToJsonString(data)
    Document(id, json)
  }

  private def toDocument(data: ValidQueryFinal): Document = {
    val id = DigestUtils.md5Hex(s"${data.apiKey},${data.topQuery}".getBytes(Codec.UTF8.charSet))
    val json = ValidQueriesSetup.transformToJsonString(data)
    Document(id, json)
  }

  def serialSaveTopQueries(topQueries: Seq[TopQueries], bulkSize: Int)
                          (implicit timeout: Duration): Future[Iterator[String]] = {
    Future.sequence(topQueries.groupBy(_.datetime).map {
      case (datetime, groupedTopQueries) =>
        val processBulk = (bulk: Seq[TopQueries]) => bulkIndex(s"etl-top_queries-$datetime", "top_queries", bulk.map(toDocument))
        serialExecution(groupedTopQueries, processBulk, bulkSize)
    }).map(_.flatten.toIterator)
  }

  def serialSaveValidQueries(indexName: String, validQueries: Seq[ValidQueryFinal], bulkSize: Int)
                            (implicit timeout: Duration): Future[Iterator[String]] =
    serialExecution(validQueries.map(toDocument), (bulk: Seq[Document]) => bulkIndex(indexName, "query", bulk), bulkSize)

  private def createNewIndex(indexName: String, jsonIndexConfig: String): Future[Unit] =
    post(s"/$indexName", jsonIndexConfig).map(_ => ())

  private def indexesInfo(): Future[Seq[IndexInfo]] = get("/_aliases").map { json =>
    Json.parseRaw[RawIndexesInfo](json).map {
      case (indexName, aliases) =>
        IndexInfo(indexName, aliases.get("aliases").map(_.keys.toSet).getOrElse(Set.empty))
    }.toSeq
  }

  def saveValidQueries(validQueries: Seq[ValidQueryFinal],
                       jsonIndexConfig: String,
                       now: DateTime = DateTime.now,
                       aliasName: String  = "valid_queries",
                       bulk: Int = 1000)
                      (implicit bulkTimeout: Duration) = {
    val indexName = s"${aliasName}_${now.toString("yyyyMMdd-HHmmss")}"
    createNewIndex(indexName, jsonIndexConfig)
      .flatMap(_ => serialSaveValidQueries(indexName, validQueries, bulk))
      .flatMap(_ => updateAliases(aliasName, indexName))
  }

  private def updateAliases(aliasName: String, newIndexName: String): Future[String] =
    indexesInfo().flatMap { indexes =>
      val oldIndexes = indexes.filter(index => index.aliases.contains(aliasName))
      val remove = oldIndexes.map(index => Map("remove" -> Map("index" -> index.name, "alias" -> aliasName)))
      val add = Map("add" -> Map("index" -> newIndexName, "alias" -> aliasName))
      val actions = Map("actions" -> List(remove ++ add))
      val json = Json.toJsonString(actions)
      post("/_aliases", json).flatMap { response =>
        Future.sequence(oldIndexes.map(index => delete(s"/${index.name}"))).map(_ => response)
      }
    }

  private def bulkIndex(indexName: String, documentType: String, documents: Seq[Document]): Future[String] = {
    val metaTemplate = s"""{ "index": { "_index": "$indexName", "_type": "$documentType", "_id": "__id__" } }"""
    val bulkJson = Stream.continually(metaTemplate).zip(documents).map {
      case (meta, document) => s"${meta.replaceAll("__id__", document.id)}\n${document.json}"
    }.mkString("\n")
    post("/_bulk", bulkJson)
  }

}
