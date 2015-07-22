package ignition.jobs.pojo

import ignition.chaordic.utils.Json
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol

case class QueryCount(query: String, count: Long)

case class RawTopQueries(apiKey: String,
                         datetime: String,
                         queries_has_results: Boolean,
                         event: String,
                         top_queries: Seq[QueryCount]) {

  def convert = TopQueries(
    apiKey = apiKey,
    datetime = datetime,
    hasResult = queries_has_results,
    topQueries = top_queries.map(e => QueryCount(query = e.query, count = e.count)))

  def toJson: String = Json.toJsonString(this)
}

case class TopQueries(apiKey: String, datetime: String, hasResult: Boolean, topQueries: Seq[QueryCount]) {
  def toRaw: RawTopQueries =
    RawTopQueries(
      apiKey = apiKey,
      datetime = datetime,
      queries_has_results = hasResult,
      event = "top_queries",
      top_queries = topQueries)
}

case class SearchKey(apiKey: String, feature: String, searchId: String)

case class QueryKey(apiKey: String, datetime: String, query: String, hasResult: Boolean)


object TopQueriesSprayJsonFormatter extends DefaultJsonProtocol with SprayJsonSupport {

    implicit val rawQueryCountFormat = jsonFormat2(QueryCount)
    implicit val rawTopQueriesFormat = jsonFormat5(RawTopQueries)

}
