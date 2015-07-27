package ignition.jobs.pojo

import ignition.chaordic.Chaordic
import org.joda.time.DateTime
import spray.httpx.SprayJsonSupport
import spray.json._
import ignition.chaordic.utils.Json

case class RawValidQuery(raw_ctr: Double,
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

case class RawValidQueries(top_query: String,
                           apiKey: String,
                           raw_ctr: Double,
                           tokens: Seq[String],
                           latest_search_log_results: Int,
                           searchs: Long,
                           active: Boolean,
                           average_results: Long,
                           latest_search_log: String,
                           queries: Seq[RawValidQuery],
                           clicks: Long) {
  def convert: ValidQueries = ValidQueries(
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

  def toJson: String = Json.toJsonString(this)

}

case class ValidQuery(apiKey: String,
                      query: String,
                      searches: Long,
                      clicks: Long,
                      rawCtr: Double,
                      latestSearchLog: DateTime,
                      latestSearchLogResults: Int,
                      latestSearchLogFeature: String,
                      sumResults: Long,
                      averageResults: Long) {

  def toRaw: RawValidQuery = {
    RawValidQuery(
      raw_ctr = rawCtr,
      apiKey = apiKey,
      average_results = averageResults,
      latest_search_log_results = latestSearchLogResults,
      latest_search_log = latestSearchLog.toString("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
      latest_search_log_feature = latestSearchLogFeature,
      searchs = searches,
      query = query,
      clicks = clicks)
  }

  def toJson: String = Json.toJsonString(this)
}

case class ValidQueries(apiKey: String,
                           tokens: Seq[String],
                           topQuery: String,
                           searches: Long,
                           clicks: Long,
                           rawCtr: Double,
                           latestSearchLog: DateTime,
                           latestSearchLogResults: Int,
                           averageResults: Long,
                           queries: Seq[ValidQuery],
                           active: Boolean = true) {
  def toRaw: RawValidQueries = {
    RawValidQueries(
      top_query = topQuery,
      apiKey = apiKey,
      raw_ctr = rawCtr,
      tokens = tokens,
      latest_search_log_results = latestSearchLogResults,
      searchs = searches,
      active = true,
      average_results = averageResults,
      latest_search_log = latestSearchLog.toString("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
      queries = queries.map(_.toRaw),
      clicks = clicks
    )
  }
}


object ValidQueriesSprayJsonFormatter extends DefaultJsonProtocol with SprayJsonSupport {

  implicit val rawValidQueryFormat = jsonFormat9(RawValidQuery)
  implicit val rawValidQueriesFormat = jsonFormat11(RawValidQueries)

}
