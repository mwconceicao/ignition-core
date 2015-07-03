package ignition.jobs

import java.security.MessageDigest

import ignition.chaordic.pojo._
import ignition.chaordic.utils.{HtmlUtils, Json}
import org.apache.commons.codec.binary.Hex
import org.apache.spark.rdd.RDD

object TopQueriesJob extends SearchETL {

  private val invalidQueries = Set("pigdom")
  private val invalidIpAddresses = Set("107.170.51.250")

  implicit class SearchLogEnhancements(searchLog: SearchLog) {

    private val validFeatures = Set("autocomplete", "redirect")

    def searchKey = SearchKey(apiKey = searchLog.apiKey, feature = searchLog.realFeature, searchId = searchLog.searchId)
    def queryKey = QueryKey(apiKey = searchLog.apiKey, day = searchLog.date.toString(aggregationLevel), query = searchLog.normalizedQuery, hasResult = searchLog.hasResult)
    def normalizedQuery: String = HtmlUtils.stripHtml(searchLog.query)
    def searchId: String = searchLog.info.getOrElse("searchId", "")
    def realFeature: String = if (validFeatures.contains(searchLog.feature)) searchLog.feature else "search"
    def hasResult: Boolean = searchLog.totalFound > 0
    def isValidFeature: Boolean = if (searchLog.realFeature == "autocomplete" && !searchLog.hasResult) false else true
  }

  def buildHash(searchLog: SearchLog): String = {
    val json = Json.toJson4sString(searchLog)
    val digest = MessageDigest.getInstance("SHA1").digest(json.getBytes("UTF-8"))
    Hex.encodeHexString(digest)
  }

  def execute(parsedSearchLogs: RDD[SearchLog]): RDD[TopQueries] = {
    val filtered = filterValidEvents(parsedSearchLogs).keyBy(_.searchKey).groupByKey()
    val uniqueSearchesByAutocomplete = fixAutocompleteDuplication(filtered)
    val queryIds = transformSearchLogsToHashesByQueryId(uniqueSearchesByAutocomplete)
    calculateTopQueries(queryIds)
  }

  def filterValidEvents(searchLogs: RDD[SearchLog]): RDD[SearchLog] =
    searchLogs
      .filter(_.valid(invalidQueries, invalidIpAddresses))
      .filter(_.isValidFeature)

  def fixAutocompleteDuplication(searchLogByKey: RDD[(SearchKey, Iterable[SearchLog])]): RDD[(SearchKey, Iterable[SearchLog])] =
    searchLogByKey.map {
      case (key @ SearchKey(_, "autocomplete", _), events) =>
        (key, events.toSeq.sortBy(_.query.length).reverse.lastOption.toSeq)
      case other => other
    }

  def transformSearchLogsToHashesByQueryId(searchLogByKey: RDD[(SearchKey, Iterable[SearchLog])]): RDD[(QueryKey, Iterable[String])] =
    searchLogByKey.flatMap {
      case (searchKey, events) =>
        events
          .filter(_.isValidFeature)
          .filter(event => event.normalizedQuery.nonEmpty && event.page == 1)
          .map(event => (event.queryKey, buildHash(event)))
    }.groupByKey()

  def calculateTopQueries(hashesByQueryKey: RDD[(QueryKey, Iterable[String])]): RDD[TopQueries] = {
    val rawTopQueries = hashesByQueryKey.map {
      case (QueryKey(apiKey, day, query, hasResult), hashes) =>
        ((apiKey, day, hasResult), QueryCount(query, hashes.size))
    }.groupByKey()

    rawTopQueries.map {
      case ((apiKey, day, hasResult), queries) =>
        val topQueries = queries.toSeq.sortBy(_.count).reverse.take(100)
        TopQueries(apiKey = apiKey, day = day, hasResult = hasResult, topQueries = topQueries)
    }
  }

}
