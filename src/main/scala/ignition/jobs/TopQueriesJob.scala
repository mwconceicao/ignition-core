package ignition.jobs

import ignition.chaordic.pojo._
import org.apache.spark.rdd.RDD

object TopQueriesJob extends SearchETL {

  import ignition.jobs.utils.SearchEventValidations.SearchEventValidations

  case class QueryCount(query: String, count: Long)
  case class TopQueries(apiKey: String, day: String, hasResult: Boolean, topQueries: Seq[QueryCount])
  case class SearchKey(apiKey: String, feature: String, searchId: String)
  case class QueryKey(apiKey: String, day: String, query: String, hasResult: Boolean)

  private val invalidQueries = Set("pigdom")
  private val invalidIpAddresses = Set("107.170.51.250")

  implicit class SearchLogEnhancements(searchLog: SearchLog) {

    private val validFeatures = Set("autocomplete", "redirect")

    def queryKey = QueryKey(
      apiKey = searchLog.apiKey,
      day = searchLog.date.withTimeAtStartOfDay.toString,
      query = searchLog.normalizedQuery,
      hasResult = searchLog.hasResult)

    def searchKey = SearchKey(
      apiKey = searchLog.apiKey,
      feature = searchLog.realFeature,
      searchId = searchLog.searchId)

    def normalizedQuery: String = searchLog.query.toLowerCase.replaceAll("\\<.*?>", "").replaceAll("&lt;.*?&gt;", "")
    def realFeature: String = if (validFeatures.contains(searchLog.feature)) searchLog.feature else "search"
    def hasResult: Boolean = searchLog.totalFound > 0
    def isValidFeature: Boolean = if (searchLog.realFeature == "autocomplete" && !searchLog.hasResult) false else true
  }

  def execute(searchLogs: RDD[SearchLog]): RDD[TopQueries] = {
    val filtered = filterValidEvents(searchLogs).keyBy(_.searchKey).groupByKey()
    val uniqueSearchesByAutocomplete = fixAutocompleteDuplication(filtered)
    val queries = filterAndGroupQuery(uniqueSearchesByAutocomplete)
    calculateTopQueries(queries)
  }

  def filterValidEvents(searchLogs: RDD[SearchLog]): RDD[SearchLog] =
    searchLogs
      .filter(_.valid(invalidQueries, invalidIpAddresses))
      .filter(_.isValidFeature)

  def fixAutocompleteDuplication(searchLogByKey: RDD[(SearchKey, Iterable[SearchLog])]): RDD[(SearchKey, Iterable[SearchLog])] =
    searchLogByKey.map {
      case (key @ SearchKey(_, "autocomplete", _), events) =>
        // should materialize?
        (key, events.toSeq.sortBy(_.query.length).reverse.lastOption.toSeq)
      case other => other
    }

  def filterAndGroupQuery(searchLogByKey: RDD[(SearchKey, Iterable[SearchLog])]): RDD[(QueryKey, Iterable[SearchLog])] =
    searchLogByKey.flatMap {
      case (searchKey, events) =>
        events
          .filter(event => event.normalizedQuery.nonEmpty && event.page == 1)
          .map(event => (event.queryKey, event))
    }.groupByKey()

  def calculateTopQueries(logsByQuery: RDD[(QueryKey, Iterable[SearchLog])]): RDD[TopQueries] = {
    val rawTopQueries = logsByQuery.map {
      case (QueryKey(apiKey, day, query, hasResult), events) =>
        ((apiKey, day, hasResult), QueryCount(query, events.size))
    }.groupByKey()

    rawTopQueries.map {
      case ((apiKey, day, hasResult), queries) =>
        val topQueries = queries.toSeq.sortBy(_.count).reverse.take(100)
        TopQueries(apiKey = apiKey, day = day, hasResult = hasResult, topQueries = topQueries)
    }
  }

}
