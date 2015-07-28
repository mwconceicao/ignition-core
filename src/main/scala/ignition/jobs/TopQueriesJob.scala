package ignition.jobs

import ignition.chaordic.pojo._
import ignition.jobs.pojo.{TopQueries, QueryCount, SearchKey, QueryKey}
import org.apache.spark.rdd.RDD

object TopQueriesJob extends SearchETL {

  import ignition.jobs.utils.SearchEventValidations.SearchEventValidations

  private val invalidQueries = Set("pigdom")
  private val invalidIpAddresses = Set("107.170.51.250")

  implicit class SearchLogEnhancements(searchLog: SearchLog) {

    private val validFeatures = Set("autocomplete", "redirect")

    def queryKey = QueryKey(
      apiKey = searchLog.apiKey,
      datetime = searchLog.date.withTimeAtStartOfDay.toString,
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
      case (key, events) if key.feature == "autocomplete" =>
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
      case (QueryKey(apiKey, datetime, query, hasResult), events) =>
        ((apiKey, datetime, hasResult), QueryCount(query, events.size))
    }.groupByKey()

    rawTopQueries.map {
      case ((apiKey, datetime, hasResult), queries) =>
        val topQueries = queries.toSeq.sortBy(_.count).reverse.take(100)
        TopQueries(apiKey = apiKey, datetime = datetime, hasResult = hasResult, topQueries = topQueries)
    }
  }

}