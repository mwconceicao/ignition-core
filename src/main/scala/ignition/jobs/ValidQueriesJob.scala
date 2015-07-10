package ignition.jobs

import ignition.chaordic.pojo.{SearchClickLog, SearchEvent, SearchLog}
import ignition.jobs.utils.text.{ChaordicStemmer, Tokenizer}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * This Job is of utmost importance. This generate values for the index `valid_queries` that is maintained on our
 * Elasticsearch. This index is used to generate query recommendations in auto complete and also query suggestions on
 * the search page.
 *
 * It essentially aggregate all queries and clicks of a given apikey/query. This query is normalized so we can aggregate
 * them. We them filter the query that was most searched and use that as a valid suggestion. Valid Suggestions are
 * sorted by importance (Clicks/Search/CTR) [TODO: Review logic on simpleui or chaordic_search/__init__.py].
 *
 * ValidQueries on the job is always considered valid, and they are set to invalid by search front-backend, i.e., the
 * valid query becomes invalid when the search given by this suggestion yield no results.
 *
 */

object ValidQueriesJob {

  import ignition.core.utils.DateUtils._
  import ignition.jobs.utils.SearchEventValidations.SearchEventValidations

  val invalidQueries = Set("pingdom")
  val invalidIPs = Set("107.170.51.250")

  val lastDaysToConsider = DateTime.now.minusDays(365)

  implicit class SearchEventImprovements(searchEvent: SearchEvent) {
    def validDate =
      searchEvent.date.isAfter(lastDaysToConsider)
  }

  implicit class TypoRemover(query: String) {

    val defaultTypos = Set("~", "'", """\[""").mkString("|")

    val substitutionRegex = s"($defaultTypos)*$$"

    /**
     * Remove typos from the end of the string.
     *
     * Current Regex is (~|'|\[)*$
     *
     * @return
     */
    def removeTyposAtQueryTail(): String = {
      query.replaceAll(substitutionRegex, "")
    }
  }

  implicit class SearchLogsImprovements(searchLog: SearchLog) {
    def hasFilters =
      searchLog.filters.getOrElse(Map.empty).nonEmpty
  }

  /**
   * Filter SearchLogs.
   *
   * A search log is considered valid if:
   *  - it is not an autocomplete
   *  - it does not have filters
   *  - it does not contain invalidQueries and don't come from invalidIPs
   *  - The date is after our threshold
   *
   * @param searchLogs SearchLogs to be filtered.
   * @return
   */
  def getValidSearchLogs(searchLogs: RDD[SearchLog]) =
    searchLogs
      .filter(_.feature != "autocomplete")
      .filter(!_.hasFilters)
      .filter(_.valid(invalidQueries, invalidIPs))
      .filter(_.validDate)

  /**
   * Filter ClickLogs
   *
   * A click log is considered valid if:
   *  - it does not have invalidQueries or invalidIPs
   *  - The date is after our threshold
   * @param clickLogs ClickLogs to be filtered.
   * @return
   */
  def getValidClickLogs(clickLogs: RDD[SearchClickLog]) =
    clickLogs
      .filter(_.valid(invalidQueries, invalidIPs))
      .filter(_.validDate)

  /**
   * Normalize query. Trim, lowercase and remove typos from the end of the string.
   * @param query Query to be normalized.
   * @return
   */
  def normalizeQuery(query: String) =
    query.trim.toLowerCase.removeTyposAtQueryTail()

  /**
   * Used to reduce valid queries. The key consist of the normalized strings removing all the spaces.
   * @param query
   * @return
   */
  def queryToKey(query: String) =
    ChaordicStemmer.asciiFold(query.split("\\s+")).mkString

  /**
   * Extract tokens. This function tokenize the words, asciifold, convert to lower and stem words.
   * @param query
   * @return
   */
  def extractTokens(query: String): Seq[String] =
    ChaordicStemmer.stem(ChaordicStemmer.asciiFold(Tokenizer.tokenize(query)).map(_.toLowerCase))

  /**
   * This method removes clicks that does not have an associated SearchLog. This is needed because we are strict with
   * the type of searchlog, i.e., only standard searches are considered valid. The output is a keyed RDD, with apiKey
   * and query as keys and the clicklogs as values. The query come from the matched SearchLogs.
   * @param validSearchLogs
   * @param validClickLogs
   */
  def filterClicksWithoutSearch(validSearchLogs:  RDD[((String, String), SearchLog)],
                                validClickLogs: RDD[((String, String), SearchClickLog)]): RDD[((String, String), SearchClickLog)] = {
    validSearchLogs
      .map { case ((apiKey, searchId), log) => ((apiKey, searchId), log.query) }
      .distinct()
      .join(validClickLogs)
      .filter { case ((apiKey, searchId), (query, log)) => query.nonEmpty }
      .map { case ((apiKey, searchId), (query, clickLog)) => ((apiKey, query), clickLog) }
  }

  /**
    * This function join clicks, searches, latest search log and the sum of all results given by all the searches inside
    * it. It generate a RDD of ValidQuery.
    * @param clicks
    * @param searches
    * @param latestSearchLogs
    * @param sumOfResults
    * @return
    */
  def joinEventsAndGetValidQueries(clicks: RDD[((String, String), Long)],
                                   searches: RDD[((String, String), Long)],
                                   latestSearchLogs: RDD[((String, String), SearchLog)],
                                   sumOfResults: RDD[((String, String), Long)]): RDD[ValidQuery] = {
    clicks.join(searches).join(latestSearchLogs).join(sumOfResults).map {
      case ((apiKey, query), (((clickCount, searchCount), latestSearchLog), sumResult)) =>
        ValidQuery(apiKey = apiKey,
          query = query,
          searches = searchCount,
          clicks = clickCount,
          rawCtr = clickCount.toDouble / searchCount,
          latestSearchLog = latestSearchLog.date,
          latestSearchLogResults = latestSearchLog.totalFound,
          latestSearchLogFeature = latestSearchLog.feature,
          sumResults = sumResult,
          averageResults = sumResult / searchCount)
    }
  }

  /**
   * Get the Bigger ValidQuery of a given key (apikey, queryToKey(validquery.query)).
   * @param validQueries
   * @return A keyed RDD: Key = (Apikey, Tokens of query), Value = Bigger ValidQuery
   */
  def getValidQueriesWithMostSearches(validQueries: RDD[ValidQuery]): RDD[((String, Seq[String]), ValidQuery)] =
    validQueries
      .keyBy(validQuery => (validQuery.apiKey, queryToKey(validQuery.query)))
      .reduceByKey((v1, v2) => if (v1.searches > v2.searches) v1 else v2)
      .map {
        case ((apiKey, _), validQuery) =>
          val tokens = extractTokens(validQuery.query)
          ((apiKey, tokens), validQuery)
      }

  /**
   * Given the bigger valid query for a given apikey and tokens output and final output.
   * @param biggestValidQueries
   * @return
   */
  def generateFinalValidQueries(biggestValidQueries: RDD[((String, Seq[String]), ValidQuery)]) = {
    biggestValidQueries.groupByKey().map {
      case ((apiKey, tokens), allValidQueries) =>
        val queries = allValidQueries.toSeq
        val topValidQuery = queries.sortBy(_.searches).reverse.head
        val latestValidQuery = queries.sortBy(_.latestSearchLog).reverse.head
        val totalSearches = queries.map(_.searches).sum
        val totalClicks = queries.map(_.clicks).sum
        val totalResult = queries.map(_.sumResults).sum
        ValidQueryFinal(
          apiKey = apiKey,
          tokens = tokens,
          topQuery = topValidQuery.query,
          searches = totalSearches,
          clicks = totalClicks,
          rawCtr = totalClicks / totalSearches,
          latestSearchLog = latestValidQuery.latestSearchLog,
          latestSearchLogResults = latestValidQuery.latestSearchLogResults,
          averageResults = totalResult / totalSearches,
          queries = queries
        )
    }
  }

  def process(searchLogs: RDD[SearchLog], clickLogs: RDD[SearchClickLog]): RDD[ValidQueryFinal] = {

    val validSearchLogs = getValidSearchLogs(searchLogs)
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))

    val validClickLogs = getValidClickLogs(clickLogs)
      .keyBy(clickLog => (clickLog.apiKey, clickLog.searchId))

    // Filter and remap key
    val filteredClickLogs = filterClicksWithoutSearch(validSearchLogs, validClickLogs)

    // remap key
    val filteredSearchLogs = validSearchLogs
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    // Count Events
    val clicks: RDD[((String, String), Long)] = filteredClickLogs.aggregateByKey(0L)((_, _) => 1, _ + _)
    val searches: RDD[((String, String), Long)] = filteredSearchLogs.aggregateByKey(0L)((_, _) => 1, _ + _)

    // Sum the number of products returned by the all the searches by a given apikey and query.
    val sumOfResults: RDD[((String, String), Long)] = filteredSearchLogs
      .mapValues(_.totalFound.toLong).reduceByKey(_ + _)

    // Get the latest by date.
    val latestSearchLogs: RDD[((String, String), SearchLog)] = filteredSearchLogs
      .reduceByKey((s1, s2) => if (s1.date.isAfter(s2.date)) s1 else s2)

    // Get Valid Queries
    val validQueries = joinEventsAndGetValidQueries(clicks, searches, latestSearchLogs, sumOfResults)

    val biggestValidQueries: RDD[((String, Seq[String]), ValidQuery)] = getValidQueriesWithMostSearches(validQueries)

    generateFinalValidQueries(biggestValidQueries)

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
                        averageResults: Long)

  case class ValidQueryFinal(apiKey: String,
                             tokens: Seq[String],
                             topQuery: String,
                             searches: Long,
                             clicks: Long,
                             rawCtr: Double,
                             latestSearchLog: DateTime,
                             latestSearchLogResults: Int,
                             averageResults: Long,
                             queries: Seq[ValidQuery],
                             active: Boolean = true)

}
