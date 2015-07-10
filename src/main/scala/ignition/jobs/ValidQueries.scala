package ignition.jobs

import ignition.chaordic.pojo.{SearchEvent, SearchClickLog, SearchLog}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import ignition.jobs.utils.text.{Tokenizer, ChaordicStemmer}
import ignition.jobs.utils.SearchEventValidations.SearchEventValidations

object ValidQueries {

  import ignition.core.utils.DateUtils._

  val invalidQueries = Set[String]().empty
  val invalidIPs = Set[String]().empty

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
      searchLog.filters.getOrElse(Map.empty()).nonEmpty
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

  def queryToKey(query: String) =
    ChaordicStemmer.asciiFold(query.split("\\s+")).mkString

  def extractTokens(query: String): Seq[String] =
    ChaordicStemmer.stem(ChaordicStemmer.asciiFold(Tokenizer.tokenize(query)).map(_.toLowerCase))

  def joinSearchLogsWithClickLogs(validSearchLogs: RDD[((String, String), SearchLog)],
                                  validClickLogs: RDD[((String, String), SearchClickLog)]) = {

    val filteredClickLogs: RDD[((String, String), SearchClickLog)] =
      validSearchLogs
        .map { case ((apiKey, searchId), log) => ((apiKey, searchId), log.query) }
        .distinct()
        .join(validClickLogs)
        .filter { case ((apiKey, searchId), (query, log)) => query.nonEmpty }
        .map { case ((apiKey, searchId), (query, clickLog)) => ((apiKey, query), clickLog) }


    val filteredSearchLogs: RDD[((String, String), SearchLog)] =
      validSearchLogs
        .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    val clicks: RDD[((String, String), Long)] = filteredClickLogs.aggregateByKey(0L)((_, _) => 1, _ + _)
    val searches: RDD[((String, String), Long)] = filteredSearchLogs.aggregateByKey(0L)((_, _) => 1, _ + _)

    val sumOfResults = filteredSearchLogs.mapValues(_.totalFound.toLong).reduceByKey(_ + _)

    val latestSearchLogs = filteredSearchLogs.reduceByKey((s1, s2) => if (s1.date.isAfter(s2.date)) s1 else s2)

    val validQueries = clicks.join(searches).join(latestSearchLogs).join(sumOfResults).map {
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

    val biggestValidQuery = validQueries
      .keyBy(validQuery => (validQuery.apiKey, queryToKey(validQuery.query)))
      .reduceByKey((v1, v2) => if (v1.searches > v2.searches) v1 else v2)
      .map {
      case ((apiKey, _), validQuery) =>
        val tokens = extractTokens(validQuery.query)
        ((apiKey, tokens), validQuery)
    }

    biggestValidQuery.groupByKey().map {
      case ((apiKey, tokens), allValidQueries) =>
        val queries = allValidQueries.toSeq
        val topValidQuery = queries.toSeq.sortBy(_.searches).reverse.head
        val latestValidQuery = queries.toSeq.sortBy(_.latestSearchLog).reverse.head
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

    joinSearchLogsWithClickLogs(validSearchLogs, validClickLogs)

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
