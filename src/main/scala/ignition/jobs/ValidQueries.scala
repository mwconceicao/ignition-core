package ignition.jobs

import ignition.chaordic.pojo.{SearchEvent, SearchClickLog, SearchLog}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

object ValidQueries {

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
  def normalize_query(query: String) =
    query.trim.toLowerCase.removeTyposAtQueryTail()

  def process(searchLogs: RDD[SearchLog], clickLogs: RDD[SearchClickLog]) = {

    val validSearchLogs = getValidSearchLogs(searchLogs)
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))

    val validClickLogs = getValidClickLogs(clickLogs)
      .keyBy(clickLog => (clickLog.apiKey, clickLog.searchId))



  }

  case class ValidQuery(apiKey: String,
                        query: String,
                        searches: Int,
                        clicks: Int,
                        raw_ctr: Double,
                        latestSearchLog: DateTime,
                        latestSearchLogResults: DateTime,
                        latestSearchLogFeature: DateTime,
                        sumResults: Int,
                        averageResults: Int)

}
