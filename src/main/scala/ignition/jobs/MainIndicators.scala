package ignition.jobs

import ignition.chaordic.pojo.{SearchLog, SearchClickLog, SearchEvent}
import ignition.jobs.utils.DashboardAPI.{FeaturedResultPoint, ResultPoint}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * This Job Calculate our main metrics for volume and CTR calculation.
 *
 * Some peculiarities:
 *  - bots are filtered out using event.info.browser_family
 *    Now only these bots are filtered out: "pingdombot", "googlebot", "bingbot", "facebookbot".
 *
 *  - IPs are also filtered out.
 *    Now only this IP is filtered out: 107.170.51.250 (I think we identified a large volume of searches
 *    coming from this IP when the original job was done.
 *
 *  Its original aggregation level was hourly, however, we were informed that DashboardAPI does not have a nice support
 *  for that, so we are using a daily aggregation.
 *
 *  Features:
 *    - search: is composed by all default searches:
 *      1. Standard
 *      2. or-fallback
 *      3. dym
 *      4. spelling-fallback
 *      5. rank-fallback
 *      6. isbn_search
 *      7. redirect (Yeah...)
 *
 *    - autocomplete
 *
 *    Interesting facts:
 *      - redirects are counted for the metric called "total" but not on unique.
 *      Example of format from the original job:
 *        {
 *          "apiKey":"someAPI",
 *          "sum":1,
 *          "feature":"search",
 *          "datetime":"2015-07-01T01:00:00",
 *          "type":"unique",      <<---- don't count redirects
 *          "event":"searchlog"
 *        }
 *
 *        {
 *          "apiKey": "camisariacolombo",
 *          "sum": 43,
 *          "feature": "search",
 *          "datetime": "2015-07-01T07:00:00",
 *          "type": "total",      <<----- Count redirects
 *          "event": "searchlog"
 *        }
 *
 *  Searches are only counted if they are in the first page. Pagination is not considered another search.
 *
 * NOTE: Considering that we are now aggregating by **DAY* and **NOT** by HOUR we can have less events
 *       (shouldn't be that much), however consider that if a person is typing on the widget around
 *       00:00:59 and 00:01:00 they would be split in two valid searches for the job and Aggregating by day that
 *       does **NOT** occur.
 *
 */

object MainIndicators extends SearchETL {


  import ignition.jobs.utils.SearchEventValidations.SearchEventValidations

  /** Utilities for Validation **/
  private val invalidQueries = Set("pingdom")
  private val invalidIps = Set("107.170.51.250")

  private val features = Set("autocomplete", "redirect")
  private val nonUniqueMetrics = Set("redirect")

  /**
   * Case class for holding our keys. It is used all around this job as key for sorting and getting unique events.
   *
   * @param client Client ApiKey
   * @param feature Event Feature
   * @param day Date Aggregation
   * @param searchId SearchId that was generated for that event.
   */
  case class MainIndicatorKey(client: String, feature: String, day: String, searchId: String) {
    def toResultPoint(value: Int): ResultPoint = {
      ResultPoint(day, client, value)
    }

    def toFeaturedResultPoint(value: Int): FeaturedResultPoint = {
      FeaturedResultPoint(client, day, value, feature)
    }
  }
  object MainIndicatorKey {
    def apply(event: SearchEvent): MainIndicatorKey = {
      MainIndicatorKey(
        event.apiKey,
        if (features.contains(event.feature)) event.feature else "search",
        event.date.toString(aggregationLevel),
        event.searchId)
    }

  }

  /**
   * Calculate Unique Events for AutoComplete (Search/Click)
   *
   * The logic here is that given a searchId, we only count the latest one.
   *
   * Example:
   *  00:00:00.0 -> a
   *  00:00:00.8 -> ab
   *  00:00:01.4 -> abb
   *  00:00:02.0 -> abba <- only this one is considered.
   *
   * @param events List of events.
   * @tparam T Class that extends SearchEvent
   * @return Valid events using the logic described above.
   */
  def getUniqueEventFromAutoComplete[T <: SearchEvent:ClassTag](events: RDD[T]): RDD[T] = {
    events
      .filter(_.feature == "autocomplete")
      .keyBy(_.searchId)
      .reduceByKey((s1, s2) => if (s1.date.isAfter(s2.date)) s1 else s2)
      .map(_._2)
  }

  /**
   * Count events sorted by its MainIndicatorKey
   * @param events List of events.
   * @tparam T Class that extends SearchEvent
   * @return Count of events that share the same MainIndicatorKey.
   */
  def getMetrics[T <: SearchEvent:ClassTag](events: RDD[T]): RDD[(MainIndicatorKey, Int)] = {
    events
      .map(event => {
        if (event.feature != "autocomplete")
          (MainIndicatorKey(event).copy(searchId = "daily", feature = "search"), 1)
        else
          (MainIndicatorKey(event).copy(searchId = "daily"), 1)
      })
      .reduceByKey(_ + _)
  }

  /**
   * Calculate Unique metrics this should be used for events that are not autocomplete.
   * This aggregate metrics for search (without redirects) and aggregate them yielding 1
   * for each distinct MainIndicator.
   *
   * FIXME: Note: We have issues in the code that send metrics. Sometimes there are no searchId, and they are set
   * to "null", the string with null inside (WTF). Because of that we need to create a hashcode for those events with
   * null, so we don't
   *
   * @param events List of events.
   * @tparam T Class that extends SearchEvent
   * @return
   */
  def getUniqueMetrics[T <: SearchEvent:ClassTag](events: RDD[T]): RDD[(MainIndicatorKey, Int)] = {
    events
      .filter(event => event.feature != "redirect")
      .map(MainIndicatorKey(_))
      .distinct()
      .map(_.copy(searchId = "daily"))
      .map(event => (event, 1))
      .reduceByKey(_ + _)
  }

  /**
   * Filter invalid searchlogs.
   * @param searchLogs all searchlogs
   * @return Only Valid Searchlogs.
   */
  def getValidSearchLogs(searchLogs: RDD[SearchLog]) =
    searchLogs
      .filter(_.valid(invalidQueries, invalidIps))
      .filter(_.page == 1)

  def getValidClickLogs(clickLogs: RDD[SearchClickLog]) =
    clickLogs
      .filter(_.valid(invalidQueries, invalidIps))

  def process(searchLogs: RDD[SearchLog], autoCompleteLogs: RDD[SearchLog], clickLogs: RDD[SearchClickLog]): MainIndicatorsResults = {

    val validSearchLogs = getValidSearchLogs(searchLogs)

    val validAutoCompleteLogs = getValidSearchLogs(autoCompleteLogs)

    val validClickLogs: RDD[SearchClickLog] = getValidClickLogs(clickLogs)


    // AutoComplete
    val autoCompleteEvents: RDD[SearchLog] = getUniqueEventFromAutoComplete(validAutoCompleteLogs)
    val autoCompleteClicks: RDD[SearchClickLog] =  getUniqueEventFromAutoComplete(validClickLogs)
    // AUTOCOMPLETE EVENTS
    val autoCompleteEventMetrics: RDD[(MainIndicatorKey, Int)] = getMetrics(autoCompleteEvents)
    // Don't contain redirects
    val autoCompleteUniqueEventMetrics: RDD[(MainIndicatorKey, Int)] = getUniqueMetrics(autoCompleteEvents)

    // AUTOCOMPLETE CLICKS
    val autoCompleteClickEventMetrics: RDD[(MainIndicatorKey, Int)] = getMetrics(autoCompleteClicks)
    // Don't contain redirects
    val autoCompleteClickUniqueEventMetrics: RDD[(MainIndicatorKey, Int)] = getUniqueMetrics(autoCompleteClicks)


    // Search
    // Counts All Search + Redirects
    val searchEvents: RDD[SearchLog] = validSearchLogs.filter(searchLog => searchLog.feature != "autocomplete")
    val searchClicks: RDD[SearchClickLog] = validClickLogs.filter(clickLog => clickLog.feature != "autocomplete")

    // SEARCH EVENTS
    // Contains redirects
    val searchEventMetrics: RDD[(MainIndicatorKey, Int)] = getMetrics(searchEvents)
    // Don't contain redirects
    val searchUniqueMetrics: RDD[(MainIndicatorKey, Int)] = getUniqueMetrics(searchEvents)

    // SEARCH CLICKS
    val searchClickEventMetrics: RDD[(MainIndicatorKey, Int)] = getMetrics(searchClicks)
    // Don't contain redirects
    val searchClickUniqueEventMetrics: RDD[(MainIndicatorKey, Int)] = getUniqueMetrics(searchClicks)

    MainIndicatorsResults(
      searchEventMetrics,
      searchUniqueMetrics,
      searchClickEventMetrics,
      searchClickUniqueEventMetrics,
      autoCompleteEventMetrics,
      autoCompleteUniqueEventMetrics,
      autoCompleteClickEventMetrics,
      autoCompleteClickUniqueEventMetrics)

  }

  /** Utility Case Class to Return output. */
  case class MainIndicatorsResults(searchMetrics: RDD[(MainIndicatorKey, Int)],
                                   searchUniqueMetrics: RDD[(MainIndicatorKey, Int)],
                                   searchClickMetrics: RDD[(MainIndicatorKey, Int)],
                                   searchClickUniqueMetrics: RDD[(MainIndicatorKey, Int)],
                                   autoCompleteMetrics: RDD[(MainIndicatorKey, Int)],
                                   autoCompleteUniqueMetrics : RDD[(MainIndicatorKey, Int)],
                                   autoCompleteClickMetrics: RDD[(MainIndicatorKey, Int)],
                                   autoCompleteUniqueClickMetrics: RDD[(MainIndicatorKey, Int)])

}
