package ignition.jobs

import ignition.chaordic.pojo.{SearchEvent, SearchClickLog, SearchLog}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.reflect.ClassTag


object MainIndicators extends SearchETL {


  /** Utilities for Validation **/
  val invalidBrowsers = Set("pingdombot", "googlebot", "bingbot", "facebookbot")
  val invalidIps = Set("107.170.51.250")

  val features = Set("autocomplete", "redirect")
  val nonUniqueMetrics = Set("redirect")

  case class MainIndicatorKey(client: String, feature: String, timeKey: String, searchId: String)
  object MainIndicatorKey {
    def apply(event: SearchEvent): MainIndicatorKey = {
      MainIndicatorKey(
        event.apiKey,
        if (features.contains(event.feature)) event.feature else "search",
        event.date.toString(aggregationLevel),
        event.searchId)
    }
  }

  def getUniqueEventFromAutoComplete[T <: SearchEvent:ClassTag](events: RDD[T]): RDD[T] = {
    events
      .filter(_.feature == "autocomplete")
      .keyBy(_.searchId)
      .reduceByKey((s1, s2) => if (s1.date.isAfter(s2.date)) s1 else s2)
      .map(_._2)
  }

  def getMetrics[T <: SearchEvent:ClassTag](events: RDD[T]): RDD[(MainIndicatorKey, Int)] = {
    events
      .keyBy(MainIndicatorKey(_))
      .groupByKey()
      .mapValues(_.size)
  }

  def getUniqueMetrics[T <: SearchEvent:ClassTag](events: RDD[T]): RDD[(MainIndicatorKey, Int)] = {
    events
      .filter(event => event.feature != "redirect")
      .keyBy(MainIndicatorKey(_))
      .groupByKey()
      .mapValues(_ => 1)
  }

  def process(searchLogs: RDD[SearchLog], clickLogs: RDD[SearchClickLog]): Unit = {

    val validSearchLogs = searchLogs
      .filter(_.valid(invalidBrowsers, invalidIps))
      .filter(_.page == 1)

    val validClickLogs: RDD[SearchClickLog] = clickLogs
      .filter(_.valid(invalidBrowsers, invalidIps))


    // AutoComplete
    val autoCompleteEvents: RDD[SearchLog] = getUniqueEventFromAutoComplete(validSearchLogs)
    val autoCompleteClicks: RDD[SearchClickLog] =  getUniqueEventFromAutoComplete(validClickLogs)

    // Search
    // Counts All Search + Redirects
    val searchEvents: RDD[SearchLog] = validSearchLogs
      .filter(searchLog => searchLog.feature != "autocomplete")

    val searchClicks: RDD[SearchClickLog] = validClickLogs
      .filter(clickLog => clickLog.feature != "autocomplete")

    // SEARCH EVENTS
    // Contains redirects
    val searchEventMetrics: RDD[(MainIndicatorKey, Int)] = getMetrics(searchEvents)
    // Don't contain redirects
    val searchUniqueMetrics: RDD[(MainIndicatorKey, Int)] = getUniqueMetrics(searchEvents)

    // SEARCH CLICKS
    val searchClickEventMetrics: RDD[(MainIndicatorKey, Int)] = getMetrics(searchClicks)
    // Don't contain redirects
    val searchClickUniqueEventMetrics: RDD[(MainIndicatorKey, Int)] = getUniqueMetrics(searchClicks)

    // AUTOCOMPLETE EVENTS
    val autoCompleteEventMetrics: RDD[(MainIndicatorKey, Int)] = getMetrics(autoCompleteEvents)
    // Don't contain redirects
    val autoCompleteUniqueEventMetrics: RDD[(MainIndicatorKey, Int)] = getUniqueMetrics(autoCompleteEvents)

    // AUTOCOMPLETE CLICKS
    val autoCompleteClickEventMetrics: RDD[(MainIndicatorKey, Int)] = getMetrics(autoCompleteClicks)
    // Don't contain redirects
    val autoCompleteClickUniqueEventMetrics: RDD[(MainIndicatorKey, Int)] = getUniqueMetrics(autoCompleteClicks)

  }


}
