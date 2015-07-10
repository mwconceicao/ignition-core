package ignition.jobs.utils

import ignition.chaordic.pojo.SearchEvent

object SearchEventValidations {

  implicit class SearchEventValidations(event: SearchEvent) {

    private val invalidBrowsersPattern = "bot"

    def valid(invalidQueries: Set[String], invalidIps: Set[String]): Boolean =
      validQuery(invalidQueries) && validBrowser && validIp(invalidIps)

    /**
     * Check if search log query is not empty and it is different from "pingdom".
     * @return Bool that indicate if query is valid.
     */
    def validQuery(invalidQueries: Set[String]): Boolean =
      !invalidQueries.contains(event.query.trim.toLowerCase)

    /**
     * Check if search log browser is not empty and it is a valid browser.
     * @return Bool to indicate if browser is valid.
     */
    def validBrowser: Boolean =
      !event.browser.trim.toLowerCase.contains(invalidBrowsersPattern)

    /**
     * Check if search log has a valid ip.
     * @return Bool to indicate if IP is valid.
     */
    def validIp(invalidIps: Set[String]): Boolean =
      !invalidIps.contains(event.ip)

  }

}
