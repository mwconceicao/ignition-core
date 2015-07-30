package ignition.jobs

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.io.{BufferedSource, Source}

/**
 * The configuration looks up to one or two resources: 'application.conf' and 'application.prod.conf'.
 * If the run mode is set to production, it will look up properties on application.prod.conf first, and fall back
 * to application.conf in case the configuration was not found.
 * In development mode application.conf is the sole configuration file.
 *
 * The run mode is determined by looking up the 'RUN_MODE' environment variable. Absence, or anything different from
 * 'production' (case insensitive), is considered development.
 *
 * Usage tip: Put all default and development configurations on 'application.conf', and override development
 * configurations with production ones in 'application.prod.conf'. Then create a val in the Configuration object
 * assigning the configuration a type.
 */
object Configuration {
  /**
   * Based on enviromnet variable 'RUN_MODE', loadConfiguration will load either application.conf or
   * application.prod.conf (with fallback to application.conf).
   */
  lazy val RawConfiguration = ConfigFactory.load()


  lazy val dashboardApiUrl = RawConfiguration.getString("dashboard-api.url")
  lazy val dashboardApiUser = RawConfiguration.getString("dashboard-api.user")
  lazy val dashboardApiPassword = RawConfiguration.getString("dashboard-api.password")

  lazy val elasticSearchAPI = RawConfiguration.getString("elasticsearch.api")
  lazy val elasticSearchReport = RawConfiguration.getString("elasticsearch.report")
  lazy val elasticSearchPort = RawConfiguration.getInt("elasticsearch.port")
  lazy val elasticSearchTimeoutInMinutes = RawConfiguration.getInt("elasticsearch.bulk-timeout-in-minutes")
  lazy val elasticSearchBulk = RawConfiguration.getInt("elasticsearch.bulk-size")

}
