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

  lazy val logger = LoggerFactory.getLogger("ignition.search.Configuration")

  def runMode = readConfigFile.getOrElse("development")

  private def loadConfiguration = {
    logger info s"Running in $runMode mode"

    if (runMode.equalsIgnoreCase("production"))
      ConfigFactory.load("application.prod").withFallback(ConfigFactory.load())
    else
      ConfigFactory.load()
  }

  private def readConfigFile:Option[String] = try {
    val source: BufferedSource = Source.fromFile("/etc/run-mode.conf")
    val contents = Option(source.mkString.trim)
    source.close
    contents
  } catch {
    case e:Exception => None
  }

  lazy val RawConfiguration = loadConfiguration

  lazy val dashboardApiUrl = RawConfiguration.getString("dashboard-api.url")
  lazy val dashboardApiUser = RawConfiguration.getString("dashboard-api.user")
  lazy val dashboardApiPassword = RawConfiguration.getString("dashboard-api.password")

  lazy val elasticSearchClusterName = RawConfiguration.getString("elastic-search.cluster-name")
  lazy val elasticSearchHost = RawConfiguration.getString("elastic-search.url")
  lazy val elasticSearchPort = RawConfiguration.getInt("elastic-search.port")

}
