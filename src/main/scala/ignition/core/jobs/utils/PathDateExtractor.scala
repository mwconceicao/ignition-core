package ignition.core.jobs.utils

import org.joda.time.DateTime

trait PathDateExtractor {
  def extractFromPath(path: String): DateTime
}
