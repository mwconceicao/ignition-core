package ignition.core.jobs.utils

import org.joda.time.DateTime

object PathUtils {

  private val datePatterns = Seq("")

  def extractDate(path: String): DateTime = {

    DateTime.now
  }

}
