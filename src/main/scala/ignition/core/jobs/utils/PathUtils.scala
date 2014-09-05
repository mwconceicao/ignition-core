package ignition.core.jobs.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try

object PathUtils {

  private val datePatterns = Seq(DateTimeFormat.forPattern("'dt='yyyy-MM-dd"), // mail-dumper format
    DateTimeFormat.forPattern("yyyy-MM-dd"), // platform format
    DateTimeFormat.forPattern("yyyyMMdd-HHmmss"), // engine format
    DateTimeFormat.forPattern("yyyyMMdd_HHmmss"), // EP format
    DateTimeFormat.forPattern("yyyy_MM_dd'T'HH_mm_ss'UTC'")) // ignition format

  def extractDate(path: String): DateTime = {
    val segments = path.split("/")
    val parsedDates = for {
      s <- segments
      pattern <- datePatterns
      parsedDate <- Try { DateTime.parse(s, pattern) }.toOption
    } yield parsedDate
    if (parsedDates.length == 1)
      parsedDates.head
    else
      throw new Exception(s"Failed to find a single date for path $path, found: $parsedDates")
  }

}
