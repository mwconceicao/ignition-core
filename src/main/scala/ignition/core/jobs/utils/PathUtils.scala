package ignition.core.jobs.utils

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.DateTimeFormat

import scala.util.Try

object PathUtils {

  private val datePatterns = Seq(DateTimeFormat.forPattern("'dt='yyyy-MM-dd").withZoneUTC(), // mail-dumper format
    DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC(), // platform format
    DateTimeFormat.forPattern("yyyyMMdd-HHmmss").withZoneUTC(), // engine format
    DateTimeFormat.forPattern("yyyyMMdd_HHmmss").withZone(DateTimeZone.forID("America/Sao_Paulo")), // EP format
    DateTimeFormat.forPattern("yyyy_MM_dd'T'HH_mm_ss'UTC'").withZoneUTC()) // ignition format

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
