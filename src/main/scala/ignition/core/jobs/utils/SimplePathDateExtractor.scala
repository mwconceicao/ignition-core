package ignition.core.jobs.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try

// This is a very simple date handling. It should be used as a source/example for more complex ones
trait SimplePathDateExtractor extends PathDateExtractor {

  // We work on path segments (segment1/segment2/segment3 ...)
  protected val datePatterns = Seq(
    DateTimeFormat.forPattern("'dt='yyyy-MM-dd").withZoneUTC(), // secor format
    DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC(), // abbreviated iso format
    DateTimeFormat.forPattern("yyyy_MM_dd'T'HH_mm_ss'UTC'").withZoneUTC()) // ignition format

  override def extractFromPath(path: String): DateTime = {
    val segments = path.split("/").toStream
    val parsedDates = for {
      s <- segments
      pattern <- datePatterns
      parsedDate <- Try { DateTime.parse(s, pattern) }.toOption
    } yield parsedDate
    val parsedDate = parsedDates.headOption // get first if more than one
    if (parsedDate.isDefined) {
      parsedDate.get
    } else {
        throw new Exception(s"All date parsers failed for path $path")
    }
  }

}

object SimplePathDateExtractor {
  implicit val default = new SimplePathDateExtractor {}
}
