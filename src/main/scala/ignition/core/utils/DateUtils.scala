package ignition.core.utils

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object DateUtils {
  private val isoDateTimeFormatter = ISODateTimeFormat.dateTime()

  implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

  implicit class DateTimeImprovements(val dateTime: DateTime) {
    def toIsoString = isoDateTimeFormatter.print(dateTime)
  }
}
