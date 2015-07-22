package ignition.jobs.utils.uploader

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import spray.json._


object ParsingUtils {

  implicit object DateTimeJsonFormat extends JsonFormat[DateTime] {
    def write(x: DateTime) = {
      require(x ne null)
      JsString(ISODateTimeFormat.dateTimeNoMillis.withZoneUTC.print(x))
    }
    def read(value: JsValue) = value match {
      case JsString(s) => try {
        ISODateTimeFormat.dateTimeParser.withZoneUTC.parseDateTime(s)
      } catch {
        case ex: IllegalArgumentException => deserializationError("Fail to parse DateTime string: " + s, ex)
      }
      case s => deserializationError("Expected JsString, but got " + s)
    }
  }

}
