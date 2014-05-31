package ignition.core.utils

import org.json4s._

object JsonUtils {

  implicit class JsonImprovements(val obj: JValue) {
    /**
      * Checks if the object has all the fields present
      */
    def has(field: String): Boolean = {
      obj \ field != JNothing
    }
    
    def hasAll(fields: String*): Boolean = {
      fields.toStream.forall(obj.has)
    }
    
    def getOptString(name: String): Option[String] = {
      // pass through Option to transform null into None
      // Avoid extractOpt because it's very slow
      obj \ name match {
      	case JString(t) => Option(t)
      	case _ => None
      }
    }

    def toOptSeqString: Option[Seq[String]] = {
      obj match {
        case JArray(list) => Option(list
          .flatMap({
            case JString(value) => Option(value)
            case _ => None
          }))
        case JString(value) if value != null => Option(Seq(value))
        case _ => None
      }
    }
    
    implicit lazy val formats = DefaultFormats
  }
}