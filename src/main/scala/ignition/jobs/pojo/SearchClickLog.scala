package ignition.jobs.pojo

case class SearchClickLog(apiKey: String, userId:String, query: String, feature: String)

package Parsers {

import ignition.chaordic.JsonParser
import ignition.chaordic.utils.Json
import Json.formats

class SearchClickLogParser extends JsonParser[SearchClickLog] {
  override def from(jsonStr: String): SearchClickLog = Json.parseJson4s(jsonStr).extract[SearchClickLog]
}

}