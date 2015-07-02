package ignition.jobs.pojo

import org.joda.time.DateTime

case class SearchProductInfo(purchase_weight: Double, score: Double, view_weight: Double)
case class SearchProduct(info: SearchProductInfo, id: String)

case class SearchLog(apiKey: String, date: DateTime, feature: String, filters: Option[Map[String, List[String]]],
                     order: String, page: Int, pageSize: Int, products: List[SearchProduct], query: String,
                     totalFound: Int, userId: String, info: Map[String, String])
package Parsers {

import ignition.chaordic.JsonParser
import ignition.chaordic.utils.Json
import Json.formats

class SearchLogParser extends JsonParser[SearchLog] {
  override def from(jsonStr: String): SearchLog = Json.parseJson4s(jsonStr).extract[SearchLog]
}

}
