package ignition.jobs.pojo

import org.joda.time.DateTime

case class SearchProductInfo(purchase_weight: Double, score: Double, view_weight: Double)
case class SearchProduct(info: SearchProductInfo, id: String)

case class SearchLog(apiKey: String, date: DateTime, feature: String, filters: Option[Map[String, List[String]]],
                     order: String, page: Int, pageSize: Int, products: List[SearchProduct], query: String,
                     totalFound: Int, userId: String, info: Map[String, String])
package Parsers {

import ignition.chaordic.{Chaordic, JsonParser}
import ignition.chaordic.utils.Json
import org.json4s.JsonAST.JObject

case class SearchLogRaw(apiKey: String, date: DateTime, feature: String, filters: Option[Map[String, List[String]]],
                     order: String, page: Int, pageSize: Int, products: List[SearchProduct], query: String,
                     totalFound: Int, userId: String, info: JObject)

  class SearchLogParser extends JsonParser[SearchLog] {
    import Json.formats

    def fromRaw(raw: SearchLogRaw): SearchLog = {
      val info = Chaordic.normalizeStringMap(raw.info.values)
      SearchLog(
        apiKey = raw.apiKey,
        date = raw.date,
        feature = raw.feature,
        filters = raw.filters,
        order = raw.order,
        page = raw.page,
        pageSize = raw.pageSize,
        products = raw.products,
        query = raw.query,
        totalFound = raw.totalFound,
        userId = raw.userId,
        info = info
      )
    }

    override def from(jsonStr: String): SearchLog =
      fromRaw(Json.parseJson4s(jsonStr).extract[SearchLogRaw])
  }

}
