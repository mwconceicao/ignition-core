package ignition.jobs

import ignition.chaordic.pojo.{SearchClickLog, SearchProductInfo, SearchProduct, SearchLog}
import ignition.core.testsupport.spark.SharedSparkContext
import ignition.jobs.TestTags.SlowTest
import org.joda.time.DateTime
import org.scalacheck.Gen
import org.scalatest._
import ignition.chaordic.pojo.ChaordicGenerators._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

trait SearchGenerators {
  def searchProductGenerator(gId: Gen[String] = Gen.alphaStr,
                             gPurchaseWeight: Gen[Double] = Gen.chooseNum(0, 5),
                             gScore: Gen[Double] = Gen.chooseNum(-1000, 1000),
                             gViewWeight: Gen[Double] = Gen.chooseNum(0, 5)): Gen[SearchProduct] = {
    for {
      id <- gId
      purchaseWeight <- gPurchaseWeight
      score <- gScore
      viewWeight <- gViewWeight
    } yield {
      SearchProduct(id = id, info = SearchProductInfo(purchase_weight = purchaseWeight, score = score, view_weight = viewWeight))
    }
  }

  def searchClickLogGenerator(gApiKey: Gen[String] = gApiKey,
                              gDate: Gen[DateTime] = dateGenerator(),
                              gFeature: Gen[String] = gFeature,
                              gQuery: Gen[String] = Gen.alphaStr,
                              gUserId: Gen[String] = Gen.alphaStr,
                              gInfo: Gen[Map[String, String]] = gInfo): Gen[SearchClickLog] = {
    for {
      apiKey <- gApiKey
      date <- gDate
      query <- gQuery
      feature <- gFeature
      userId <- gUserId
      info <- gInfo
    } yield SearchClickLog(apiKey, userId, date, query, feature, info)
  }

  def searchLogGenerator(gApiKey: Gen[String] = gApiKey,
                         gDate: Gen[DateTime] = dateGenerator(),
                         gFeature: Gen[String] = gFeature,
                         gProducts: Gen[List[SearchProduct]] = gProducts,
                         gFilters: Gen[Option[Map[String, List[String]]]] = Gen.option(Gen.mapOf(gFilter)),
                         gPage: Gen[Int] = Gen.chooseNum(1, 15),
                         gPageSize: Gen[Option[Int]] = Gen.option(Gen.oneOf(10, 25, 50)),
                         gTotalFound: Gen[Int] = Gen.chooseNum(0, 100),
                         gQuery: Gen[String] = Gen.alphaStr,
                         gUserId: Gen[String] = Gen.alphaStr,
                         gOrder: Gen[Option[String]] = Gen.option(Gen.alphaStr),
                         gInfo: Gen[Map[String, String]] = gInfo): Gen[SearchLog] = {
    for {
      apiKey <- gApiKey
      date <- gDate
      feature <- gFeature
      page <- gPage
      products <- gProducts
      pageSize <- gPageSize
      totalFound <- gTotalFound
      filters <- gFilters
      query <- gQuery
      userId <- gUserId
      order <- gOrder
      info <- gInfo
    } yield {
      SearchLog(apiKey = apiKey,
        date = date,
        feature = feature,
        page = page,
        products = products,
        pageSize = pageSize,
        totalFound = totalFound,
        filters = filters,
        query = query,
        userId = userId,
        order = order,
        info = info)
    }
  }

  val gInfo = Gen.alphaStr.map(id => Map("searchId" -> id))

  val gProducts = Gen.listOf(searchProductGenerator())

  val gFilter = Gen.oneOf("ano", "author", "brand", "color", "fabricante", "Genero", "idioma", "material", "price_ranges",
    "rootCategory", "tamanho", "Tipo", "voltage").flatMap(filter => Gen.listOf(Gen.alphaStr).map((filter, _)))

  val gApiKey = Gen.oneOf("camisariacolombo", "imaginarium-v5", "insinuante", "lenovo", "lojasrenner", "madeiramadeira",
    "marcyn", "marisa", "mmm-v5", "mobly-v5", "peixeurbano", "ricardoeletro", "saraiva-v5", "staples", "telhanorte", "vivara")

  val gFeature = Gen.oneOf("rank-fallback", "redirect", "spelling-fallback", "standard", "autocomplete")
}

class TopQueriesJobSpec extends FlatSpec with ShouldMatchers with SharedSparkContext with GeneratorDrivenPropertyChecks with SearchGenerators {

  implicit override val generatorDrivenConfig = PropertyCheckConfig(workers = 4)

  "TopQueriesJob" should "calculate top queries" taggedAs SlowTest in {
    val queries = Gen.frequency(
      (20, "banco imobiliário"),
      (20, "cerveja"),
      (15, "o guia do mochileiro das galáxias"),
      (10, "blu-ray 3d"),
      (10, "box livros"),
      (5, "o mercado de ações ao seu alcance"),
      (5, "fisica classica"),
      (5, "micro computadores"),
      (5, "emergencias clinicas"),
      (5, "caixa d'água 500 litros")
    )
    val gSearchLog = Gen.listOfN(500, searchLogGenerator(gQuery = queries))
    forAll(gSearchLog) { logs =>
      val rdd = sc.parallelize(logs)
      val allTopQueries = TopQueriesJob.execute(rdd).collect()
      allTopQueries.foreach { topQueries =>
        val topQuery = topQueries.topQueries.sortBy(_.count).reverse.head.query
        topQuery == "banco imobiliário" || topQuery == "cerveja" || topQuery == "o guia do mochileiro das galáxias"
      }
    }
  }

  it should "filter invalid events" taggedAs SlowTest in {
    val invalidQueries = Set("pigdom")
    val invalidIpAddresses = Set("107.170.51.250")
    val invalidBrowser = Map("browser_family" -> "bot") // FIXME this is hardcoded in TopQueriesJob private property
    val invalidIp = Map("ip" -> invalidIpAddresses.head) // FIXME this is hardcoded in TopQueriesJob private property
    val gInvalidInfo = gInfo.flatMap(info => Gen.oneOf(invalidBrowser, invalidIp).map(info ++ _) )

    // FIXME this is hardcoded in TopQueriesJob private property
    val gSearchLog = searchLogGenerator(gInfo = Gen.oneOf(gInfo, gInvalidInfo), gQuery = Gen.oneOf(Gen.alphaStr, Gen.const(invalidQueries.head)))
      .suchThat(event => !event.valid(invalidQueries, invalidIpAddresses))

    forAll(Gen.nonEmptyListOf(gSearchLog)) { logs =>
      val rdd = sc.parallelize(logs)
      val topQueries = TopQueriesJob.execute(rdd).collect()
      topQueries.isEmpty shouldBe true
    }
  }

}
