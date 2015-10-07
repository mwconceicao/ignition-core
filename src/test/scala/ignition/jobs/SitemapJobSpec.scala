package ignition.jobs

import ignition.chaordic.pojo.Parsers.{SearchClickLogParser, ProductV2Parser, SearchLogParser}
import ignition.core.testsupport.spark.SharedSparkContext
import ignition.jobs.utils.SearchApi.SitemapConfig
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, ShouldMatchers}

class SitemapJobSpec extends FlatSpec with ShouldMatchers with SharedSparkContext {

  val p =
    new ProductV2Parser().from("""
                                   |{
                                   |  "version": "V2",
                                   |  "tags": [
                                   |    {
                                   |      "parents": null,
                                   |      "name": "Literatura Estrangeira",
                                   |      "id": "literatura_estrangeira"
                                   |    },
                                   |    {
                                   |      "parents": null,
                                   |      "name": "Livro Digital",
                                   |      "id": "livro_digital"
                                   |    },
                                   |    {
                                   |      "parents": null,
                                   |      "name": "Produtos Digitais",
                                   |      "id": "produtos_digitais"
                                   |    }
                                   |  ],
                                   |  "categories": [
                                   |    {
                                   |      "parents": [
                                   |        "Livro Digital"
                                   |      ],
                                   |      "name": "Literatura Estrangeira",
                                   |      "id": "Literatura Estrangeira"
                                   |    },
                                   |    {
                                   |      "parents": [
                                   |        "Produtos Digitais"
                                   |      ],
                                   |      "name": "Livro Digital",
                                   |      "id": "Livro Digital"
                                   |    },
                                   |    {
                                   |      "parents": null,
                                   |      "name": "Produtos Digitais",
                                   |      "id": "Produtos Digitais"
                                   |    }
                                   |  ],
                                   |  "unit": null,
                                   |  "eanCode": null,
                                   |  "stock": null,
                                   |  "brand": null,
                                   |  "remoteUrl": null,
                                   |  "url": "www.saraiva.com.br/o-olho-do-mundo-col-a-roda-do-tempo-livro-1-5297675.html",
                                   |  "id": "5297675",
                                   |  "apiKey": "saraiva-v5",
                                   |  "status": "AVAILABLE",
                                   |  "name": "O Olho do Mundo - Col. A Roda do Tempo -  Livro 1",
                                   |  "price": 17.91,
                                   |  "oldPrice": 19.9,
                                   |  "basePrice": null,
                                   |  "description": "Um dia houve uma guerra tão definitiva que rompeu o mundo, e no girar da Roda do Tempo o que ficou na memória dos homens virou esteio das lendas. Como a que diz que, quando as forças tenebrosas se reerguerem, o poder de combatê-las renascerá em um único homem, o Dragão, que trará de volta a guerra e, de novo, tudo se fragmentará.Nesse cenário em que trevas e redenção são igualmente temidas, vive Rand al’Thor, um jovem de uma vila pacata na região dos Dois Rios. É a época dos festejos de final de inverno — o mais rigoroso das últimas décadas —, e mesmo na agitação que antecipa o festival, chama a atenção a chegada de uma misteriosa forasteira.Quando a vila é invadida por bestas que para a maioria dos homens pertenciam apenas ao universo das lendas, a mulher não só ajuda Rand e seus amigos a escapar dali, como os conduz àquela que será a maior de todas as jornadas. A desconhecida é uma Aes Sedai, artífice do poder que move a Roda do Tempo, e acredita que Rand seja o profético Dragão Renascido — aquele que poderá salvar ou destruir o mundo.",
                                   |  "images": {
                                   |    "default": "images.livrariasaraiva.com.br/imagemnet/imagem.aspx/?pro_id=5297675"
                                   |  },
                                   |  "imagesSsl": {},
                                   |  "installment": null,
                                   |  "kitProducts": [],
                                   |  "specs": {},
                                   |  "details": {
                                   |    "preVenda": "",
                                   |    "idioma": "Português",
                                   |    "numeroDePaginas": "800 (aproximado)",
                                   |    "sample": "N",
                                   |    "bestseller_factor": "0.00000000",
                                   |    "release_date": "08/2013",
                                   |    "isbn": "9788580573626",
                                   |    "publisher": "Intrinseca (Edição Digital)",
                                   |    "author": "Jordan, Robert",
                                   |    "volume": "1",
                                   |    "territorialidade": "Brasil",
                                   |    "formatoLivroDigital": "Epub",
                                   |    "colecao/Serie": "A Roda do Tempo",
                                   |    "tamanhoDoArquivo": "8917",
                                   |    "inicioDaVenda": "02/09/2013",
                                   |    "rating": {
                                   |      "total": 0,
                                   |      "value": 0
                                   |    },
                                   |    "produtoDigital": "sim"
                                   |  },
                                   |  "skus": [
                                   |    {
                                   |      "properties": {},
                                   |      "specs": {},
                                   |      "sku": "2972350"
                                   |    }
                                   |  ],
                                   |  "isRecommendation": false,
                                   |  "created": "2013-09-03 01:21:17",
                                   |  "clientLastUpdated": "2015-06-09 00:00:01",
                                   |  "published": null,
                                   |  "extraInfo": {
                                   |    "hash": ""
                                   |  },
                                   |  "auditInfo": {
                                   |    "updatedByImporterAt": "2015-06-02 05:30:05",
                                   |    "updatedBy": "importer",
                                   |    "updatedBySyncAt": "2015-04-21 13:47:00",
                                   |    "updatedBySync2At": "2015-03-30T01:38:23"
                                   |  },
                                   |  "syncBlacklisted": false,
                                   |  "organicRemoval": false,
                                   |  "type": "product"
                                   |}
                                   |
                                 """.stripMargin)

  val searchLogs = Seq(
    """
      |{"apiKey":"saraiva-v5","query":"watchman","pageSize":45,"totalFound":5,"userId":"anon-cd7af330-66dd-11e4-b19e-61a4930b63fd","filters":{"rootCategory":["Música"]},"date":"2015-06-01T00:00:00.874836","info":{"searchId":"2c6551ad-f292-4aea-9a40-2cb5a0aecc9b","resultsExpansion":false,"ip":"177.23.94.32","personalizeResults":false,"realUserId":null,"queryTime":61,"relatedQuery":null,"sesssion":"1433125746433-0.17949257581494749","chaordic_testGroup":{"code":null,"testCode":null,"experiment":null,"group":null,"session":null},"hasSynonyms":false,"browser_family":"Chrome","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":1.0375755736583676,"score":900011.06,"view_weight":0.88442279856244},"id":"2630839"},{"info":{"purchase_weight":0,"score":-1999999999.9,"view_weight":0},"id":"1320813"},{"info":{"purchase_weight":0,"score":-1999999999.9,"view_weight":0},"id":"1446738"},{"info":{"purchase_weight":0,"score":-1999999999.9,"view_weight":0},"id":"1229212"},{"info":{"purchase_weight":0,"score":-1999999999.9,"view_weight":0},"id":"798373"}],"order":"popularity"}
    """.stripMargin,
    """
      |{"apiKey":"saraiva-v5","query":"estante home","pageSize":48,"totalFound":0,"userId":"anon-34a6d2f4-080a-11e5-9fc8-024ce8a621bb","filters":null,"date":"2015-06-01T00:00:04.090027","info":{"searchId":"66ecf418-b15a-4d8d-9a55-c759d1dd1ef8","resultsExpansion":true,"ip":"66.249.64.146","personalizeResults":false,"realUserId":null,"queryTime":17,"relatedQuery":null,"sesssion":null,"chaordic_testGroup":null,"hasSynonyms":false,"browser_family":"Googlebot","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3353748,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3978953,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.1102133,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.5257285,"view_weight":0},"id":"127209"}],"order":null}
    """.stripMargin,
    """
      |{"apiKey":"saraiva-v5","query":"livro%2Bromance","pageSize":48,"totalFound":0,"userId":"anon-34a6d2f4-080a-11e5-9fc8-024ce8a621bb","filters":null,"date":"2015-06-01T00:00:04.090027","info":{"searchId":"66ecf418-b15a-4d8d-9a55-c759d1dd1ef8","resultsExpansion":true,"ip":"66.249.64.146","personalizeResults":false,"realUserId":null,"queryTime":17,"relatedQuery":null,"sesssion":null,"chaordic_testGroup":null,"hasSynonyms":false,"browser_family":"Googlebot","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3353748,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3978953,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.1102133,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.5257285,"view_weight":0},"id":"127209"}],"order":null}
    """.stripMargin,
    """
      |{"apiKey":"saraiva-v5","query":"livro  romance","pageSize":48,"totalFound":0,"userId":"anon-34a6d2f4-080a-11e5-9fc8-024ce8a621bb","filters":null,"date":"2015-06-01T00:00:04.090027","info":{"searchId":"66ecf418-b15a-4d8d-9a55-c759d1dd1ef8","resultsExpansion":true,"ip":"66.249.64.146","personalizeResults":false,"realUserId":null,"queryTime":17,"relatedQuery":null,"sesssion":null,"chaordic_testGroup":null,"hasSynonyms":false,"browser_family":"Googlebot","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3353748,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3978953,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.1102133,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.5257285,"view_weight":0},"id":"127209"}],"order":null}
    """.stripMargin,
    """
      |{"apiKey":"saraiva-v5","query":"livro++romance+""pageSize":48,"totalFound":0,"userId":"anon-34a6d2f4-080a-11e5-9fc8-024ce8a621bb","filters":null,"date":"2015-06-01T00:00:04.090027",,"info":{"searchId":"66ecf418-b15a-4d8d-9a55-c759d1dd1ef8","resultsExpansion":true,"ip":"66.249.64.146","personalizeResults":false,"realUserId":null,"queryTime":17,"relatedQuery":null,"sesssion":null,"chaordic_testGroup":null,"hasSynonyms":false,"browser_family":"Googlebot","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3353748,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3978953,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.1102133,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.5257285,"view_weight":0},"id":"127209"}],"order":null}
    """.stripMargin,
    """
      |{"apiKey":"saraiva-v5","query":"livro-romance","pageSize":48,"totalFound":0,"userId":"anon-34a6d2f4-080a-11e5-9fc8-024ce8a621bb","filters":null,"date":"2015-06-01T00:00:04.090027","info":{"searchId":"66ecf418-b15a-4d8d-9a55-c759d1dd1ef8","resultsExpansion":true,"ip":"66.249.64.146","personalizeResults":false,"realUserId":null,"queryTime":17,"relatedQuery":null,"sesssion":null,"chaordic_testGroup":null,"hasSynonyms":false,"browser_family":"Googlebot","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3353748,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3978953,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.1102133,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.5257285,"view_weight":0},"id":"127209"}],"order":null}
    """.stripMargin,
    """
      |{"apiKey":"saraiva-v5","query":"livro rômance","pageSize":48,"totalFound":0,"userId":"anon-34a6d2f4-080a-11e5-9fc8-024ce8a621bb","filters":null,"date":"2015-06-01T00:00:04.090027","info":{"searchId":"66ecf418-b15a-4d8d-9a55-c759d1dd1ef8","resultsExpansion":true,"ip":"66.249.64.146","personalizeResults":false,"realUserId":null,"queryTime":17,"relatedQuery":null,"sesssion":null,"chaordic_testGroup":null,"hasSynonyms":false,"browser_family":"Googlebot","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3353748,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3978953,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.1102133,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.5257285,"view_weight":0},"id":"127209"}],"order":null}
    """.stripMargin,
    """
      |{"apiKey":"saraiva-v5","query":"Livro Romance","pageSize":48,"totalFound":0,"userId":"anon-34a6d2f4-080a-11e5-9fc8-024ce8a621bb","filters":null,"date":"2015-06-01T00:00:04.090027","info":{"searchId":"66ecf418-b15a-4d8d-9a55-c759d1dd1ef8","resultsExpansion":true,"ip":"66.249.64.146","personalizeResults":false,"realUserId":null,"queryTime":17,"relatedQuery":null,"sesssion":null,"chaordic_testGroup":null,"hasSynonyms":false,"browser_family":"Googlebot","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3353748,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3978953,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.1102133,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.5257285,"view_weight":0},"id":"127209"}],"order":null}
    """.stripMargin,
    """
      |{"apiKey":"saraiva-v5","query":"livro%20romance","pageSize":48,"totalFound":0,"userId":"anon-34a6d2f4-080a-11e5-9fc8-024ce8a621bb","filters":null,"date":"2015-06-01T00:00:04.090027","info":{"searchId":"66ecf418-b15a-4d8d-9a55-c759d1dd1ef8","resultsExpansion":true,"ip":"66.249.64.146","personalizeResults":false,"realUserId":null,"queryTime":17,"relatedQuery":null,"sesssion":null,"chaordic_testGroup":null,"hasSynonyms":false,"browser_family":"Googlebot","forceOriginal":false},"feature":"standard","page":1,"products":[{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":3.043074,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3353748,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.3978953,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"21178"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"22578"},{"info":{"purchase_weight":0,"score":2.5505974,"view_weight":0},"id":"68774"},{"info":{"purchase_weight":0,"score":3.8903718,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.1102133,"view_weight":0},"id":"127209"},{"info":{"purchase_weight":0,"score":3.5257285,"view_weight":0},"id":"127209"}],"order":null}
    """.stripMargin
  ).map(new SearchLogParser().from)

  val config = SitemapConfig(host = "myhost", generatePages = true, generateSearch = true, details = Set("detailField1"), useDetails = true, maxSearchItems = 100, numberPartFiles = 3)

  val searchClicks = Seq(
    """
      |{"apiKey":"saraiva-v5","userId":null,"paginationInfo":{"pageIndex":0,"itemIndex":0,"pageItems":45},"anonymous":true,"date":"2015-06-01T00:00:00.231446","query":"watchman","info":{"requestExpansion":false,"searchId":"f2ec02a2-e820-41c7-84fa-da03611f5039","sesssion":null,"chaordic_testGroup":null,"url":"http://www.saraiva.com.br/o-jardim-secreto-336062.html","ip":"66.249.64.133","realUserId":null,"browser_family":"Googlebot"},"feature":"search","version":"V2","products":[{"sku":null,"price":34.0,"id":"336062"}],"type":"clicklog","page":"search","interactionType":"PRODUCT_DETAILS"}
    """.stripMargin
  ).map(new SearchClickLogParser().from)

  "Search" should "create link for product based on *categories" in {
    // Note: ignoring details for now because its semantics will change soon
    val links = SitemapXMLPagesJob.generateLink(config.copy(useDetails = false), p, List(Set("publisher")))
    links shouldBe List("http://myhost/pages/produtos-digitais",
      "http://myhost/pages/produtos-digitais/livro-digital",
      "http://myhost/pages/produtos-digitais/livro-digital/literatura-estrangeira")
  }

  it should "create link for search logs" in {
    val result = SitemapXMLSearchJob.generateSearchUrlXMLs(sc, DateTime.now,
      sc.parallelize(searchLogs),
      sc.parallelize(searchClicks), config).collect()

    val sorted_result = result.sorted
    sorted_result.size shouldBe 3
    sorted_result(0).contains("<loc>http://myhost/?q=estante+home</loc>") shouldBe true
    sorted_result(1).contains("<loc>http://myhost/?q=livro+romance</loc>") shouldBe true
    sorted_result(2).contains("<loc>http://myhost/?q=watchman</loc>") shouldBe true
  }
}
