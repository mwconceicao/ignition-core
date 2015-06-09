package ignition.jobs

import ignition.chaordic.Chaordic
import ignition.chaordic.pojo.Parsers.ProductV2Parser
import org.joda.time.DateTime
import org.scalatest.{ShouldMatchers, FlatSpec}

class SitemapJobSpec extends FlatSpec with ShouldMatchers {

  val p =
    (new ProductV2Parser()).from("""
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

  "Search" should "create link for product" in {
    val links = SitemapXMLJob.generateLink(p, "teste", List(Set("publisher")))
    links shouldBe Seq.empty
  }
}
