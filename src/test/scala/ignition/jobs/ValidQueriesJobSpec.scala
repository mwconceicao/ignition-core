package ignition.jobs


import ignition.chaordic.pojo.Parsers.{SearchClickLogParser, SearchLogParser}
import ignition.chaordic.pojo.{SearchClickLog, SearchLog}
import ignition.core.testsupport.spark.SharedSparkContext
import ignition.core.utils.BetterTrace
import ignition.jobs.pojo.{ValidQueries, ValidQuery}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, ShouldMatchers}

class ValidQueriesJobSpec extends FlatSpec with ShouldMatchers with SharedSparkContext with BetterTrace with SearchGenerators with GeneratorDrivenPropertyChecks {

  import ValidQueriesJob._

  "SearchEventImprovements" should "provide a validDate function" in {
    forAll(Gen.oneOf(searchLogGenerator(), searchClickLogGenerator())) {
      searchEvent => searchEvent.validDate match {
        case true => searchEvent.date.isAfter(DateTime.now.minusDays(365)) should be(true)
        case false => searchEvent.date.isAfter(DateTime.now.minusDays(365)) should be(false)
      }
    }
  }

  "TypoRemover" should "remove any combination of `defaultTypos` in the end of the string" in {
    forAll(Gen.alphaStr) {
      str =>
        str.removeTyposAtQueryTail.endsWith("~") should be(false)
        str.removeTyposAtQueryTail.endsWith("'") should be(false)
        str.removeTyposAtQueryTail.endsWith("[") should be(false)
    }
  }

  "normalizeQuery" should "return a query without endTypos and lowercased" in {
    forAll(Gen.alphaStr) {
      str => normalizeQuery(str) match {
        case s =>
          s.endsWith("~") || s.endsWith("'") || s.endsWith("[") should be(false)
          str.toLowerCase should be(s)
      }
    }
  }

  "queryToKey" should "remove all spaces and asciifold" in {
    forAll(Gen.alphaStr) {
      str =>
        queryToKey(str).contains(" ") should be(false)
      // AsciiFolding is tested in utils.text
    }
  }

  "SearchLogsImprovements" should "check if a SearchLog have filters or not" in {
    forAll(searchLogGenerator()) {
      searchLog => searchLog.hasFilters match {
        case true => searchLog.filters.isDefined && searchLog.filters.get.nonEmpty should be(true)
        case false => searchLog.filters.isDefined && searchLog.filters.get.nonEmpty should be(false)
      }
    }
  }

  "parseEvents" should "properly parse a searchLog" in {
    withBetterTrace {
      val event: SearchLog =
        new SearchLogParser().from( s"""{
                                       | "info": {
                                       |   "searchId": "779759e2-76fb-4ffe-b477-7f7a59654884",
                                       |   "ip": "201.92.214.99",
                                       |   "personalizeResults": true,
                                       |   "forceOriginal": false,
                                       |   "browser_family": "Chrome",
                                       |   "originalResultListing": [
                                       |     "501509"
                                       |   ]
                                       | },
                                       | "apiKey": "marcyn",
                                       | "pageSize": 3,
                                       | "totalFound": 100,
                                       | "userId": "anon-8993c800-179f-11e4-877a-4dacbde6a7e4",
                                       | "feature": "standard",
                                       | "page": 1,
                                       | "products": [
                                       |   {"id": "501509",
                                       |    "info": {
                                       |      "purchase_weight": 5.419288057975395,
                                       |      "score": 1392916.5,
                                       |      "view_weight":12.797831187927807}
                                       |   },
                                       |   {"id": "501510",
                                       |    "info": {
                                       |      "purchase_weight": 5.419288057975395,
                                       |      "score": 1392916.5,
                                       |      "view_weight":12.797831187927807}
                                       |   },
                                       |   {"id": "501511",
                                       |    "info": {
                                       |      "purchase_weight": 5.419288057975395,
                                       |      "score": 1392916.5,
                                       |      "view_weight":12.797831187927807}
                                       |   }
                                       | ],
                                       | "filters": null,
                                       | "date": "${DateTime.now.toString}",
                                                                            | "query": "almofada de pelucia",
                                                                            | "order": "popularity"
                                                                            |}""".stripMargin)
      ValidQueriesJob.
        getValidSearchLogs(sc.parallelize(Seq(event)))
        .keyBy(searchLog => (searchLog.
        apiKey, searchLog.searchId)).collect().toSeq should be(
        Seq((("marcyn", "779759e2-76fb-4ffe-b477-7f7a59654884"), event)))
    }
  }

  val rawSearchLogs: Seq[SearchLog] = Seq(
    new SearchLogParser().from("""{
                                 |  "apiKey": "saraiva-v5",
                                 |  "pageSize": 45,
                                 |  "totalFound": 6,
                                 |  "userId": "anon-22053a00-ff72-11e4-8d6b-9d8522454095",
                                 |  "filters": null,
                                 |  "date": "2015-05-28T07:53:18.395904",
                                 |  "query": "Batman: Arkham PS4",
                                 |  "info": {
                                 |    "searchId": "04d72fde-0c2e-45c5-81c7-bbe9f7e53173",
                                 |    "resultsExpansion": false,
                                 |    "ip": "191.204.200.213",
                                 |    "personalizeResults": false,
                                 |    "realUserId": null,
                                 |    "queryTime": 60,
                                 |    "relatedQuery": null,
                                 |    "sesssion": "1432809034953-0.9085735487751663",
                                 |    "chaordic_testGroup": {
                                 |      "code": null,
                                 |      "testCode": null,
                                 |      "experiment": null,
                                 |      "group": null,
                                 |      "session": null
                                 |    },
                                 |    "hasSynonyms": false,
                                 |    "browser_family": "Mobile Safari",
                                 |    "forceOriginal": false
                                 |  },
                                 |  "feature": "standard",
                                 |  "page": 1,
                                 |  "products": [
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 0.6876893938196759,
                                 |        "score": 34011.934,
                                 |        "view_weight": 3.7396817901838646
                                 |      },
                                 |      "id": "3869739"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 1.5721017164460243,
                                 |        "score": 34011.387,
                                 |        "view_weight": 3.4158242195661432
                                 |      },
                                 |      "id": "5542616"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 0.3937902739603207,
                                 |        "score": 34009.984,
                                 |        "view_weight": 1.9540075826409946
                                 |      },
                                 |      "id": "7023006"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 5.262848,
                                 |        "score": -999964990,
                                 |        "view_weight": 15.120896000000009
                                 |      },
                                 |      "id": "8884789"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 10.92196403904512,
                                 |        "score": -1999964929.9,
                                 |        "view_weight": 46.16314351517692
                                 |      },
                                 |      "id": "8883499"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 0.05629062132595858,
                                 |        "score": -1999990979.9,
                                 |        "view_weight": 0.056149118175881196
                                 |      },
                                 |      "id": "4063170"
                                 |    }
                                 |  ],
                                 |  "order": "popularity"
                                 |}""".stripMargin),
    new SearchLogParser().from("""{
                                 |  "apiKey": "saraiva-v5",
                                 |  "pageSize": 45,
                                 |  "totalFound": 6,
                                 |  "userId": "anon-22053a00-ff72-11e4-8d6b-9d8522454095",
                                 |  "filters": null,
                                 |  "date": "2015-05-28T07:50:18.395904",
                                 |  "query": "Batman: Arkham PS4",
                                 |  "info": {
                                 |    "searchId": "04d72fde-0c2e-45c5-81c7-bbe9f7e53173",
                                 |    "resultsExpansion": false,
                                 |    "ip": "191.204.200.213",
                                 |    "personalizeResults": false,
                                 |    "realUserId": null,
                                 |    "queryTime": 60,
                                 |    "relatedQuery": null,
                                 |    "sesssion": "1432809034953-0.9085735487751663",
                                 |    "chaordic_testGroup": {
                                 |      "code": null,
                                 |      "testCode": null,
                                 |      "experiment": null,
                                 |      "group": null,
                                 |      "session": null
                                 |    },
                                 |    "hasSynonyms": false,
                                 |    "browser_family": "Mobile Safari",
                                 |    "forceOriginal": false
                                 |  },
                                 |  "feature": "standard",
                                 |  "page": 1,
                                 |  "products": [
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 0.6876893938196759,
                                 |        "score": 34011.934,
                                 |        "view_weight": 3.7396817901838646
                                 |      },
                                 |      "id": "3869739"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 1.5721017164460243,
                                 |        "score": 34011.387,
                                 |        "view_weight": 3.4158242195661432
                                 |      },
                                 |      "id": "5542616"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 0.3937902739603207,
                                 |        "score": 34009.984,
                                 |        "view_weight": 1.9540075826409946
                                 |      },
                                 |      "id": "7023006"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 5.262848,
                                 |        "score": -999964990,
                                 |        "view_weight": 15.120896000000009
                                 |      },
                                 |      "id": "8884789"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 10.92196403904512,
                                 |        "score": -1999964929.9,
                                 |        "view_weight": 46.16314351517692
                                 |      },
                                 |      "id": "8883499"
                                 |    },
                                 |    {
                                 |      "info": {
                                 |        "purchase_weight": 0.05629062132595858,
                                 |        "score": -1999990979.9,
                                 |        "view_weight": 0.056149118175881196
                                 |      },
                                 |      "id": "4063170"
                                 |    }
                                 |  ],
                                 |  "order": "popularity"
                                 |}""".stripMargin)
  )

  val rawSearchClickLogs: Seq[SearchClickLog] = Seq(
    new SearchClickLogParser().from("""{
                                      |  "apiKey": "saraiva-v5",
                                      |  "userId": "anon-372566b0-2062-11e5-97dc-5932d943836a",
                                      |  "paginationInfo": {
                                      |    "pageIndex": 0,
                                      |    "itemIndex": 0,
                                      |    "pageItems": 45
                                      |  },
                                      |  "anonymous": true,
                                      |  "date": "2015-07-02T00:00:01.115357",
                                      |  "query": "O lado sombrio dos buscadores de",
                                      |  "info": {
                                      |    "requestExpansion": false,
                                      |    "searchId": "aeb76914-2f04-4170-808f-2b22a9560dd4",
                                      |    "sesssion": null,
                                      |    "chaordic_testGroup": {
                                      |      "code": null,
                                      |      "testCode": null,
                                      |      "experiment": null,
                                      |      "group": null,
                                      |      "session": null
                                      |    },
                                      |    "url": "http://www.saraiva.com.br/o-lado-sombrio-dos-buscadores-da-luz-457344.html",
                                      |    "ip": "186.247.156.29",
                                      |    "realUserId": null,
                                      |    "browser_family": "Mobile Safari"
                                      |  },
                                      |  "feature": "autocomplete",
                                      |  "version": "V2",
                                      |  "products": [
                                      |    {
                                      |      "sku": null,
                                      |      "price": 32,
                                      |      "id": "457344"
                                      |    }
                                      |  ],
                                      |  "type": "clicklog",
                                      |  "page": "search",
                                      |  "interactionType": "PRODUCT_DETAILS"
                                      |}""".stripMargin)
  )

  "ValidQueriesJob" should "generate searches value" in {
    val events = getValidSearchLogs(sc.parallelize(rawSearchLogs))
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    val searches: RDD[((String, String), Long)] = countByKey(events)
    searches.collect()(0) should be(("saraiva-v5", "Batman: Arkham PS4"), 2)
  }

  it should "calculate sumOfResults" in {
    val events = getValidSearchLogs(sc.parallelize(rawSearchLogs))
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    // Sum the number of products returned by the all the searches by a given apikey and query.
    val sumOfResults: RDD[((String, String), Long)] = sumOfResultsByKey(events)
    sumOfResults.collect()(0) should be(("saraiva-v5", "Batman: Arkham PS4"), 12)
  }

  it should "get latest event by date" in {
    val events = getValidSearchLogs(sc.parallelize(rawSearchLogs))
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    // Get the latest by date.
    val latestSearchLogs: RDD[((String, String), SearchLog)] = getLatestSearchLogsByKey(events)
    latestSearchLogs.collect()(0) should be((("saraiva-v5", "Batman: Arkham PS4"), rawSearchLogs(0)))
  }

  it should "Generate ValidQueries" in {
    val events = getValidSearchLogs(sc.parallelize(rawSearchLogs))
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    val searches: RDD[((String, String), Long)] = countByKey(events)
    val latestSearchLogs: RDD[((String, String), SearchLog)] = getLatestSearchLogsByKey(events)
    val sumOfResults: RDD[((String, String), Long)] = sumOfResultsByKey(events)

    val validQueries = joinEventsAndGetValidQueries(sc.emptyRDD, searches, latestSearchLogs, sumOfResults)

    val expectedValidQueries = ValidQuery("saraiva-v5", "batman: arkham ps4", 2, 0, 0.0,
      DateTime.parse("2015-05-28T07:53:18.395Z"), 6, "standard", 12, 6)

    validQueries.collect()(0) should be(expectedValidQueries)
  }

  it should "calculate the BiggestValidQueries" in {
    val events = getValidSearchLogs(sc.parallelize(rawSearchLogs))
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    val expectedValidQueries = ValidQuery("saraiva-v5", "batman: arkham ps4", 2, 0, 0.0,
      DateTime.parse("2015-05-28T07:53:18.395Z"), 6, "standard", 12, 6)

    val validQueries: RDD[ValidQuery] = sc.parallelize(Seq(expectedValidQueries))

    val biggestValidQueries: RDD[((String, Seq[String]), ValidQuery)] = getValidQueriesWithMostSearches(validQueries)

    biggestValidQueries.collect()(0) should be(("saraiva-v5", Seq("batman", "arkham", "ps4")), expectedValidQueries)
  }

  it should "generate the final ValidQueries" in {
    val expectedValidQueries = ValidQuery("saraiva-v5", "batman: arkham ps4", 2, 0, 0.0,
      DateTime.parse("2015-05-28T07:53:18.395Z"), 6, "standard", 12, 6)

    val biggestValidQueries = sc.parallelize(Seq((("saraiva-v5", Seq("batman", "arkham", "ps4")), expectedValidQueries)))

    val finalValidQuery = generateFinalValidQueries(biggestValidQueries)

    val expectedFinalValidQuery = ValidQueries(expectedValidQueries.apiKey, Seq("batman", "arkham", "ps4"),
      expectedValidQueries.query, expectedValidQueries.searches, expectedValidQueries.clicks,
      expectedValidQueries.rawCtr, expectedValidQueries.latestSearchLog, expectedValidQueries.latestSearchLogResults,
      expectedValidQueries.averageResults, Seq(expectedValidQueries), active = true)

    finalValidQuery.collect()(0) should be(expectedFinalValidQuery)

  }

  it should "filter searchlogs from pingdom" in {
    val event: SearchLog =
      new SearchLogParser().from(s"""{
                                     | "info": {
                                     |   "searchId": "779759e2-76fb-4ffe-b477-7f7a59654884",
                                     |   "ip": "201.92.214.99",
                                     |   "personalizeResults": true,
                                     |   "forceOriginal": false,
                                     |   "browser_family": "Chrome",
                                     |   "originalResultListing": [
                                     |     "501509"
                                     |   ]
                                     | },
                                     | "apiKey": "marcyn",
                                     | "pageSize": 3,
                                     | "totalFound": 100,
                                     | "userId": "anon-8993c800-179f-11e4-877a-4dacbde6a7e4",
                                     | "feature": "standard",
                                     | "page": 1,
                                     | "products": [
                                     |   {"id": "501509",
                                     |    "info": {
                                     |      "purchase_weight": 5.419288057975395,
                                     |      "score": 1392916.5,
                                     |      "view_weight":12.797831187927807}
                                     |   },
                                     |   {"id": "501510",
                                     |    "info": {
                                     |      "purchase_weight": 5.419288057975395,
                                     |      "score": 1392916.5,
                                     |      "view_weight":12.797831187927807}
                                     |   },
                                     |   {"id": "501511",
                                     |    "info": {
                                     |      "purchase_weight": 5.419288057975395,
                                     |      "score": 1392916.5,
                                     |      "view_weight":12.797831187927807}
                                     |   }
                                     | ],
                                     | "filters": null,
                                     | "date": "${DateTime.now.toString}",
                                     | "query": "pingdom",
                                     | "order": "popularity"
                                     |}""".stripMargin)

    ValidQueriesJob.getValidSearchLogs(sc.parallelize(Seq(event))).isEmpty() should be(true)
  }

  it should "filter searchlogs with filters" in {
    val event: SearchLog =
      new SearchLogParser().from( s"""{
                                     | "info": {
                                     |   "searchId": "779759e2-76fb-4ffe-b477-7f7a59654884",
                                     |   "ip": "201.92.214.99",
                                     |   "personalizeResults": true,
                                     |   "forceOriginal": false,
                                     |   "browser_family": "Chrome",
                                     |   "originalResultListing": [
                                     |     "501509"
                                     |   ]
                                     | },
                                     | "apiKey": "marcyn",
                                     | "pageSize": 3,
                                     | "totalFound": 100,
                                     | "userId": "anon-8993c800-179f-11e4-877a-4dacbde6a7e4",
                                     | "feature": "standard",
                                     | "page": 1,
                                     | "products": [
                                     |   {"id": "501509",
                                     |    "info": {
                                     |      "purchase_weight": 5.419288057975395,
                                     |      "score": 1392916.5,
                                     |      "view_weight":12.797831187927807}
                                     |   },
                                     |   {"id": "501510",
                                     |    "info": {
                                     |      "purchase_weight": 5.419288057975395,
                                     |      "score": 1392916.5,
                                     |      "view_weight":12.797831187927807}
                                     |   },
                                     |   {"id": "501511",
                                     |    "info": {
                                     |      "purchase_weight": 5.419288057975395,
                                     |      "score": 1392916.5,
                                     |      "view_weight":12.797831187927807}
                                     |   }
                                     | ],
                                     | "filters": {"color": ["Bege" ]},
                                     | "date": "${DateTime.now.toString}",
                                     | "query": "pingdom",
                                     | "order": "popularity"
                                     |}""".stripMargin)

    ValidQueriesJob.getValidSearchLogs(sc.parallelize(Seq(event))).isEmpty() should be(true)
  }

  it should "filter clicks without searches" in {

    val searchLogs = getValidSearchLogs(sc.parallelize(rawSearchLogs))
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    val clickLogs = getValidClickLogs(sc.parallelize(rawSearchClickLogs))
      .keyBy(clickLog => (clickLog.apiKey, clickLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    filterClicksWithoutSearch(searchLogs, clickLogs).collect().toSeq should be(Seq.empty)
  }

  it should "keep clicks that has an associated search" in {
    val searchLogs = getValidSearchLogs(sc.parallelize(rawSearchLogs))
      .keyBy(searchLog => (searchLog.apiKey, searchLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    val patchedClicks = rawSearchClickLogs.map(_.copy(query = rawSearchLogs(0).query,info = Map("searchid" -> rawSearchLogs(0).searchId)))

    val clickLogs = getValidClickLogs(sc.parallelize(patchedClicks))
      .keyBy(clickLog => (clickLog.apiKey, clickLog.searchId))
      .map { case ((apiKey, searchId), log) => ((apiKey, log.query), log) }

    filterClicksWithoutSearch(searchLogs, clickLogs).collect().toSeq(0)._2 should be (patchedClicks(0))

  }

  it should "filter valid clicks" in {
    getValidClickLogs(sc.parallelize(rawSearchClickLogs)).collect()(0) should be(rawSearchClickLogs(0))
  }

  it should "process all the job and dont output one valid query without a click" in {
    val result = process(sc.parallelize(rawSearchLogs), sc.parallelize(rawSearchClickLogs)).collect()
    result(0).clicks should be(0)
  }

}
