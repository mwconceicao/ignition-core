package ignition.jobs


import ignition.chaordic.pojo.Parsers.SearchLogParser
import ignition.chaordic.pojo.SearchLog
import ignition.core.testsupport.spark.SharedSparkContext
import ignition.core.utils.BetterTrace
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, ShouldMatchers}


class ValidQueriesJobSpec extends FlatSpec with ShouldMatchers with SharedSparkContext with BetterTrace {

  "parseEvents" should "properly parse a searchLog" in {
    withBetterTrace {
      val event: SearchLog =
        new SearchLogParser().from(s"""{
                  |  "info": {
                  |    "searchId": "779759e2-76fb-4ffe-b477-7f7a59654884",
                  |    "ip": "201.92.214.99",
                  |    "personalizeResults": true,
                  |    "forceOriginal": false,
                  |    "browser_family": "Chrome",
                  |    "originalResultListing": [
                  |      "501509"
                  |    ]
                  |  },
                  |  "apiKey": "marcyn",
                  |  "pageSize": 3,
                  |  "totalFound": 100,
                  |  "userId": "anon-8993c800-179f-11e4-877a-4dacbde6a7e4",
                  |  "feature": "standard",
                  |  "page": 1,
                  |  "products": [
                  |    {"id": "501509",
                  |     "info": {
                  |       "purchase_weight": 5.419288057975395,
                  |       "score": 1392916.5,
                  |       "view_weight":12.797831187927807}
                  |    },
                  |    {"id": "501510",
                  |     "info": {
                  |       "purchase_weight": 5.419288057975395,
                  |       "score": 1392916.5,
                  |       "view_weight":12.797831187927807}
                  |    },
                  |    {"id": "501511",
                  |     "info": {
                  |       "purchase_weight": 5.419288057975395,
                  |       "score": 1392916.5,
                  |       "view_weight":12.797831187927807}
                  |    }
                  |  ],
                  |  "filters": null,
                  |  "date": "${DateTime.now.toString}",
                  |  "query": "almofada de pelucia",
                  |  "order": "popularity"
                  |}""".stripMargin)

      ValidQueriesJob.
        getValidSearchLogs(sc.parallelize(Seq(event)))
        .keyBy(searchLog => (searchLog.
        apiKey, searchLog.searchId)).collect().toSeq should be (
        Seq((("marcyn", "779759e2-76fb-4ffe-b477-7f7a59654884"), event)))
    }
  }

  it should "filter searchlogs from pingdom" in {
    val event: SearchLog =
      new SearchLogParser().from(s"""{
        |  "info": {
        |    "searchId": "779759e2-76fb-4ffe-b477-7f7a59654884",
        |    "ip": "201.92.214.99",
        |    "personalizeResults": true,
        |    "forceOriginal": false,
        |    "browser_family": "Chrome",
        |    "originalResultListing": [
        |      "501509"
        |    ]
        |  },
        |  "apiKey": "marcyn",
        |  "pageSize": 3,
        |  "totalFound": 100,
        |  "userId": "anon-8993c800-179f-11e4-877a-4dacbde6a7e4",
        |  "feature": "standard",
        |  "page": 1,
        |  "products": [
        |    {"id": "501509",
        |     "info": {
        |       "purchase_weight": 5.419288057975395,
        |       "score": 1392916.5,
        |       "view_weight":12.797831187927807}
        |    },
        |    {"id": "501510",
        |     "info": {
        |       "purchase_weight": 5.419288057975395,
        |       "score": 1392916.5,
        |       "view_weight":12.797831187927807}
        |    },
        |    {"id": "501511",
        |     "info": {
        |       "purchase_weight": 5.419288057975395,
        |       "score": 1392916.5,
        |       "view_weight":12.797831187927807}
        |    }
        |  ],
        |  "filters": null,
        |  "date": "${DateTime.now.toString}",
        |  "query": "pingdom",
        |  "order": "popularity"
        |}""".stripMargin)

      ValidQueriesJob.getValidSearchLogs(sc.parallelize(Seq(event))).isEmpty() should be (true)
  }

  it should "filter searchlogs with filters" in {
    val event: SearchLog =
      new SearchLogParser().from(s"""{
        |  "info": {
        |    "searchId": "779759e2-76fb-4ffe-b477-7f7a59654884",
        |    "ip": "201.92.214.99",
        |    "personalizeResults": true,
        |    "forceOriginal": false,
        |    "browser_family": "Chrome",
        |    "originalResultListing": [
        |      "501509"
        |    ]
        |  },
        |  "apiKey": "marcyn",
        |  "pageSize": 3,
        |  "totalFound": 100,
        |  "userId": "anon-8993c800-179f-11e4-877a-4dacbde6a7e4",
        |  "feature": "standard",
        |  "page": 1,
        |  "products": [
        |    {"id": "501509",
        |     "info": {
        |       "purchase_weight": 5.419288057975395,
        |       "score": 1392916.5,
        |       "view_weight":12.797831187927807}
        |    },
        |    {"id": "501510",
        |     "info": {
        |       "purchase_weight": 5.419288057975395,
        |       "score": 1392916.5,
        |       "view_weight":12.797831187927807}
        |    },
        |    {"id": "501511",
        |     "info": {
        |       "purchase_weight": 5.419288057975395,
        |       "score": 1392916.5,
        |       "view_weight":12.797831187927807}
        |    }
        |  ],
        |  "filters": {"color": ["Bege" ]},
        |  "date": "${DateTime.now.toString}",
        |  "query": "pingdom",
        |  "order": "popularity"
        |}""".stripMargin)

    ValidQueriesJob.getValidSearchLogs(sc.parallelize(Seq(event))).isEmpty() should be (true)
  }
}
