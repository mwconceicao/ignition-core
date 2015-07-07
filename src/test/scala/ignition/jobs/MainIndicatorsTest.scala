package ignition.jobs

import ignition.chaordic.pojo.{SearchClickLog, SearchEvent, SearchLog}
import ignition.core.testsupport.spark.SharedSparkContext
import ignition.jobs.MainIndicators.MainIndicatorKey
import org.joda.time.DateTime
import org.scalacheck.Gen
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalautils.TolerantNumerics


class MainIndicatorsTest extends FlatSpec with SearchGenerators with ShouldMatchers with GeneratorDrivenPropertyChecks with SharedSparkContext {

  implicit override val generatorDrivenConfig = PropertyCheckConfig(workers = 4)
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.1)

  implicit object Joda {
    implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)
  }

  "getUniqueEventFromAutoComplete" should "filter only autocomplete events" in {
    forAll(Gen.listOfN(10, Gen.oneOf(searchLogGenerator(), searchClickLogGenerator()))) {
      (searchEvents: List[SearchEvent]) => {
        val seq = MainIndicators.getUniqueEventFromAutoComplete(sc.parallelize(searchEvents)).collect().toSeq
        seq.foreach(_.feature should be("autocomplete"))
      }
    }
  }

  "getUniqueEventFromAutoComplete" should "left only the latest element given a searchId" in {
    import Joda._
    forAll(Gen.listOfN(10,
      Gen.oneOf(searchLogGenerator(gInfo = Gen.const(Map("searchId" -> "myID"))),
        searchClickLogGenerator(gInfo = Gen.const(Map("searchId" -> "myID")))))) {
      (searchEvents: List[SearchEvent]) => {
        val seq = MainIndicators.getUniqueEventFromAutoComplete(sc.parallelize(searchEvents)).collect().toSeq
        if (seq.size > 0) seq.size should be(1)
        searchEvents.filter(_.feature == "autocomplete").foreach(_.date should be <= seq.head.date)
      }
    }
  }

  "getMetrics" should "count elements result should only have search or autocomplete with daily as searchId" in {
    forAll(Gen.listOfN(10, Gen.oneOf(searchLogGenerator(), searchClickLogGenerator()))) {
      (searchEvents: List[SearchEvent]) => {
        val seq = MainIndicators.getMetrics(sc.parallelize(searchEvents)).collect().toSeq
        seq.map(_._1.feature).toSet.diff(Set("autocomplete", "search")) should be(Set.empty)
        seq.map(_._1.searchId).toSet should be(Set("daily"))
        seq.foreach(_._2 should be >= 1)
      }
    }
  }


  /**
   * This test may fail with small probability.
   */
  "getMetrics" should "count elements with the same MainIndicatorKey" in {
    val freqs = Gen.frequency(
      (4, SearchLog("apikey1", DateTime.now, "search", 1, List.empty, None, 1, None, "", "", None, Map("searchId" -> "schorstein"))),
      (4, SearchClickLog("apikey1", "", DateTime.now, "", "search", Map("searchId" -> "schorstein"))),
      (6, SearchLog("apikey2", DateTime.now, "search", 1, List.empty, None, 1, None, "", "", None, Map("searchId" -> "bierland"))),
      (6, SearchClickLog("apikey2", "", DateTime.now, "", "search", Map("searchId" -> "bierland")))
    )
    val seqSize = 1000.0
    forAll(Gen.listOfN(seqSize.toInt, freqs)) {
      (searchEvents: List[SearchEvent]) => {
        val seq = MainIndicators.getMetrics(sc.parallelize(searchEvents)).collect().toSeq
        seq.foreach {
          case (MainIndicatorKey("apikey1", _, _, _), value: Int) =>
            value / seqSize === 0.4
          case (MainIndicatorKey("apikey2", _, _, _), value: Int) =>
            value / seqSize === 0.6
        }
      }
    }
  }

  "getUniqueMetrics" should "remove redirects and aggregate" in {
    val searchEvents = List(
      SearchLog("apikey", DateTime.now, "redirect", 1, List.empty, None, 1, None, "", "", None, Map("searchId" -> "schorstein")),
      SearchLog("apikey", DateTime.now, "search", 1, List.empty, None, 1, None, "", "", None, Map("searchId" -> "schorstein")),
      SearchLog("apikey", DateTime.now, "search", 1, List.empty, None, 1, None, "", "", None, Map("searchId" -> "schorstein")),
      SearchLog("apikey", DateTime.now, "search", 1, List.empty, None, 1, None, "", "", None, Map("searchId" -> "schorstein2"))
    )
    val seq = MainIndicators.getUniqueMetrics(sc.parallelize(searchEvents)).collect().toSeq
    seq.size should be(1)
    seq.head._2 should be(2)
  }

  "getValidSearchLogs" should "filter invalid Queries and Invalid Ips yielding only searches with page == 1" in {

    val gIp: Gen[String] = Gen.oneOf(Gen.alphaStr, Gen.const("107.170.51.250"))
    val gInfo = for {
      str <- Gen.alphaStr
      ip <- gIp
    } yield Map("searchId" -> str, "ip" -> ip)

    forAll(Gen.listOfN(10, searchLogGenerator(gQuery = Gen.oneOf(Gen.alphaStr, Gen.const("pingdom")), gInfo = gInfo))) {
      searchEvents: List[SearchLog] => {
        MainIndicators.getValidSearchLogs(sc.parallelize(searchEvents)).foreach {
          searchEvent =>
            searchEvent.page should be(1)
            searchEvent.query should not be("pingdom")
            searchEvent.ip should not be "107.170.51.250"
        }
      }
    }
  }

  "getValidClickLogs" should "filter invalid Ips and Invalid queries" in {
    val gIp: Gen[String] = Gen.oneOf(Gen.alphaStr, Gen.const("107.170.51.250"))
    val gInfo = for {
      str <- Gen.alphaStr
      ip <- gIp
    } yield Map("searchId" -> str, "ip" -> ip)

    forAll(Gen.listOfN(10, searchClickLogGenerator(gQuery = Gen.oneOf(Gen.alphaStr, Gen.const("pingdom")), gInfo = gInfo))) {
      searchEvents: List[SearchClickLog] => {
        MainIndicators.getValidClickLogs(sc.parallelize(searchEvents)).foreach {
          searchEvent =>
            searchEvent.query should not be("pingdom")
            searchEvent.ip should not be "107.170.51.250"
        }
      }
    }
  }

}
