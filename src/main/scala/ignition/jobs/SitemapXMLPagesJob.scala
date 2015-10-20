package ignition.jobs

import java.net.URLEncoder
import java.net.URLDecoder

import ignition.chaordic.JsonParser
import ignition.chaordic.pojo.{SearchClickLog, SearchLog, Product}
import ignition.chaordic.utils.Json
import ignition.core.utils.DateUtils._
import ignition.jobs.SitemapXMLJob._
import ignition.jobs.utils.SearchApi.SitemapConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime


object SitemapXMLJob {

  def encode(s: String): String = URLEncoder.encode(s, "UTF-8")
  def decode(s: String): String = {
    try {
      URLDecoder.decode(s, "UTF-8")
    } catch {
      case e: Exception => s
    }
  }

  def generateUrlXml(url: String, lastMod: DateTime, changeFreq: String, priority: Double): String = {
    val xml = <url>
      <loc>{url}</loc>
      <lastmod>{lastMod.toIsoString}</lastmod>
      <changefreq>{changeFreq}</changefreq>
      <priority>{priority}</priority>
    </url>

    xml.toString()
  }

  def generateUrlSetPerPartition(urlXMLs: RDD[String]): RDD[String] = {
    urlXMLs.mapPartitions { urls =>
      val header =
        """<?xml version="1.0" encoding="UTF-8"?>
          |<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        """.stripMargin

      val footer = "</urlset>"

      Iterator(header) ++ urls ++ Iterator(footer)
    }
  }

  def slugify(input: String): String = {
    import java.text.Normalizer
    Normalizer.normalize(input, Normalizer.Form.NFKD)
      .replaceAll("[^\\w\\s-]", "") // Remove all non-word, non-space or non-dash characters
      .replace('-', ' ')            // Replace dashes with spaces
      .trim                         // Trim leading/trailing whitespace (including what used to be leading/trailing dashes)
      .replaceAll("\\s+", "-")      // Replace whitespace (including newlines and repetitions) with single dashes
      .toLowerCase                  // Lowercase the final results
  }

  def slugifySpace(input: String): String = {
    // to keep the space and in the final replace all dashes (-) to space
    // to be used in query normalization
    slugify(
      decode(input).replace('+', ' ')
    ).replace('-', ' ')
  }

}


object SitemapXMLSearchJob {

  def generateSearchUrlXMLs(sc: SparkContext,
                            now: DateTime,
                            searchLogs: RDD[SearchLog],
                            clickLogs: RDD[SearchClickLog],
                            config: SitemapConfig): RDD[String] = {
    val rankedQueries = searchLogs
      .filter(p => p.feature == "standard" && p.products.nonEmpty)
      .map(p => (slugifySpace(p.query), 1))
      .reduceByKey(_ + _)

    val rankedQueriesByClicks = clickLogs
      .filter(p => p.feature == "search")
      .map(p => (slugifySpace(p.query), 1))
      .reduceByKey(_ + _)

    val combinedQueriesWithRanks: RDD[String] = sc.parallelize {
      rankedQueries.leftOuterJoin(rankedQueriesByClicks).map {
        case (query, (queryCount, optClickCount)) =>
          (query, optClickCount.getOrElse(0) * 1000 + queryCount)
      }.top(config.maxSearchItems)(Ordering.by(_._2))
    }.keys

    val queryStrings = combinedQueriesWithRanks.map {
      query =>
        val link = s"${config.normalizedHost}/?q=${encode(query)}"
        generateUrlXml(link, now, "daily", 0.5)
    }

    queryStrings
  }

}


object SitemapXMLPagesJob {
  // This returns a RDD where each key is a word and the value is how many times it appeared in the content of lines

  def generateDetailsKeySets(conf: SitemapConfig): List[Set[String]] = conf.details.subsets.toList

  def getDetails(product: Product, detailsKeySets: List[Set[String]]): List[Option[String]] =
    detailsKeySets.map {
      detail =>
        val mappedDetails: Map[String, List[String]] =
          product.details
            .filterKeys(k => detail.contains(k))
            .filterNot { case (k, v) => v.trim.isEmpty }
            .mapValues(s => List(s))

        if (mappedDetails.isEmpty) Option.empty
        else Option(encode(Json.toJsonString(mappedDetails)))
    }.distinct


  def generateLink(conf: SitemapConfig, p: Product, detailsKeySets: List[Set[String]]): Seq[String] = {
    val encodedDetails = if (conf.useDetails) getDetails(p, detailsKeySets) else List(Option.empty[String])

    p.categoryPaths.toList.flatMap { categories =>
      (0 until categories.size).flatMap { i =>
        val prefix = conf.normalizedHost + "/pages/"
        val basePath = prefix + categories.take(i + 1)
          .flatMap(_.name)
          .map(slugify)
          .mkString("/")

        encodedDetails.map {
          case None => basePath
          case Some(encodedDetail) => s"$basePath?f=$encodedDetail"
        }
      }
    }
  }

  def generateUrlXMLs(sc: SparkContext, _now: DateTime, products: RDD[Product], conf: SitemapConfig): RDD[String] = {
    val detailsKeySets = sc.broadcast(generateDetailsKeySets(conf))
    val now = sc.broadcast(_now)
    products
      .filter(p => p.status.map(_.toUpperCase) match {
        case Some("AVAILABLE") | Some("UNAVAILABLE") => true
        case _ => false
      })
      .flatMap { product =>
        generateLink(conf, product, detailsKeySets.value).map { link =>
          generateUrlXml(link, now.value, "daily", 1)
        }
      }.distinct()
  }

}
