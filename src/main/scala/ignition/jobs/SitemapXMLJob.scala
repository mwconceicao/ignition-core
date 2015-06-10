package ignition.jobs

import java.net.URLEncoder
import java.net.URLEncoder

import ignition.chaordic.pojo.Product
import ignition.chaordic.utils.Json
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.joda.time.DateTime

import ignition.core.utils.DateUtils._



object SitemapXMLJob {
  // This returns a RDD where each key is a word and the value is how many times it appeared in the content of lines

  case class Config(baseHost: String = "", detailsKeys: Set[String] = Set.empty)

  def slugify(input: String): String = {
    import java.text.Normalizer
    Normalizer.normalize(input, Normalizer.Form.NFKD)
      .replaceAll("[^\\w\\s-]", "") // Remove all non-word, non-space or non-dash characters
      .replace('-', ' ')            // Replace dashes with spaces
      .trim                         // Trim leading/trailing whitespace (including what used to be leading/trailing dashes)
      .replaceAll("\\s+", "-")      // Replace whitespace (including newlines and repetitions) with single dashes
      .toLowerCase                  // Lowercase the final results
  }

  def encode(s: String): String = URLEncoder.encode(s, "UTF-8")


  def generateDetailsKeySets(conf: Config): List[Set[String]] = conf.detailsKeys.subsets.toList

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


  def generateLink(p: Product, baseHost: String, detailsKeySets: List[Set[String]]): Seq[String] = {
    val encodedDetails = List(Option.empty[String])//getDetails(p, detailsKeySets)

    p.categoryPaths.toList.flatMap { categories =>
      (0 until categories.size).flatMap { i =>
        val basePath = baseHost + "/" + categories.take(i + 1)
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

  def generateUrlXml(url: String, lastMod: DateTime, changeFreq: String, priority: Double): String = {
    val xml = <url>
      <loc>{url}</loc>
      <lastmod>{lastMod.toIsoString}</lastmod>
      <changefreq>{changeFreq}</changefreq>
      <priority>{priority}</priority>
    </url>

    xml.toString()
  }

  def generateUrlXMLs(sc: SparkContext, _now: DateTime, products: RDD[Product], conf: Config): RDD[String] = {
    val detailsKeySets = sc.broadcast(generateDetailsKeySets(conf))
    val now = sc.broadcast(_now)
    products.filter(p => p.status.map(_.toUpperCase) match {
      case Some("AVAILABLE") | Some("UNAVAILABLE") => true
      case _ => false
    }).flatMap { product =>
      generateLink(product, conf.baseHost, detailsKeySets.value).map { link =>
        generateUrlXml(link, now.value, "hourly", 1)
      }
    }.distinct()
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


}
