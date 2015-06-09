package ignition.jobs

import java.net.URLEncoder
import java.net.URLEncoder

import ignition.chaordic.pojo.Product
import ignition.chaordic.utils.Json
import org.apache.spark.rdd.RDD


object SitemapJob {
  // This returns a RDD where each key is a word and the value is how many times it appeared in the content of lines

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


  val detailsKeySets: List[Set[String]] = Set("ratings", "publisher", "brand", "ano", "produtoDigital").subsets.toList

  def getDetails(product: Product): List[Option[String]] =
    detailsKeySets.map {
      detail =>
        val mappedDetails: Map[String, List[String]] =
          product.details
            .filterKeys(k => detail.contains(k))
            .filterNot(_._2.trim.isEmpty)
            .mapValues(s => List(s))

        if (mappedDetails.isEmpty) Option.empty
        else Option(encode(Json.toJsonString(mappedDetails)))
    }.distinct


  def generateLink(p: Product): Seq[String] = {
    val encodedDetails = getDetails(p)

    val categories = p.categoryPaths(0)
    (0 until categories.size).flatMap { i =>
      val basePath = categories.take(i+1)
        .map(c => slugify(c.id))
        .mkString("/")

      encodedDetails.map {
        case None => basePath
        case Some(encodedDetail) => s"$basePath?f=$encodedDetail"
      }
    }
  }
}
