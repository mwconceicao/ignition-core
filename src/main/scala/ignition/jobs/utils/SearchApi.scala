package ignition.jobs.utils

import java.io.InputStreamReader
import java.net.URLEncoder

import ignition.chaordic.utils.Json

import scala.util.Try
import scalaj.http.{Http, HttpOptions}

object SearchApi {
  import Json.formats

  private val httpOpts = List(HttpOptions.connTimeout(30000), HttpOptions.readTimeout(60000))

  val apiUrl = "https://search.chaordic.com.br/api/v1"
  val user = "dashboard-ignition"
  val password = "#d45h-z655as2AF78v"

  def getApiJson(path: String) = {
    Http(s"$apiUrl$path").auth(user, password).options(httpOpts) { inputStream =>
      Json.parseJson4s(new InputStreamReader(inputStream))
    }
  }

  def encode(s: String): String = URLEncoder.encode(s, "UTF-8")

  case class SitemapConfig(host: String, generatePages: Boolean, generateSearch: Boolean,
                    details: Set[String], useDetails: Boolean, maxSearchItems: Int,
                    numberPartFiles: Int) {
    def removeTrailingSlash(s: String) = s.replaceAll("/$", "")

    lazy val normalizedHost = removeTrailingSlash(host match {
      case host if host.startsWith("http://") || host.startsWith("https://") => host
      case host if host.startsWith("//") => s"http:$host"
      case host => s"http://$host"
    })
  }

  def getSitemapConfig(apiKey: String): SitemapConfig = {
    getApiJson(s"/confs/sitemap/${encode(apiKey)}/").extract[SitemapConfig]
  }

  def getClients(): Set[String] = {
    getApiJson(s"/clients/").extract[Set[String]]
  }

}
