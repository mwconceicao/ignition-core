package ignition.jobs.utils

import org.apache.commons.lang.StringEscapeUtils

// TODO search for duplicated code and move it to a common library
object HtmlUtils {
  private def strip(str: String): String = str.replaceAll("\\<.*?>", "").replaceAll("&lt;.*?&gt;", "")

  def escapeHtml(str: String): String =
    if (str.isEmpty) "" else StringEscapeUtils.escapeHtml(StringEscapeUtils.unescapeHtml(str).trim)

  def stripHtml(str: String): String =
    if (str.isEmpty) "" else escapeHtml(strip(str))
}
