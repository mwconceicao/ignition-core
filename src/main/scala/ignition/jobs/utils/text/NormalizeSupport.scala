package ignition.jobs.utils.text

/**
 * Kindly borrowed from gist: https://gist.github.com/agemooij/15a0eaebc2c1ddd5ddf4
 */
trait NormalizeSupport {
  import java.text.Normalizer.{normalize => jnormalize, _}

  def normalize(in: String): String = {
    val cleaned = in.trim.toLowerCase
    val normalized = jnormalize(cleaned, Form.NFD)
      .replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{IsM}\\p{IsLm}\\p{IsSk}]+", "")

    normalized.replaceAll("'s", "")
      .replaceAll("ß", "ss")
      .replaceAll("ø", "o")
      .replaceAll("[^a-zA-Z0-9-]+", "-")
      .replaceAll("-+", "-")
      .stripSuffix("-")
  }
}
