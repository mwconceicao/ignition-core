package ignition.jobs.utils.text

trait ChaordicStemmer extends NormalizeSupport {

  val singularizationRules = Map("ns" -> "m", "ões"-> "ão", "oes"-> "ao",  "ães"-> "ão", "aes"-> "ao", "ais"-> "al",
    "éis"-> "el", "eis"-> "el", "óis"-> "ol", "ois"-> "ol", "is"-> "il", "les"-> "l", "res"-> "r", "s"-> "")

  val pluralExceptions = Set("cais", "mais", "lápis", "cais", "mais", "crúcis", "biquínis", "pois", "depois", "dois",
    "leis", "árvores", "aliás", "pires", "lápis", "cais", "mais", "mas", "menos", "férias", "fezes", "pêsames", "tenis",
    "crúcis", "gás", "atrás", "moisés", "através", "convés", "ês", "país", "após", "ambas", "ambos","messias", "depois")

  def asciiFold(tokens: Seq[String]) =
    tokens.map(token => normalize(token))

  def stemTokenByRule(token: String, suffix: String): String =
    token.endsWith(suffix) match {
      case true => token.stripSuffix(suffix).concat(singularizationRules(suffix))
      case false => token
    }

  def stemToken(token: String, singularizationRules: Seq[String]): String = {
    if ((pluralExceptions contains token) || !token.endsWith("s") || singularizationRules.isEmpty) token
    else stemToken(stemTokenByRule(token, singularizationRules.head), singularizationRules.tail)
  }

  def stem(tokens: Seq[String]) =
    tokens.map(token => stemToken(token, singularizationRules.keys.toSeq.sortBy(rule => -rule.length)))

  def fullChain(sentence: String) =
    stem(asciiFold(Tokenizer.tokenize(sentence)))

  def preProcessTerms(terms: String) =
    fullChain(terms).mkString(" ")

}

object ChaordicStemmer extends ChaordicStemmer
