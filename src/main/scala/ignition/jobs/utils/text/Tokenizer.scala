package ignition.jobs.utils.text

import scala.annotation.tailrec


trait Tokenizer {
  val stopwords = Set("a", "ainda", "alem", "ambas", "ambos", "antes", "ao", "aonde", "aos", "apos", "aquele",
    "aqueles", "as", "assim", "com", "como", "contra", "contudo", "cuja", "cujas", "cujo", "cujos", "da", "das", "de",
    "dela", "dele", "deles", "demais", "depois", "desde", "desta", "deste", "dispoe", "dispoem", "diversa", "diversas",
    "diversos", "do", "dos", "durante", "e", "ela", "elas", "ele", "eles", "em", "entao", "entre", "essa", "essas",
    "esse", "esses", "esta", "estas", "este", "estes", "ha", "isso", "isto", "logo", "mais", "mas", "mediante", "menos",
    "mesma", "mesmas", "mesmo", "mesmos", "na", "nas", "nao", "nas", "nem", "nesse", "neste", "nos", "o", "os", "ou",
    "outra", "outras", "outro", "outros", "pelas", "pelas", "pelo", "pelos", "perante", "pois", "por", "porque",
    "portanto", "proprio", "propios", "quais", "qual", "qualquer", "quando", "quanto", "que", "quem", "quer", "se",
    "seja", "sem", "sendo", "seu", "seus", "sob", "sobre", "sua", "suas", "tal", "tambem", "teu", "teus", "toda",
    "todos", "tua", "tuas", "tudo", "um", "uma", "umas", "uns")

  val delimiters = Seq("\\ ", "\\-", "\\.", "\\,", "\\/", "\\_", "\\]", "\\[", "\\)", "\\(", "\\}", "\\{", "\\~", "\\'")

  def filterEmptyTokens(tokens: Seq[String]) =
    tokens.map(token => token.trim).filterNot(token => token.isEmpty)

  def splitBy(tokens: Seq[String], delimiter: String) =
    tokens.map(token => token.split(delimiter)).flatten

  def filterStopWords(tokens: Seq[String]) =
    tokens.filterNot(token => stopwords contains token)

  def tokenize(query: String) =
    filterStopWords(filterEmptyTokens(splitByDelimiters(List(query), delimiters)))

  @tailrec
  final def splitByDelimiters(tokens: Seq[String], delimiters: Seq[String]): Seq[String] = {
    if (delimiters == Nil) tokens
    else splitByDelimiters(splitBy(tokens, delimiters.head), delimiters.tail)
  }
}

object Tokenizer extends Tokenizer
