package ignition.jobs.utils.text

import org.scalatest.{FlatSpec, Matchers}


class ChaordicStemmerSpec extends FlatSpec with Matchers {

  "stemmer" should "stem simple plural words" in {
    ChaordicStemmer.stem(Seq("bicicletas")) should be (Seq("bicicleta"))
    ChaordicStemmer.stem(Seq("carros")) should be (Seq("carro"))
    ChaordicStemmer.stem(Seq("bicicletas", "carros")) should be (Seq("bicicleta", "carro"))
  }

  it should "stem words with ães" in {
    ChaordicStemmer.stem(Seq("cães", "pães")) should be (Seq("cão", "pão"))
  }

  it should "stem plural words and not stem exception" in {
    ChaordicStemmer.stem(Seq("carnais", "mais")) should be (Seq("carnal", "mais"))
  }

  it should "stem simple plural words without special characters" in {
    ChaordicStemmer.stem(Seq("caes", "paes")) should be (Seq("cao", "pao"))
  }

  "Pre Process Tokens" should "preprocess and return analyzed content" in {
    ChaordicStemmer.preProcessTerms("estes são os meus tokens processados tenis!") should be ("sao meu tokem processado tenis")
  }

}
