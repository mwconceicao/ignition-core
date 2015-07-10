package ignition.jobs.utils.text

import org.scalatest.{FlatSpec, Matchers}


class NormalizeSupportSpec extends FlatSpec with Matchers {

  "NormalizeSupport" should "correctly normalize non -ASCII characters" in {
    ChaordicStemmer.normalize("ÀÁÂÃĀĂȦÄẢÅǍȀȂĄẠḀẦẤàáâä") should be ("aaaaaaaaaaaaaaaaaaaaaa")
    ChaordicStemmer.normalize("ÉÊẼĒĔËȆȄȨĖèéêẽēȅë") should be ("eeeeeeeeeeeeeeeee")
    ChaordicStemmer.normalize("ÌÍÏïØøÒÖÔöÜüŇñÇçß") should be ("iiiioooooouunnccss")
  }

  it should "normalize 's to nothing" in {
    ChaordicStemmer.normalize("aa'sbba") should be ("aabba")
  }

  it should "normalize & for -" in {
    ChaordicStemmer.normalize("aa & bb") should be ("aa-bb")
    ChaordicStemmer.normalize("aa&& & &&& bb") should be ("aa-bb")
    }

  it should "normalize brackets to -" in {
    ChaordicStemmer.normalize("aa(bb)cc") should be ("aa-bb-cc")
    ChaordicStemmer.normalize("aa((((bb)))cc") should be ("aa-bb-cc")
  }

  it should "normalize multiples of '-' to a single '-'" in {
    ChaordicStemmer.normalize("a----a--b-b-------a") should be ("a-a-b-b-a")
  }

  it should "normalize to lowercase" in {
    ChaordicStemmer.normalize("AAbAbbB") should be ("aababbb")
  }

  it should "normalize a string with several diacritical marks" in {
    ChaordicStemmer.normalize("a'sa((%%$ & b___--BB a") should be ("aa-b-bb-a")
  }

}

