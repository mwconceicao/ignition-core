package ignition.jobs.utils.text

import org.scalatest.{FlatSpec, Matchers}

class TokenizerSpec extends FlatSpec with Matchers {

  "tokenize" should "split tokens with spaces" in {
    Tokenizer.tokenize("   Oi    tchau   ") should be (Seq("Oi", "tchau"))
  }

  it should "split tokens with hyphens and remove stopwords" in  {
    Tokenizer.tokenize("porta-retrato de lata") should be (Seq("porta", "retrato", "lata"))
  }

  it should "split words by comma and remove stopwords" in {
    Tokenizer.tokenize("a colecionava, livros") should be (Seq("colecionava", "livros"))
  }

  it should "split words by slash" in {
    Tokenizer.tokenize("token1/token2") should be (Seq("token1", "token2"))
  }

  it should "split words by underline" in {
    Tokenizer.tokenize("token1_token2") should be (Seq("token1", "token2"))
  }

  it should "split words ending in bracket" in {
    Tokenizer.tokenize("my query]") should be (Seq("my", "query"))
  }

  it should "split words ending with reverse bracket" in {
    Tokenizer.tokenize("my query[") should be (Seq("my", "query"))
  }

  it should "split words ending in pontucation marks" in {
    Tokenizer.tokenize("my] query[ very) crazy( never} happens{ anyway~ man'") should be (Seq("my", "query", "very",
      "crazy", "never", "happens", "anyway", "man"))
  }

  it should "remove stop words" in {
    Tokenizer.tokenize("casa de barro") should be (Seq("casa", "barro"))
  }
}