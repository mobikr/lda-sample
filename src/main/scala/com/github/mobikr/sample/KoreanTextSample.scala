package com.github.mobikr.sample

import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.phrase_extractor.KoreanPhraseExtractor.KoreanPhrase
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer.KoreanToken
import com.twitter.penguin.korean.util.KoreanPos

/**
 * 한글 형태소 분석기
 * @see https://github.com/twitter/twitter-korean-text
 * @author mobi
 */
object KoreanTextSample {

  def run(text:String = "한국어를 처리하는 예시입니닼ㅋㅋㅋㅋㅋ #한국어" ):Seq[String] = {

    // Normalize
    val normalized: CharSequence = TwitterKoreanProcessor.normalize(text)
    println("== Normalize == \n")
    println(normalized)
    println("\n")

    // Tokenize
    val tokens: Seq[KoreanToken] = TwitterKoreanProcessor.tokenize(normalized)
    println("== Tokenize == \n")
    println(tokens)
    println("\n")

    // Stemming
    val stemmed: Seq[KoreanToken] = TwitterKoreanProcessor.stem(tokens)
    println("== Stemming == \n")
    println(stemmed)
    println("\n")

    // Phrase extraction
    val phrases: Seq[KoreanPhrase] = TwitterKoreanProcessor.extractPhrases(tokens, filterSpam = true, enableHashtags = true)
    println("== Phrase extraction == \n")
    println(phrases)
    println("\n")

    tokens.filter(x => x.pos == KoreanPos.Noun).map(_.text)
  }

}
