package com.github.mobikr.sample

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import scala.collection.mutable
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.phrase_extractor.KoreanPhraseExtractor.KoreanPhrase
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer.KoreanToken
import com.twitter.penguin.korean.util.KoreanPos
/**
 * @author mobi
 *
 *
 */
class LDASample(spark: SparkSession){

  import spark.implicits._

  def run() {
    val userKeywordPairRDD = getUserKeywordPairRDD();

    // 유저 키워드 형태소 분석
    val userMorphemeKeywords = getUserMorphemeKeywords(userKeywordPairRDD);

    // 유저 키워드 데이터 저장
    userMorphemeKeywords.toDF("user", "morphemeKeywords").write.format("parquet").mode("Overwrite").save("/home/mobi/parks/t_user_morpheme_keywords")

    // 유저 키워드 목록에 유저 IDX 먹이기
    addUserIdxFromUserMorphemeKeywords();

    // 키워드 단어 빈도수 추출
    val termCounts = getTermCounts();

    // 모델 학습
    trainingLdaModel(termCounts);

    // 모델 로딩
    val loadModel = DistributedLDAModel.load(spark.sparkContext, "/home/mobi/parks/lda_model")

    val loadVoca = spark.sparkContext.textFile("/home/mobi/parks/lda_model_vocaArray").collect()

    // 토픽 출력
    val topicIndices = loadModel.describeTopics(maxTermsPerTopic = 10)
    var i = 0
    topicIndices.foreach { case (terms, termWeights) =>
      println(s"TOPIC: $i")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${loadVoca(term.toInt)}\t$weight")
      }
      i = i + 1
      println("----")
    }


  }

  /**
   * Get. 유저 키워드 페어 RDD
   * @return
   */
  def getUserKeywordPairRDD(): RDD[(String, String)] = {

    val filepaath = "/home/mobi/parks/user_keyword_sample.csv"
    val schema = StructType(Array(
      StructField("user", StringType, false),
      StructField("keyword", StringType, true)
    ))

    val userKeywordPair = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(schema)
      .load(filepaath)
      .map(r => (r.getString(0), r.getString(1))).rdd

    return userKeywordPair
  }

  /**
   * 유저 키워드 형태소 분석
   * @param userKeywordPair
   * @return
   */
  def getUserMorphemeKeywords(userKeywordPair:RDD[(String,String)]): RDD[(String,Seq[String])] = {
    val userMorphemeKeywords = userKeywordPair.mapPartitions(x => {
      x.map(y => {
        val normalized: CharSequence = TwitterKoreanProcessor.normalize(y._2)
        val tokens: Seq[KoreanToken] = TwitterKoreanProcessor.tokenize(normalized)
        val stemmed: Seq[KoreanToken] = TwitterKoreanProcessor.stem(tokens)
        val morphemeKeywords = stemmed.filter(x => x.pos == KoreanPos.Noun).map(_.text).filter(_.length > 1)
        (y._1, morphemeKeywords)
      })
    }).filter(_._2.size > 0)

    return userMorphemeKeywords
  }

  /**
   * 유저 키워드 목록에 유저 IDX 먹이기
   */
  def addUserIdxFromUserMorphemeKeywords(): Unit = {
    val t_user_morpheme_keywords = spark.read.load("/home/mobi/parks/t_user_morpheme_keywords")
    t_user_morpheme_keywords.map(r => {
      (r.getString(0), r.getSeq[String](1))
    }).rdd.zipWithIndex().map(x => {
      val idx = x._2
      val user = x._1._1
      val morphemeKeywords = x._1._2
      (idx, user, morphemeKeywords)
    }).toDF("idx", "user", "morphemeKeywords").write.format("parquet").mode("Overwrite").save("/home/mobi/parks/t_idx_user_morpheme_keywords")

  }

  /**
   * 키워드 빈도 추출
   */
  def getTermCounts() : Array[(String,Long)] = {
    val idx_user_morpheme_keywords_rdd =  spark.read.load("/home/mobi/parks/t_idx_user_morpheme_keywords").map(x =>{
      (x.getLong(0), x.getString(1), x.getSeq[String](2))
    })
    val term_counts = idx_user_morpheme_keywords_rdd.map {
      case (idx, user, morphemeKeywords) =>
        morphemeKeywords
    }.flatMap(_.map(_ -> 1L)).rdd.reduceByKey( _ + _ ).collect().sortBy(-_._2)

    return term_counts
  }

  /**
   * LDA 모델 학습
   * @param termCount
   */
  def trainingLdaModel(termCount: Array[(String,Long)]): Unit = {

    val numStopwords = 100
    val vocabArray = termCount.takeRight(termCount.size - numStopwords).map(_._1)
    val vocab = vocabArray.zipWithIndex.toMap
    val idx_user_morpheme_keywords_rdd =  spark.read.load("/home/mobi/parks/t_idx_user_morpheme_keywords").map(x =>{
      (x.getLong(0), x.getString(1), x.getSeq[String](2))
    })

    val documents =  idx_user_morpheme_keywords_rdd.map { case (id, user, morphemeKeywords) =>
      val counts = new mutable.HashMap[Int, Double]()
      morphemeKeywords.foreach(term => {
        if (vocab.contains(term)) {
          val idx = vocab(term)
          counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
        }
      })
      (id, Vectors.sparse(vocab.size, counts.toSeq))
    }.rdd

    documents.persist(StorageLevel.MEMORY_AND_DISK)

    val numTopics = 20
    val maxLDAIters = 50

    val lda = new LDA().setK(numTopics).setMaxIterations(maxLDAIters).setCheckpointInterval(10).setOptimizer("em")
    val ldaModel = lda.run(documents)

    ldaModel.save(spark.sparkContext, "/home/mobi/parks/lda_model")

    documents.unpersist()

    spark.sparkContext.parallelize(vocabArray).saveAsTextFile("/home/mobi/parks/lda_model_vocaArray")

  }
}
