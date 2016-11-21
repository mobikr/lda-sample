package com.github.mobikr.sample

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * @author mobi
 */
object App {

  def main(args: Array[String]): Unit = {
    // 형태소 분석 셈플 실행
    KoreanTextSample.run()
    val spark = SparkSession.builder().getOrCreate();
    val ldaSample = new LDASample(spark)
    ldaSample.run();
  }

}
