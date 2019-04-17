package com.btb.spark.structuredApi.sentimentAnalyser

import com.vader.sentiment.analyzer.SentimentAnalyzer
import org.apache.spark.sql.{Column, SparkSession}
//import org.apache.spark.sql
import org.apache.log4j.Logger
import org.apache.log4j.Level
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lower

object SentimentUsingDictionary {
  def main(args: Array[String]): Unit = {

// https://acadgild.com/blog/sentiment-analysis-on-tweets-with-apache-hive-using-afinn-dictionary

    val spark = SparkSession.builder().appName("Sentiment Analyser").master("local[*]").enableHiveSupport().getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val reviews =   spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      //      .option("timestampFormat", "MM/dd/yyyy")
      //      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/sentiments/1429_1.csv")

    val cleanupUdf = udf(cleanup _)

    reviews.printSchema()

    //val reviewsDF = reviews.select(reviews.col("reviews_text"), cleanupUdf(reviews.col("reviews_text")).alias("cleanup_text")).withColumn("Idcol", monotonicallyIncreasingId)
    val reviewsDF = reviews.select(reviews.col("reviews_text"), reviews.col("reviews_text").alias("cleanup_text")).withColumn("Idcol", monotonicallyIncreasingId)
    //reviewsDF.show(false)

    reviewsDF.createTempView("reviews_data")

    val split_words = spark.sql("select Idcol as id, split(lower(cleanup_text), ' ') as words from reviews_data")

    split_words.createTempView("split_words")
    val reviewCross = spark.sql("select id,word from split_words LATERAL VIEW explode(words) w as word")
    reviewCross.show()

    reviewCross.createTempView("reviewCross")

    val dictionary =  spark.read
      .option("inferSchema","true")
      .option("sep","\t")
      //.option("header","true")
      //      .option("timestampFormat", "MM/dd/yyyy")
      //      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/sentiments/AFINN.txt").withColumnRenamed("_c0", "word").withColumnRenamed("_c1","rating")

    dictionary.createTempView("dictionary")

    val word_join = spark.sql(" select a.id, a.word, b.rating as rating from reviewCross as a left join dictionary as b on a.word = b.word")
    word_join.show()
    word_join.createTempView("word_join")
    spark.sql("select id, avg(w.rating) as ratings from word_join as w group by id order by id").show()

    //word_join.show()

    //  dictionary.printSchema()
  //  dictionary.show()




  }
  def cleanup(text: String) : String = {
    text.split(" ").filter(_.matches("^[a-zA-Z0-9 ]+$")).fold("")((a,b) => a + " " + b).trim.toLowerCase
  }
}
