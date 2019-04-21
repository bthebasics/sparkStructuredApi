package com.btb.spark.structuredApi.structuredStreaming

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

object streamingExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Sentiment Analyser").master("local[3]").config("spark.sql.shuffle.partitions","2").getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)
      .load()

    val words = lines.as[String].flatMap(_.split(","))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()


    query.awaitTermination()


  }

}
