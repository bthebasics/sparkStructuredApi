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
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3


object dstreamsExample {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

   // val spark = SparkSession.builder().appName("Sentiment Analyser").master("local[*]").enableHiveSupport().getOrCreate()

  //  import spark.implicits._
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(","))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
  }
