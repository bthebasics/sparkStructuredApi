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
import org.apache.spark.sql.functions.udf

object sentimentAnalyser {
  def main(args: Array[String]): Unit = {

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

    //reviews.printSchema()
    val df2 = reviews.select("id","reviews_text") //.createTempView("AmazonData")
    val cleanupUdf = udf(onlyWords _)

    val detectSentiUDF = udf(guessSenti _ )

    import spark.implicits
    //df2.show()
    val df3 = df2.withColumn("CleanEd_Text", cleanupUdf(df2.col("reviews_text")) )
    val df4 = df3.withColumn("sentiMents", detectSentiUDF(df3.col("CleanEd_Text")))
    df4.show(false)
    val sent = "beautiful"

    val currn_senti = SentimentAnalysisUtils.detectSentiment(onlyWords(sent))
    println(currn_senti.toString)

    //df2.show()
/*
    val AFINN = spark.sparkContext.textFile("file:///C:/DataForCourses/sentiments/AFINN.txt").map(x=> x.split("\t")).map(x=>(x(0).toString,x(1).toInt))

    AFINN.take(4)g243
    //val senti =

    val extracted_rvw = spark.sql("select id,reviews_text from AmazonData").collect

    val sentiments  = extracted_rvw.map(tweetText => {
      val tweetWordsSentiment = tweetText(1).toString.split(" ").map(word => {
        var senti: Int = 0;
        if (AFINN.lookup(word.toLowerCase()).length > 0) {
          senti = AFINN.lookup(word.toLowerCase())(0)
        }
        senti
      })
      val tweetSentiment = tweetWordsSentiment.sum
   0 bvff bZAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaazXGVVVVVVVVVVVVVV   (tweetSentiment ,tweetText.toString)
    })

    val tweetsSentiRDD = spark.sparkContext.parallelize(sentiments.toList).sortBy(x => x._1, false);
    tweetsSentiRDD.foreach(println)
*/
/*    val x = new SentimentAnalyzer(txt)
    x.analyze()
    println(x.getPolarity)
*/

    println("***********************************************************")

  }

  def guessSenti (text:String) = {
    SentimentAnalysisUtils.detectSentiment(text).toString
  }

  def onlyWords(text: String) : String = {
    text.split(" ").filter(_.matches("^[a-zA-Z0-9 ]+$")).fold("")((a,b) => a + " " + b).trim.toLowerCase
  }


}
