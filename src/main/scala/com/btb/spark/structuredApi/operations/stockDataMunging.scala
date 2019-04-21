package com.btb.spark.structuredApi.operations

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{expr, col}
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.apache.spark.storage.StorageLevel


object stockDataMunging {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("StockData Processing").master("local[*]").enableHiveSupport().getOrCreate()

    //ref : https://www.kaggle.com/szrlee/stock-time-series-20050101-to-20171231/version/3#AABA_2006-01-01_to_2018-01-01.csv
    //TODO :https://github.com/aurosark/DataMunging/blob/master/Dealing%20with%20variable%20length%20records.scala
  /*  val aaba_rdd = spark.sparkContext.textFile("file:///C:/DataForCourses/StockTimeseries/AABA_2006-01-01_to_2018-01-01.csv")
    val header = aaba_rdd.first()
    val aapl_rdd = spark.sparkContext.textFile("file:///C:/DataForCourses/StockTimeseries/AAPL_2006-01-01_to_2018-01-01.csv")
    val amzn_rdd = spark.sparkContext.textFile("file:///C:/DataForCourses/StockTimeseries/AMZN_2006-01-01_to_2018-01-01.csv")
    val inputRDD = aaba_rdd.union(aapl_rdd).union(amzn_rdd)


    val inputRDDwithourHeader = inputRDD.filter(row => row != header)

    println(inputRDDwithourHeader.count())
    println(inputRDD.count())

    import spark.implicits._
    //inputRDD.take(5).foreach(println)

    val inputDF = inputRDDwithourHeader.toDF()
    val inputDropDupDF = inputDF.dropDuplicates()
    inputDropDupDF.show()

    //println("count = ",inputRDD.count())
    */


    val stockDF =   spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "yyyy-mm-dd")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/StockTimeseries")

    println(stockDF.count())

    //stockDF.where("Name == 'AMZN'").show()

    stockDF.select("Open", "High", "Low", "Name").describe().show()
    stockDF.printSchema()
    val df = stockDF.withColumn("yrmonth", date_format(col("Date"),"YYYYMM") ).withColumn("keyID", monotonically_increasing_id())
    df.groupBy("Name").pivot("yrmonth")
      .agg(round(avg("Volume"),2))
      .show()


  }

}
