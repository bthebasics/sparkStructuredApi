package com.btb.spark.structuredApi.sparksql


import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

//case class FeedbackRow(manager_name: String, response_time: Double, satisfaction_level: Double)
case class StockData(
                      MarketDate:String,
                      Open:Double,
                      High:Double,
                      Low:Double,
                      Close:Double,
                      Adj_Close:Double,
                      Volume:Integer
)

object sparksqlDatasets {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    val carDF:DataFrame = spark.read
      .option("inferSchema","true")
      .option("sep","#")
      .option("mode", "DROPMALFORMED")  //DROPMALFORMED FAILFAST
      .option("header","true")
      .csv("file:///C:/DataForCourses/carInventory.csv")

    carDF.createGlobalTempView("carInventory")
    import spark.implicits._

/*
    val ds: Dataset[FeedbackRow] = carDF.as[FeedbackRow]
    val theSameDS = spark.read.parquet("example.parquet").as[FeedbackRow]

*/
    //TODO: Research - not clarity

    spark.stop()


  }// end of main
}

// reference :https://spark.apache.org/docs/2.2.0/api/java/org/apache/spark/sql/types/package-summary.html
// oreally spark definative guide - chapter 2 describing structfield and type