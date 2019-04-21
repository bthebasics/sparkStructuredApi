package com.btb.spark.structuredApi.operations

import org.apache.avro.generic.GenericData
//import org.apache.spark.sql.sources.v2.reader.
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{expr, col}
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{col, udf}
import org.graphframes.GraphFrame
import org.apache.spark.storage.StorageLevel
import org.graphframes._
import com.twosigma.flint.timeseries.TimeSeriesRDD
import scala.concurrent.duration._
import com.twosigma.flint.timeseries.CSV
import com.twosigma.flint.timeseries.summarize
import com.twosigma.flint.FlintConf
import com.twosigma.flint.timeseries

object ChicagoChrimeAnalysis {

  def main(args: Array[String]): Unit = {

    // Ref : https://datascienceplus.com/spark-dataframes-exploring-chicago-crimes/
    // begining apache spark 2.0

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("StockData Processing").master("local[*]").enableHiveSupport().getOrCreate()

    val crimes_schema = StructType(Array(StructField("ID", StringType, true),
      StructField("CaseNumber", StringType, true),
      StructField("Date", TimestampType, true ),
      StructField("Block", StringType, true),
      StructField("IUCR", StringType, true),
      StructField("PrimaryType", StringType, true  ),
      StructField("Description", StringType, true ),
      StructField("LocationDescription", StringType, true ),
      StructField("Arrest", BooleanType, true),
      StructField("Domestic", BooleanType, true),
      StructField("Beat", IntegerType, true),
      StructField("District", StringType, true),
      StructField("Ward", StringType, true),
      StructField("CommunityArea", StringType, true),
      StructField("FBICode", StringType, true ),
      StructField("XCoordinate", DoubleType, true),
      StructField("YCoordinate", DoubleType, true ),
      StructField("Year", IntegerType, true),
      StructField("UpdatedOn", TimestampType, true ),
      StructField("Latitude", DoubleType, true),
      StructField("Longitude", DoubleType, true),
      StructField("Location", StringType, true )
    ))

    val chicagoCrime =   spark.read
      .schema(crimes_schema)
      //.option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "mm/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/Crimes2018.csv")

    chicagoCrime.printSchema()

    //chicagoCrime.show()
    // todo: resolve how to print below
    //println(chicagoCrime.dtypes)
    //chicagoCrime.columns

    println("count = ",chicagoCrime.count())

    chicagoCrime.sample(false, 0.003).show(3)
    chicagoCrime.sample(true, 0.003, 1000).show(10)


    chicagoCrime.na.drop().show()
    chicagoCrime.na.drop("any").show()
    chicagoCrime.na.drop("all").show()

    chicagoCrime.na.drop(Array("XCoordinate", "YCoordinate")).show()

    chicagoCrime.describe("PrimaryType").show()

  //  val chicagoCrimeDF = chicagoCrime.withColumn("Key", monotonically_increasing_id())
  //  chicagoCrimeDF.show()

    // ref : https://medium.com/@mrpowers/spark-user-defined-functions-udfs-6c849e39443b


  //  chicagoCrimeDF.select(lowerRemoveAllWhitespace(col("CaseNumber"))).alias("trim_cases").show()

  }

  def lowerRemoveAllWhitespace(s: String): String = {
    s.toLowerCase().replaceAll("\\s", "")
  }

}


//crimes.select(["Latitude","Longitude","Year","XCoordinate","YCoordinate"]).describe().show()