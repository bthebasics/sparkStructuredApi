package com.btb.spark.structuredApi.sparksql

// $example on:programmatic_schema$
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.Row
// $example off:programmatic_schema$
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$
import org.apache.spark.sql.types._

object sparksqlDataframesJsonReader {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    val carDF = spark.read.format("csv").option("inferSchema","true").option("header","true").load("file:///C:/DataForCourses/cars04.csv")

    val jsonDF = spark.read
      .option("multiline","true")
      .json("file:///C:/DataForCourses/multiLine.json")

    jsonDF.printSchema()
    jsonDF.show()

    val twitter = spark.read
      .option("multiline","true")
      .json("file:///C:/DataForCourses/twitter.json")

    twitter.printSchema()

    //twitter.show(false)

    val foresq = spark.read
      .option("multiline","true")
      .json("file:///C:/DataForCourses/foresquare.json")

    foresq.printSchema()



  }// end of main
}
