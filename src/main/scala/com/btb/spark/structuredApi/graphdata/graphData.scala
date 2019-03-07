package com.btb.spark.structuredApi.graphdata

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{expr, col}
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.apache.spark.storage.StorageLevel
import org.graphframes._


object graphData {
  def main(args: Array[String]): Unit = {
    println("  ***************** graphframes ***************** ")

    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val airlinedelay =   spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/airline_delay/airlineDealySmall.csv")

    airlinedelay.printSchema()
    airlinedelay.show()

  }

}
