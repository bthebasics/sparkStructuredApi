package com.btb.spark.structuredApi.JsonRead
import com.databricks.spark.xml._
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

object xmlReader {
  def main(args: Array[String]): Unit = {

    println("hello XML")
    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()

   // val df = spark.read.xml("file:///C:/gitRepo/json-examples/src/accounts.xml")
   // val df = spark.read.format("com.databricks.spark.xml").load("file:///C:/gitRepo/json-examples/src/account.xml")


    val df = spark.read
    .format("com.databricks.spark.xml")
    .option("rootTag", "root")
    .option("rowTag", "user")
    .load("file:///C:/gitRepo/json-examples/src/account.xml")
    df.printSchema()

  }

}
