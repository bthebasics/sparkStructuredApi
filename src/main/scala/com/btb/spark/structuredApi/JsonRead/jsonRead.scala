package com.btb.spark.structuredApi.JsonRead


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


object jsonRead {
  def main(args: Array[String]): Unit = {

    println("hello world")
    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()


    val df = spark.read
      .option("multiline","true")
      .json("file:///C:/gitRepo/json-examples/src/youtube-search-results.json")

    df.printSchema()

    import spark.implicits._

    //df.select("kind", "regionCode", "items", "pageInfo.totalResults", "pageInfo.resultsPerPage").show()

    //df.select("kind", "regionCode", col("items"), "pageInfo.totalResults", "pageInfo.resultsPerPage").show()
    df.select(explode(df("items")).alias("items_flat")).printSchema()
    df.select(explode(df("items")).alias("items_flat")).select("items_flat.kind", "items_flat.id.kind", "items_flat.id.channelId").show()

  }

}
