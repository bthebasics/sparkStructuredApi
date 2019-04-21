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


object redditJson {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()

    /*val fields = List(StructField("archived", BooleanType(), true),
      StructField("author", StringType(), true),
      StructField("author_flair_css_class", StringType(), true),
      StructField("body", StringType(), true),
      StructField("controversiality", LongType(), true),
      StructField("created_utc", StringType(), true),
      StructField("distinguished", StringType(), true),
      StructField("downs", LongType(), true),
      StructField("edited", StringType(), true),
      StructField("gilded", LongType(), true),
      StructField("id", StringType(), true),
      StructField("link_id", StringType(), true),
      StructField("name", StringType(), true),
      StructField("parent_id", StringType(), true),
      StructField("retrieved_on", LongType(), true),
      StructField("score", LongType(), true),
      StructField("score_hidden", BooleanType(), true),
      StructField("subreddit", StringType(), true),
      StructField("subreddit_id", StringType(), true),
      StructField("ups", LongType(), true))
*/

    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("fs.s3n.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAI3OBW2VPNXZGHVOQ")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val rawDF = spark.read.json("s3n://reddit-comments/2015/RC_2015-05")

    rawDF.show()



  }

}
