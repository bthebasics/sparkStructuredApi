package com.btb.spark.structuredApi.integration

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

object s3Datasets {

  // ref : https://stackoverflow.com/questions/39605312/accessing-s3-from-spark-2-0
  //https://docs.databricks.com/spark/latest/data-sources/aws/amazon-s3.html#access-aws-s3-directly

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("fs.s3n.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAI3OBW2VPNXZGHVOQ")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val words = sc.textFile("s3n://btb-datasets/DatasetForCourse/ship_routes.csv")
    println(s" count = ${words.count()}")

    val stockprice =   spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "yyyy-mm-dd")
      .option("mode", "DROPMALFORMED")
      .csv("s3n://btb-datasets/DatasetForCourse/stockprice.csv")

    stockprice.show()

  }

}
