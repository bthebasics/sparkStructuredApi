package com.btb.spark.structuredApi.timeSeries

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
import org.graphframes._
import com.twosigma.flint.timeseries.TimeSeriesRDD
import scala.concurrent.duration._
import com.twosigma.flint.timeseries.CSV
import com.twosigma.flint.timeseries.summarize
import com.twosigma.flint.FlintConf
import com.twosigma.flint.timeseries



// ref : https://github.com/twosigma/flint

object flintTimeseries {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()

    val sc = spark.sqlContext
    val tsRdd = CSV.from(
      sc,
      "file:///C:/DataForCourses/StockTimeSeries.csv",
      header = true,
      dateFormat = "yyyy-mm-dd",
      sorted = true
    )

    val df =

    tsRdd.cache()
    val dataset = tsRdd.collect()

    dataset.foreach(println)


    //println(tsRdd.count())

  }

}

