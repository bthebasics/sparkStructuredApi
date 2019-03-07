package com.btb.spark.structuredApi.operations


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
import org.apache.spark.sql.functions.approx_count_distinct
import org.apache.spark.sql.functions.skewness


object aggregates_adv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()


    val trxn =   spark.read
      .option("inferSchema","true")
      .option("sep",";")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/data_berka/trans.asc")

    trxn.printSchema()

   // trxn.show(10)
  //  trxn.agg(approx_count_distinct("trans_id", 0.05))
    println("***********************************************************")
    //println(trxn.agg(approx_count_distinct("trans_id", 0.05)))

    //trxn.agg(approx_count_distinct("trans_id", 0.05)).show() //38.6
   //trxn.select("trans_id").distinct().count() // 809

    //trxn.select(skewness("account_id"), skewness("trans_id"),skewness("date"),kurtosis("account_id"), count("trans_id"), countDistinct("trans_id")).show()
//trxn.selectExpr("* , (account_id % 6 ) as partKey").show()

  }

}
