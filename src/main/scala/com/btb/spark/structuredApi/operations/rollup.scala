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
import org.apache.spark.sql.expressions.Window

object rollup {
  def main(args: Array[String]): Unit = {

    println("hello Rollup")
    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()

    val trxn =   spark.read
      .option("inferSchema","true")
      .option("sep",";")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/data_berka/trans.asc")

    val trxnDf = trxn.withColumnRenamed("date", "accountDate")

    trxnDf.printSchema()
    trxnDf.cache()
    //trxn.show(15)

    trxnDf.rollup("type","operation" ).sum("amount").show()
    trxnDf.cube("type","operation").count().show()
    trxnDf.groupBy("type","operation").count().show()

 // TODO: ref https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup

    //cube is equivalent to CUBE extension to GROUP BY. It takes a list of
    // columns and applies aggregate expressions to all possible combinations of the grouping columns. Lets say you have data like this:

    /*
    df.cube($"x", $"y").count.show

      // +----+----+-----+
      // |   x|   y|count|
      // +----+----+-----+
      // |null|   1|    1|   <- count of records where y = 1
      // |null|   2|    3|   <- count of records where y = 2
      // | foo|null|    2|   <- count of records where x = foo
      // | bar|   2|    2|   <- count of records where x = bar AND y = 2
      // | foo|   1|    1|   <- count of records where x = foo AND y = 1
      // | foo|   2|    1|   <- count of records where x = foo AND y = 2
      // |null|null|    4|   <- total count of records
      // | bar|null|    2|   <- count of records where x = bar
      // +----+----+-----+
      A similar function to cube is rollup which computes hierarchical subtotals from left to right:

      df.rollup($"x", $"y").count.show
      // +----+----+-----+
      // |   x|   y|count|
      // +----+----+-----+
      // | foo|null|    2|   <- count where x is fixed to foo
      // | bar|   2|    2|   <- count where x is fixed to bar and y is fixed to  2
      // | foo|   1|    1|   ...
      // | foo|   2|    1|   ...
      // |null|null|    4|   <- count where no column is fixed
      // | bar|null|    2|   <- count where x is fixed to bar
// +----+----+-----+
     */



  }
}
