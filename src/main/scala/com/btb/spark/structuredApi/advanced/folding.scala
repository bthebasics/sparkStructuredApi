package com.btb.spark.structuredApi.advanced


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

object folding {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Folding Function")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

import spark.implicits._


 /*   val someDF = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")   */


    // Ref : https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393
    

    val sourceDF = Seq(
      ("  p a   b l o", "Paraguay"),
      ("Neymar", "B r    asil")
    ).toDF("name", "country")

   sourceDF.show()


    val actualDF = Seq(
      "name",
      "country"
    ).foldLeft(sourceDF) { (memoDF, colName) =>
      memoDF.withColumn(
        colName,
        regexp_replace(col(colName), "\\s+", "")
      )
    }

    actualDF.show()

    def snakeCaseColumns(df: DataFrame): DataFrame = {
      df.columns.foldLeft(df) { (memoDF, colName) =>
        memoDF.withColumnRenamed(colName, toSnakeCase(colName))
      }
    }

    def toSnakeCase(str: String): String = {
      str.toLowerCase().replace(" ", "_")
    }

    val sourceDF1 = Seq(
      ("funny", "joke")
    ).toDF("A b C", "de F")

    sourceDF1.show()

    val actualDF1 = sourceDF1.transform(snakeCaseColumns)
    actualDF1.show()

  }

}
