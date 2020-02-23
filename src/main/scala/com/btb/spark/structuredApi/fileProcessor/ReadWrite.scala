package com.btb.spark.structuredApi.fileProcessor

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object ReadWrite {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    // https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala
    val fileType = "csv" // csv, json
    val keystartpos = 10;
    val keylength = 9;
    val recIDexist = true
    val recID = "0039"
    val recIDlength = 4
    val recIDstart = 1
    val inputFilepath = "file:///C:/DataForCourses/cars04.csv"
    val outputPath = "file:///C:/DataForCourses/FixedLength_mod.txt"


  /*  if (fileType == "fixedLength") {

      if (recIDexist) {
        val rdd = spark.read.textFile(inputFilepath).withColumn("rowNum", monotonicallyIncreasingId).withColumn("recID", col("value").substr(1, 4))
        val rdd2 = rdd.withColumn("joinKey", expr(" case when recID = " + recID + " then substring( value, " + keystartpos + "," + keylength + ") else null end  "))
        rdd2.printSchema()
        rdd2.show()
        val rdd3 = createFinalDF(rdd2, recIDexist)
        rdd3.printSchema()
        rdd3.write.mode("overwrite").text(outputPath)
      }

    } */
   if (fileType == "csv") {
      println("csv file processing")
     //val rdd = spark.read.csv(inputFilepath).rdd
     //val rdd = spark.read.textFile(inputFilepath).withColumn("key", split(col("value"),",").getItem(100))
     val rdd = spark.read.textFile(inputFilepath).withColumn("key", getColSplit(col("value"),",",1) ).withColumn("joinKey", getColSplit(col("value"),",",7) )
     rdd.show(2)
     //println("rdd Data = ", rdd.first())

   }

  }

  def getColSplit(column: Column, delim: String, index: Int): Column = {
    split(column, delim).getItem(index)

  }

  def createFinalDF(df: DataFrame, recIDexist: Boolean): DataFrame = {

    if (recIDexist) {
      df.withColumn("final", concat(col("value"), col("joinKey"))).drop("value", "rownum", "joinkey", "recID")
    }
    else {
      df.withColumn("final", concat(col("value"), col("joinKey"))).drop("value", "rownum", "joinkey")
    }
  }
}

