package com.btb.spark.structuredApi.fileProcessor

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}


object fixedLegthReadWrite {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    val fileType = "fixedLength" // csv, json
    val keystartpos = 10;
    val keylength = 9;
    val recIDexist=true
    val recID = "0039"
    val recIDlength = 4
    val recIDstart = 1
    val inputFilepath = "file:///C:/DataForCourses/FixedLength.txt"
    val outputPath = "file:///C:/DataForCourses/FixedLength_mod.txt"


    if ( fileType == "fixedLength") {

      if (recIDexist) {
        val rdd = spark.read.textFile(inputFilepath).withColumn("rowNum", monotonicallyIncreasingId).withColumn("recID", col("value").substr(1, 4))
        val rdd2 = rdd.withColumn("joinKey", expr(" case when recID = " + recID + " then substring( value, " + keystartpos + "," + keylength + ") else null end  "))
        rdd2.printSchema()
        rdd2.show()
        val rdd3 = createFinalDF(rdd2, recIDexist)
        rdd3.printSchema()
        rdd3.write.mode("overwrite").text(outputPath)
      }

    }

    /*else {
      val rdd = spark.read.textFile("file:///C:/DataForCourses/FixedLength.txt").withColumn("recID", col("value").substr(recIDlength,recIDstart ))
    }

    if ( recIDexist ){
      val rdd2 = rdd.withColumn("rowNum", monotonicallyIncreasingId).withColumn("joinKey", expr( """ case when    """ )  col("value").substr(keylength,keystartpos))
    }
    else  {
      val rdd2 = rdd.withColumn("rowNum", monotonicallyIncreasingId).withColumn("joinKey", col("value").substr(keylength,keystartpos))

    }

    val rdd3 = rdd2.withColumn("final",concat(col("value"),col("joinKey"))).drop("value","rownum","joinkey")
    rdd3.printSchema()



    rdd3.write.mode("overwrite").text("file:///C:/DataForCourses/FixedLength_mod.txt")

*/

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

