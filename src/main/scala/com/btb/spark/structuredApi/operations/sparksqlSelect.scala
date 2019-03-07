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



import org.apache.spark.sql.Column

object sparksqlSelect {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    val carDF = spark.read
      .option("inferSchema","true")
      .option("sep","#")
      .option("header","true")
      .csv("file:///C:/DataForCourses/carInventory.csv")

    carDF.printSchema()
    carDF.select("Brand","Vehicle_Name", "SuggestedRetailPrice").show()

    import spark.implicits._

    //val col1:Column = $"Length"

    // there are various ways to refer to col
    carDF.select(carDF.col("Brand"), carDF.col("SuggestedRetailPrice"),carDF.col("HighwayMPG")).show()
    carDF.select(
      carDF.col("DealerCost").alias("DealerSellPrice").cast(DoubleType),
      carDF.col("CityMPG").alias("CityMileage").cast(DoubleType),
      $"Length".alias("lengthOfCar").cast(DoubleType),
      'width.alias("widthOfCar").cast(DoubleType)
    ).show()

  carDF.selectExpr("SuggestedRetailPrice",
    "(SuggestedRetailPrice / 1000.0) as SuggestedRetailPrice1K",
    "case when Hybrid == '1' then true else false end as HybrindInd")
    .filter("HybrindInd == true").show()

    val sourceDF = Seq(
      ("  p a   b l o", "Paraguay"),
      ("Neymar", "B r    asil")
    ).toDF("name", "country")

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
    sourceDF.show()

    carDF.columns.foreach(println)

    carDF.withColumnRenamed("Vehicle_Name","VehicleName").columns.foreach(println)





  }
}
