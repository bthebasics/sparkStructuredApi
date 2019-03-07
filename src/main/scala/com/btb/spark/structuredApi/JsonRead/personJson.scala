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

//TODO: http://blogs.adatis.co.uk/ivanvazharov/post/Parsing-nested-JSON-lists-in-Databricks-using-Python

object personJson {
  def main(args: Array[String]): Unit = {

    println("hello world")
    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()


    val df = spark.read
      .option("multiline","true")
      .json("file:///C:/gitRepo/json-examples/src/persons.json")

    df.printSchema()
    //df.select(explode(df("persons")).alias("person")).select(explode(col("person.cars")).alias("brand")).select("brand.name", "brand.models").printSchema() //select("brand.name", explode(col("brand.models"))).show()

    import spark.implicits._


    val persons = df.select(explode(col("persons")).alias("persons"))
    persons.printSchema()
    persons.show()

    val persons_cars = persons.select(
      col("persons.name").alias("Name"),
      col("persons.age").alias("Age"),
      explode(col("persons.cars")).alias("Car_brands"),
      col("Car_brands.name").alias("car_brand_list")
    )
    persons_cars.printSchema()
    persons_cars.show()

    val personCarsAll = persons_cars.select(
      col("Name"),
      col("Age"),
      col("car_brand_list").alias("BrandName"),
      explode(col("Car_brands.models"))
    )

    personCarsAll.printSchema()
    personCarsAll.show()





  }

}
