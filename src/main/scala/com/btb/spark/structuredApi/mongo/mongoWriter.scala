package com.btb.spark.structuredApi.mongo


// $example on:programmatic_schema$
import org.apache.avro.generic.GenericData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
// $example off:programmatic_schema$
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._


object mongoWriter {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Northwind.orders")
      .config("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Northwind.orders")
      .getOrCreate()

    val carDF = spark.read.format("csv").option("inferSchema","true").option("header","true").load("file:///C:/DataForCourses/cars04.csv")
    carDF.printSchema()

// encountered the conenction error : https://github.com/vert-x3/vertx-mongo-client/issues/119
// updated the mongo driver jar to 3.10 and error gone - Cluster Listener

    //carDF.write.format("mongo").mode("append").save()
    val df = MongoSpark.load(spark)
    //val df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

    df.show()

    MongoSpark.save(carDF.write.option("collection", "carData").mode("overwrite"))




  } //
}
