package com.btb.spark.structuredApi.sparksql


// $example on:programmatic_schema$
import org.apache.avro.generic.GenericData
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


object sparksqlDatafranesCsv {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    val carDF = spark.read.format("csv").option("inferSchema","true").option("header","true").load("file:///C:/DataForCourses/cars04.csv")

    //carDF.show(10,false)
    //carDF.head(10)


    val schema = new StructType()
        .add("Brand",StringType, true)
      .add("VehicleName",StringType, true)
      .add("Hybrid",IntegerType, true)
      .add("SuggestedRetailPrice",IntegerType, true)
      .add("DealerCost",IntegerType, true)
      .add("EngineSize",DoubleType, true)
      .add("Cylinders",IntegerType, true)
      .add("Horsepower",IntegerType, true)
      .add("CityMPG",IntegerType, true)
      .add("HighwayMPG",IntegerType, true)
      .add("Weight",IntegerType, true)
      .add("WheelBase",IntegerType, true)
      .add("Length",IntegerType, true)
      .add("Width",IntegerType, true)

    val carDF2 = spark.read
      .schema(schema)
      .option("sep","#")
      .option("header","true")
      .csv("file:///C:/DataForCourses/carInventory.csv")

    //println(carDF.printSchema())
    //println(carDF2.printSchema())

   // carDF2.where("VehicleName like 'Audi A4%'").show(false)

    println("Total Cards in carDF2 DF = " + carDF2.count().toString)

    //DROPMALFORMED mode
    val carDF3 = spark.read
      .schema(schema)
      .option("sep","#")
      .option("mode", "DROPMALFORMED")  //DROPMALFORMED FAILFAST
      .option("header","true")
      .csv("file:///C:/DataForCourses/carInventory.csv")

    //println("Total Cards in carDF3 DF = " + carDF3.count().toString)

    println("number of partitions = ",carDF3.rdd.getNumPartitions)
    carDF3.show(false)
    carDF3.take(2)

    println(carDF3.explain())

    val carSchema = spark.read.format("csv")
      .option("inferSchema","true")
      .option("sep","#")
      .option("header","true").load("file:///C:/DataForCourses/carInventory.csv")
      .schema

    carSchema.printTreeString()

   // println(s"Count of Audi cards ",carDF2.where("VehicleName like 'Audi A4%'").count())


  }// end of main
}

//https://docs.databricks.com/spark/latest/data-sources/read-csv.html
//https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/DataFrameReader.html