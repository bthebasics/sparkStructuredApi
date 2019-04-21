package com.btb.spark.structuredApi.structuredStreaming

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType

final case class person(Firstname:String, Lastname:String,Sex:String,Age:Long  )

object monitorDir {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    val INPUT_DIRECTORY = "file:///C:/DataForCourses/streamingExample/person"  //streamingExample\person
    println("Starting Structured streaming Average job .... ")

    // 1 - Start the spark session
    val spark = SparkSession.builder()
                  .appName("structuredStreamingMonitorDir")
      .config("spark.eventLog.enabled","false")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","2g")
      .config("spark.sql.shuffle.partitions","2")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    //2 - define the input data schema
    val personSchema = new StructType()
      .add("firstname","string")
      .add("lastname","string")
      .add("sex","string")
      .add("age","long")

    val personStreamDF = spark.readStream.schema(personSchema).json(INPUT_DIRECTORY) //.as(person)

   // val personStreamDS = personStreamDF.as(person)

    personStreamDF.createTempView("person")

    val ageAverage = spark.sql("select avg(age) as average_age, sex from person group by sex")

    val query = ageAverage.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()


  }


}
