package com.btb.spark.structuredApi.sparksql


// $example on:programmatic_schema$
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


object helloWorld extends App{


    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

  val df = sparkSession.range(1000).toDF()
  df.show()
  df.printSchema()

    sparkSession.conf.getAll.foreach(println)
  while(true){
    1==1
  }
    println(" Welcome to Spark Tutorial")

}
