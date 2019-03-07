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

object sparkJoins {
  def main(args: Array[String]): Unit = {
    println("**************** Join Example **********************")

    val spark = SparkSession.builder().appName("jsonReader").master("local[*]").enableHiveSupport().getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val salesTrans =   spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/SuperStore/SalesTransactions.csv")

    val salesMgr =   spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/SuperStore/managerByRegion.csv")

    val salesReturn =   spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/SuperStore/totalReturns.csv")


    salesTrans.printSchema()
    salesMgr.printSchema()
    salesReturn.printSchema()

    salesTrans.createOrReplaceTempView("sales")
    salesMgr.createOrReplaceTempView("salesMgr")
    salesReturn.createOrReplaceTempView("returns")
    salesTrans.cache()

    spark.sql("cache table sales")
    import spark.implicits._


    /*  type 1 : using join -

    val salesTransDf = salesTrans.join(salesMgr, "Region")

    salesTransDf.printSchema()
    salesTransDf.show(10)
  */

    // Methode 2 : using broadcast join
    // ref : https://stackoverflow.com/questions/37487318/spark-sql-broadcast-hash-join

/*
    val salesTransDf = salesTrans.join(broadcast(salesMgr), Seq("Region"), "inner" )
    salesTransDf.printSchema()
    salesTransDf.explain(true)

    val returnDf = salesTrans.join(broadcast(salesMgr), Seq("Region"), "left" ).join(salesReturn, $"Order_ID" === $"Order ID", "left")

    returnDf.printSchema()
    returnDf.show()
*/
    // Method 3 : using SQL and hints
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val threshold =  spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt
    println(s"threshold = ${threshold}")
    val df = spark.sql("select /*+ BROADCAST (b) */ a.*, b.Person from sales a left join salesMgr b where a.Region = b.Region ") // or  /*+ MAPJOIN (rt) */
    println(df.queryExecution.logical.numberedTreeString)
    println(df.queryExecution.sparkPlan.numberedTreeString)
    df.show()

    // ref : https://stackoverflow.com/questions/31240148/spark-specify-multiple-column-conditions-for-dataframe-join



  }









}
