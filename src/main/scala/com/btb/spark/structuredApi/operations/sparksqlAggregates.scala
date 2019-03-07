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
import org.apache.spark.sql.expressions.Window


object sparksqlAggregates {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    val retailSales = spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/Retail_Store/SuperStore_US_Sales.csv").withColumn("OrderDate", col("OrderDate").cast(DateType))
        .withColumn("OrderMonth", year(col("OrderDate")) * 100 + month(col("OrderDate")))

    val returns = spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/Retail_Store/TotalReturns_1.csv")

    val manager = spark.read
      .option("inferSchema","true")
      .option("sep",",")
      .option("header","true")
      .option("timestampFormat", "MM/dd/yyyy")
      .option("mode", "DROPMALFORMED")
      .csv("file:///C:/DataForCourses/Retail_Store/ManagerByRegion.csv")

    retailSales.printSchema()
    //retailSales.show(false)

    import spark.implicits._

        // step: find out the sales by city and month
    //val retailSalesDF = retailSales.groupBy("City").agg(sum("Sales"), count(col("Order_ID")))

    //val retailSalesDF = retailSales.groupBy("City").agg(Map("Order_ID"->"count", "Sales"->"sum", "Sales"->"max" ))
    //1retailSalesDF.show()

    //val retailSalesDF2 = retailSales.groupBy("City", "OrderMonth").agg(sum(col("Sales")),max(col("Sales")))
    //retailSalesDF2.show()


    val retailSalesDF3 = retailSales.groupBy("Sub_Category", "OrderMonth").agg(sum(col("Sales")).alias("TotalSales"),max(col("Sales")).alias("MaxSalesAmount"))
    //retailSalesDF3.show()
    // find out the top 3 selling Sub_Category per month

    import org.apache.spark.sql.expressions.WindowSpec
    val window = Window.partitionBy(col("OrderMonth")).orderBy(col("TotalSales").desc)
    val column_row = row_number.over(window)
    //retailSalesDF3.select(col("OrderMonth"), col("Sub_Category"), col("TotalSales"), column_row.as("row_num")).filter("row_num < 4").withColumn("TotalSales", format_number(col("TotalSales"), 2) ).show


    // find ouy the most sales catagory per region
    val retailSalesDF4 = retailSales.groupBy(col("Region"), col("Category")).agg(sum(col("Sales")).alias("TotalSales"),max(col("Sales")).alias("MaxSalesAmount"))
    retailSalesDF4.show()
    retailSalesDF4.createOrReplaceTempView("SalesByRegion")

    spark.catalog.listTables()

    //spark.sql("select * from SalesByRegion").show()

   //spark.sql("select Region, Category, cast(TotalSales  as DECIMAL(10,2)) as TotalSales1, Rank() over (partition by Region order by TotalSales desc) ranker from SalesByRegion ").orderBy("Region", "ranker").show()
    // find the weekly sales, vs weekend sales

    retailSales.printSchema()

    val retailSalesDF5 = retailSales.withColumn("WeekDay", date_format(to_date(col("OrderDate")), "EEEEE"))
      .withColumn("WeekInd",
        when( col("WeekDay") ===  "Saturday" || col("WeekDay") ===  "Sunday", 1)
          .otherwise(0)
      )


   // retailSalesDF5.show()




    // number of returns per week

    //https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-joins.html

    // good 1 - retailSales.join(returns, Seq("Order_id"), "left").withColumn("Returned", when( col("Returned").isNull, 0).otherwise(1) ).show()

    //retailSales.join(return, Seq("Order_ID"), "left" )
    //returns.show()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")
    spark.conf.set("spark.sql.broadcastTimeout","300")



    retailSales.as("rs").join(returns.as("r"), retailSales("Order_ID") <=> returns("Order_ID"),"left")
      .select($"rs.*",$"r.Returned")
      .withColumn("Returned", when( col("Returned").isNull, 0).otherwise(1) )
      .explain()

    manager.show()
    manager.cache()
    broadcast(manager)

    retailSales.as("rs").join(manager.as("m"), retailSales("Region") <=> manager("Region"))
      .select($"rs.*", $"m.Person")
      .show()

    retailSales.as("rs").join(manager.as("m"), retailSales("Region") <=> manager("Region"))
      .select($"rs.*", $"m.Person")
      .explain()


    retailSales.createTempView("retailSales")
    returns.createOrReplaceTempView("returns")

    spark.sql("SELECT /*+ SKEW('retailSales') */ * FROM retailSales, returns WHERE returns.Order_ID = retailSales.Order_ID").explain()


  } // end of main
}

