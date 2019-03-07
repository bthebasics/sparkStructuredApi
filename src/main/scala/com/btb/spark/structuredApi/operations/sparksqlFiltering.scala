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


object sparksqlFiltering {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    val chicagoCrimeDFSchema = new StructType()
      .add( "ID" , IntegerType , true )
      .add( "IncidentDate" , StringType , true )
      .add( "Block" , StringType , true )
      .add( "PrimaryType" , StringType , true )
      .add( "LocationDescription" , StringType , true )
      .add( "Arrest" , BooleanType , true )
      .add( "Domestic" , BooleanType , true )
      .add( "District" , IntegerType , true )
      .add( "Ward" , IntegerType , true )
      .add( "CommunityArea" , IntegerType , true )
      .add( "FBICode" , StringType , true )
      .add( "UpdatedOn" , StringType , true )

    val chicagoCrimeDF = spark.read
      //.option("inferSchema","true")
      .schema(chicagoCrimeDFSchema)
      .option("sep",",")
      .option("header","true")
      .csv("file:///C:/DataForCourses/ChicagoChrimeTS.csv")

    val from_pattern1 = "MM/dd/yyyy"
    val to_pattern1 = "yyyy-MM-dd"

    val from_pattern2 = "MM/dd/yyyy HH:mm"
    val to_pattern2 = "yyyy-MM-dd HH:mm"


    //val chicagoCrimeDF1 = chicagoCrimeDF.withColumn("IncidentDateTS", date_format(to_date(chicagoCrimeDF.col("IncidentDate"), from_pattern1),to_pattern1))

    val chicagoCrimeDF1 = chicagoCrimeDF.select(col("*"),
      date_format(to_date(col("IncidentDate"), from_pattern1),to_pattern1).cast(DateType).alias("IncidentDateTime"),
      date_format(to_timestamp(col("UpdatedOn"), from_pattern2),to_pattern2).cast(TimestampType).alias("UpdatedOnDateTime")
              )

    //unix_timestamp($"date", "yyyy/MM/dd HH:mm:ss").cast(TimestampType)
    val chicagoCrimeDF2 =  chicagoCrimeDF1.drop("IncidentDate", "UpdatedOn")

    //.withColumn("IncidentDateTS", date_format(to_date(chicagoCrimeDF.col("IncidentDate"), from_pattern1),to_pattern1))
    chicagoCrimeDF1.printSchema()

    //chicagoCrimeDF2.show(10, false)

    // display all the incidents of type - PrimaryType = 'THEFT' and happened within last 1 year.

    val primaryTypeFilter = "PrimaryType == 'THEFT'"
    val IncidentDateFilter = "IncidentDateTime > date_sub(current_timestamp(), 365)"
    val chicagoCrimeDF3 = chicagoCrimeDF2.filter(IncidentDateFilter)
/*
    chicagoCrimeDF2
      .filter(col("PrimaryType").equalTo("THEFT"))
      .filter("IncidentDateTime > date_sub(UpdatedOnDateTime, 365)")
      .show()
*/
    import spark.implicits._

 // type # 1
  /*  chicagoCrimeDF2
      .filter(col("PrimaryType").equalTo("THEFT"))
      .filter(col("IncidentDateTime").>=(date_sub(current_timestamp(), 700)))
      .where($"LocationDescription" === "STREET")
      .show()
      */

  // type 2
    //  chicagoCrimeDF2.where("PrimaryType == 'THEFT' and IncidentDateTime >= date_sub(current_date, 700) and LocationDescription == 'STREET' ").show()

    //type 3 IN

    //val LocationDescList =  List("RESIDENCE", "STREET")
    //chicagoCrimeDF2.filter(col("LocationDescription").isin(LocationDescList:_*)).show()

    //type 4 OR
    //chicagoCrimeDF2.where($"LocationDescription" === "RESIDENCE" || $"LocationDescription" === "STREET")
    //chicagoCrimeDF2.where("LocationDescription = 'RESIDENCE' or LocationDescription = 'STREET'").show()



    //type 5 ( case statement )
/*    chicagoCrimeDF2
      .withColumn("IncidentYearmo",regexp_replace(substring($"IncidentDateTime",1,7),"-",""))
      .withColumn("SolvedInd", when(col("Arrest") === "false", "N" )otherwise("Y"))
      .show()
*/

    //type 5 ( case statement )
    val isSolved = udf((Arrest: String) => {
      if (Arrest == "false") "N"
      else "Y"
    })

    chicagoCrimeDF2.withColumn("SolvedInd", isSolved(col("Arrest"))).show()

    chicagoCrimeDF2.head(4)

    val dateDF = spark.range(1)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
    .withColumn("week_ago", date_sub(col("today"), 7))

    dateDF.show()




/*    val priceFilter = col("SuggestedRetailPrice") > 30000
    val HorsepowerFilter = col("Horsepower") > 250
  */

  }

}
