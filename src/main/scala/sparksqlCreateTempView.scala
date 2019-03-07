
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object sparksqlCreateTempView {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ReadingCSV")
      .master("local[*]")
      .getOrCreate()

    val newSessionval = spark.newSession()

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

    val carDF = spark.read
      .schema(schema)
      .option("sep","#")
      .option("mode", "DROPMALFORMED")  //DROPMALFORMED FAILFAST
      .option("header","true")
      .csv("file:///C:/DataForCourses/carInventory.csv")

    carDF.createGlobalTempView("carInventory")
    carDF.createOrReplaceTempView("carInventory")

    println("newSessionval.catalog.tableExists = ",newSessionval.catalog.tableExists("carInventory"))
    println("spark.catalog.tableExists = ",spark.catalog.tableExists("carInventory"))

    spark.stop()



  }

}

//https://stackoverflow.com/questions/42774187/spark-createorreplacetempview-vs-createglobaltempview
//https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
//https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html
//"-Dlog4j.configuration=conf/log4j-defaults.properties" to force it to
//--driver-java-options="-Dlog4j.configuration=log4j.properties"


//https://www.oreilly.com/library/view/fast-data-processing/9781785889271/ch09s02.html