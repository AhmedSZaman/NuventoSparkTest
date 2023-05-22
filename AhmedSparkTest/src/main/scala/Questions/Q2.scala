package Questions
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import version.{address, parsedAddress}
case class customerAccountData(customerID: String, forename: String, surname: String,accounts: Array[String],
                       balance: Int, totalBalance: Long, numberAccounts: Long, averageBalance: Double )
case class address(addressId:String, customerId: String, address:String)
case class parsedAddress(streetNumber: String, streetName: String, city: String, country:String)
class Q2 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkSession using every core of the local machine
  val spark = SparkSession
    .builder
    .appName("Question2")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val parqDF = spark.read.parquet("src/main/resources/data/output/Question1")

  parqDF.printSchema()


  val addressDataSet = spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/address_data2.txt")
    .as[address]
  /*
      val connections = addressDataFrame
        .withColumn("id", split(col("value"), " ")(0))
        .withColumn("connections", size(split(col("value"), " ")) - 1)
        .filter("connections == 1")
        .groupBy("id").agg(sum("connections").alias("connections"))*/
  addressDataSet.join(parqDF, Seq("customerId")).show(truncate = false)
  val p = addressDataSet
    .select("address", "addressID", "customerID")
    .withColumn("streetNumber", split(col("address"), ",")(0))
    .withColumn("streetName", split(col("address"), ",")(1))
    .withColumn("city", split(col("address"), ",")(2))
    .withColumn("country", split(col("address"), ",")(3))
    .as[parsedAddress]
  p.show(truncate = false)

  p.join(parqDF, Seq("customerID"))
    .select(parqDF("customerID"), parqDF("forename"), parqDF("surname"),
      parqDF("accounts"), p("streetNumber"), p("streetName"),
      p("city"), p("country"))
    .show(truncate = false)
  //TODO: Clean up code, parse address?
  spark.close()
}
