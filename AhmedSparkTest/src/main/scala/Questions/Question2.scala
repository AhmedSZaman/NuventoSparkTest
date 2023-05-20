package Questions

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

case class accountData(customerID: String, forename: String, surname: String,accounts: Array[String],
                       balance: Int, totalBalance: Long, numberAccounts: Long, averageBalance: Double )
case class address(addressId:String, customerId: String, address:String)
case class parsedAddress(number: String, street: String, city: String, country:String)
object Question2 {
  def main(args: Array[String]): Unit = {
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


    val addressDataFrame = spark.read
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
    addressDataFrame.join(parqDF, Seq("customerId")).show(truncate = false)
    val p = addressDataFrame
      .select("address")
      .withColumn("number", split(col("address"), ",")(0))
      .withColumn("street", split(col("address"), ",")(1))
      .withColumn("city", split(col("address"), ",")(2))
      .withColumn("country", split(col("address"), ",")(3))
      .as[parsedAddress]
    p.show(truncate = false)
    //TODO: Clean up code, parse address?
  }
}
