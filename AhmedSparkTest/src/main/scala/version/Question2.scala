package version

import org.apache.log4j._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, _}


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
    val customerAccountDataSet = spark.read.parquet("src/main/resources/data/output/Question1").as[customerAccountData]

    customerAccountDataSet.printSchema()

    //Redunct
    val addressDataSet = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/address_data2.txt")
      .as[addressData]
    val customerDocumentDataSet = createCustomerDocumentDataSet(spark, addressDataSet, customerAccountDataSet)
    customerDocumentDataSet.show()
    /*
    val connections = addressDataFrame
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .filter("connections == 1")
      .groupBy("id").agg(sum("connections").alias("connections"))*/
    /*addressDataSet.join(parqDF, Seq("customerId")).show(truncate = false)
    val p = addressDataSet
      .select("address", "addressID", "customerID")
      .withColumn("streetNumber", split(col("address"), ",")(0))
      .withColumn("streetName", split(col("address"), ",")(1))
      .withColumn("city", split(col("address"), ",")(2))
      .withColumn("country", split(col("address"), ",")(3))
      .as[parsedAddress]
    p.show(truncate = false)

    p.join(parqDF, Seq("customerID"))
      .select(parqDF ("customerID"), parqDF("forename"), parqDF("surname"),
        parqDF("accounts"), p("streetNumber"), p("streetName"),
        p("city"), p("country"))
      .show(truncate = false)*/
    //TODO: Clean up code, parse address?
    spark.close()
  }

  def createCustomerDocumentDataSet(implicit spark: SparkSession, addressDataSet: Dataset[addressData],
                                     customerAccountDataSet: Dataset[customerAccountData]): Dataset[customerDocumentData] = {
    import spark.implicits._
    //addressDataSet.join(customerAccountDataSet, Seq("customerId")).show(truncate = false)

    val parsedAddressDataSet = addressDataSet
      .select("address", "addressID", "customerID")
      .withColumn("streetNumber", split(col("address"), ",")(0))
      .withColumn("streetName", split(col("address"), ",")(1))
      .withColumn("city", split(col("address"), ",")(2))
      .withColumn("country", split(col("address"), ",")(3))
      .as[parsedAddressData]
    parsedAddressDataSet.show(truncate = false)

    val customerDocumentDataSet = parsedAddressDataSet.join(customerAccountDataSet, Seq("customerID"))
      .select(customerAccountDataSet("customerID"), customerAccountDataSet("forename"), customerAccountDataSet("surname"),
        customerAccountDataSet("accounts"), parsedAddressDataSet("streetNumber"), parsedAddressDataSet("streetName"),
        parsedAddressDataSet("city"), parsedAddressDataSet("country"))
      .as[customerDocumentData]

    customerDocumentDataSet
  }
}