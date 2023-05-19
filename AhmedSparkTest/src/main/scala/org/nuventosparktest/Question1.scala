package org.nuventosparktest

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Question1 {

  case class accountData(customerID: String, accountID: String, balance: Int)
  case class customerData(customerID: String, forename: String, surname: String)
  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("Question1")
      .master("local[*]")
      .getOrCreate()

    //Schema
    val customerDataSchema = new StructType()
      .add("customerID", StringType, nullable = true)
      .add("forename", StringType, nullable = true)
      .add("surname", StringType, nullable = true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._
    val accountDataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/account_data.txt")
      .as[accountData]


    val customerDataFrame = spark.read
      .schema(customerDataSchema)
      .option("header", "true")
      .option("sep",",")
      .csv("src/main/resources/data/customer_data1.txt")
      .as[customerData]

    val numberAccounts = accountDataFrame
      .groupBy("customerID").agg(count("customerID").alias("numberAccounts"))

    val totalBalance = accountDataFrame
      .groupBy("customerID").agg(round(sum("balance"), 2).alias("totalBalance"))

    val accountArray = accountDataFrame.groupBy("customerID").agg(collect_list("accountID").as("accounts"))

    val joinedDataFrame = accountDataFrame.join(customerDataFrame, accountDataFrame("customerID") === customerDataFrame("customerID"), "inner")
      .join(numberAccounts, Seq("customerID"), "inner")
      .join(totalBalance, Seq("customerID"), "inner")
      .join(accountArray, Seq("customerID"), "inner")
      .select(customerDataFrame("customerID"), customerDataFrame("forename"), customerDataFrame("surname"),accountDataFrame("balance"),
        accountArray("accounts"), totalBalance("totalBalance"), numberAccounts("numberAccounts") )
      .distinct()

    val finalDataFrame = joinedDataFrame.withColumn("averageBalance", $"totalBalance" / $"numberAccounts")


    finalDataFrame.show(truncate = false)
  }

}