package org.nuventosparktest

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Question1 {

  case class accountData(customerId: String, accountId: String, balance: Int)
  case class customerData(customerId: String, forename: String, surname: String)
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

    // Create schema
    val accountDataSchema = new StructType()
      .add("customerId", StringType, nullable = true)
      .add("accountId", StringType, nullable = true)
      .add("balance", IntegerType, nullable = true)

    val customerDataSchema = new StructType()
      .add("customerId", StringType, nullable = true)
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

    val numberOfAccounts = accountDataFrame
      .groupBy("customerId").agg(count("customerId").alias("numberOfAccounts"))

    val totalBalance = accountDataFrame
      .groupBy("customerId").agg(round(sum("balance"), 2).alias("totalBalance"))

    val accountArray = accountDataFrame.groupBy("customerId").agg(collect_list("accountId").as("accounts"))
/*
    customerDataFrame.join(accountDataFrame, Seq("customerId"), "inner")
      .select(customerDataFrame("customerId"), customerDataFrame("forename"), customerDataFrame("surname"),accountDataFrame("balance"),
        accountDataFrame("accountId") )
      .show()*/

    val joinedDataFrame = accountDataFrame.join(customerDataFrame, accountDataFrame("customerId") === customerDataFrame("customerId"), "inner")
      .join(numberOfAccounts, Seq("customerId"), "inner")
      .join(totalBalance, Seq("customerId"), "inner")
      .join(accountArray, Seq("customerId"), "inner")
      .select(customerDataFrame("customerId"), customerDataFrame("forename"), customerDataFrame("surname"),accountDataFrame("balance"),
        accountArray("accounts"), totalBalance("totalBalance"), numberOfAccounts("numberOfAccounts") )
      .distinct()

    val finalDataFrame = joinedDataFrame.withColumn("AverageBalance", $"totalBalance" / $"numberOfAccounts")


    finalDataFrame.show(truncate = false)
  }

}