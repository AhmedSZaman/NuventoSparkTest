package version

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

import scala.reflect.runtime.universe._

case class accountData(customerID: String, accountID: String, balance: Int)
case class customerData(customerID: String, forename: String, surname: String)
object Question1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkSession using every core of the local machine
    implicit val spark = SparkSession
      .builder
      .appName("Question1")
      .master("local[*]")
      .getOrCreate()

    //Schema
    val customerDataSchema = new StructType()
      .add("customerID", StringType, nullable = false)
      .add("forename", StringType, nullable = false)
      .add("surname", StringType, nullable = false)
/*


    val accountDataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/account_data1.txt")
      .as[accountData]


    val customerDataFrame = spark.read
      .schema(customerDataSchema)
      .option("header", "true")
      .option("sep",",")
      .csv("src/main/resources/data/customer_data1.txt")
      .as[customerData]
*/
    import spark.implicits._
    val accountDataFrame = loadDataFrame[accountData]("src/main/resources/data/account_data1.txt")
    val customerDataFrame = loadDataFrame[customerData]("src/main/resources/data/customer_data1.txt")
    val CustomerAccountOutput = createCustomerAccountDataFrame(spark, accountDataFrame, customerDataFrame)
/*
    val numberAccounts = accountDataFrame
      .groupBy("customerID").agg(count("accountID").alias("numberAccounts"))

    val totalBalance = accountDataFrame
      .groupBy("customerID").agg(round(sum("balance"), 2).alias("totalBalance"))

    val accountArray = accountDataFrame
      .groupBy("customerID").agg(collect_list("accountID").as("accounts"))


    val joinedDataFrame = accountDataFrame.join(customerDataFrame, Seq("customerID"), "inner")
      .join(numberAccounts, Seq("customerID"), "inner")
      .join(totalBalance, Seq("customerID"), "inner")
      .join(accountArray, Seq("customerID"), "inner")
      .select(customerDataFrame("customerID"), customerDataFrame("forename"), customerDataFrame("surname"),
        accountArray("accounts"), totalBalance("totalBalance"), numberAccounts("numberAccounts") )
      .distinct()

    val CustomerAccountOutput = joinedDataFrame
      .withColumn("averageBalance", round($"totalBalance"/$"numberAccounts",2).cast("double"))
*/
    CustomerAccountOutput.show(truncate = false)

    CustomerAccountOutput.printSchema()

    CustomerAccountOutput.
      coalesce(1).
      write.mode("overwrite").parquet("src/main/resources/data/output/Question1")
  spark.close()
  }
def loadDataFrame[T](path: String)(implicit spark: SparkSession, encoder: Encoder [T]): Dataset[T]={

  val loadDataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path)
    .as[T]

  loadDataFrame
}
  def createCustomerAccountDataFrame(implicit spark: SparkSession, accountDataFrame: Dataset[accountData], customerDataFrame: Dataset[customerData]): DataFrame={
    import spark.implicits._
    val numberAccounts = accountDataFrame
      .groupBy("customerID").agg(count("accountID").alias("numberAccounts"))

    val totalBalance = accountDataFrame
      .groupBy("customerID").agg(round(sum("balance"), 2).alias("totalBalance"))

    val accountArray = accountDataFrame
      .groupBy("customerID").agg(collect_list("accountID").as("accounts"))


    val joinedDataFrame = accountDataFrame.join(customerDataFrame, Seq("customerID"), "inner")
      .join(numberAccounts, Seq("customerID"), "inner")
      .join(totalBalance, Seq("customerID"), "inner")
      .join(accountArray, Seq("customerID"), "inner")
      .select(customerDataFrame("customerID"), customerDataFrame("forename"), customerDataFrame("surname"),
        accountArray("accounts"), totalBalance("totalBalance"), numberAccounts("numberAccounts"))
      .distinct()

    val CustomerAccountOutput = joinedDataFrame
      .withColumn("averageBalance", round($"totalBalance" / $"numberAccounts", 2).cast("double"))

    CustomerAccountOutput
  }

}