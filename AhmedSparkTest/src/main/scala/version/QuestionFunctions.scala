package version

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions._

class QuestionFunctions {
  def loadDataSet[T](path: String)(implicit spark: SparkSession, encoder: Encoder [T]): Dataset[T]={

    val loadDataSet = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv(path)
      .as[T]

    loadDataSet
  }

  def saveDataSet[T](path: String, dataSetToSave: Dataset[T])(encoder: Encoder [T]): Unit = {
    dataSetToSave.coalesce(1).write.mode("overwrite").parquet(path)
  }
  def createCustomerAccountDataSet(implicit spark: SparkSession, accountDataSet: Dataset[accountData], customerDataSet: Dataset[customerData]): Dataset[customerAccountData]={
    import spark.implicits._
    val numberAccounts = accountDataSet
      .groupBy("customerID").agg(count("accountID").alias("numberAccounts"))

    val totalBalance = accountDataSet
      .groupBy("customerID").agg(round(sum("balance"), 2).alias("totalBalance"))

    val accountArray = accountDataSet
      .groupBy("customerID").agg(collect_list("accountID").as("accounts"))

    val joinedDataSet = accountDataSet.join(customerDataSet, Seq("customerID"), "inner")
      .join(numberAccounts, Seq("customerID"), "inner")
      .join(totalBalance, Seq("customerID"), "inner")
      .join(accountArray, Seq("customerID"), "inner")
      .select(customerDataSet("customerID"), customerDataSet("forename"), customerDataSet("surname"),
        accountArray("accounts"), totalBalance("totalBalance"), numberAccounts("numberAccounts"))
      .distinct()

    val CustomerAccountOutput = joinedDataSet
      .withColumn("averageBalance", round($"totalBalance" / $"numberAccounts", 2).cast("double"))
      .as[customerAccountData]

    CustomerAccountOutput
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
