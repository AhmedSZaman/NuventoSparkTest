package org.nuvento.exam

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.nuvento.exam.model.{accountModel, addressModel, customerAccountModel, customerDocumentModel, customerModel, parsedAddressModel}
/** All the main functions for joining datasets
 */
class QuestionFunctions {

  /**Loads data from specified file path to a dataset with a chosen schema
   *
   * @param path - path to load dataset
   * @param spark - spark session
   * @param encoder - Specify how the dataset should be encoded
   */
  def loadDataSet[T](path: String)(implicit spark: SparkSession, encoder: Encoder [T]): Dataset[T]={

    val loadDataSet = spark.read
      .option("header", "true")
      .option("sep", ",")
      .schema(encoder.schema)
      .csv(path)
      .as[T]

    loadDataSet
  }

  /** saves the specified dataset to a parquet file
   *
   * @param path - path to save dataset
   * @param dataSetToSave - data that is being saved
   */
  def saveDataSet[T](path: String, dataSetToSave: Dataset[T]): Unit = {
    dataSetToSave.coalesce(1).write.mode("overwrite").parquet(path)
  }

  /** Joins the account data and customer dataset (Question1)
   *
   * @param customerAccountDataSetVariables -
   * @return the final Customer Account Dataset
   */
  def joinCustomerAccountDataSet(customerAccountDataSetVariables: (SparkSession, Dataset[accountModel], Dataset[customerModel],
    DataFrame, DataFrame, DataFrame, DataFrame)): Dataset[customerAccountModel] = {

    val (spark, accountDataSet, customerDataSet, numberAccounts, totalBalance, accountArray, averageBalance) = customerAccountDataSetVariables

    import spark.implicits._
    val customerAccountDataSet = accountDataSet.join(customerDataSet, Seq("customerID"), "inner")
      .join(accountArray, Seq("customerID"), "inner")
      .join(numberAccounts, Seq("customerID"), "inner")
      .join(totalBalance, Seq("customerID"), "inner")
      .join(averageBalance, Seq("customerID"), "inner")
      .select(accountDataSet("customerID"), customerDataSet("forename"), customerDataSet("surname"),
        accountArray("accounts"), totalBalance("totalBalance"), numberAccounts("numberAccounts"), averageBalance("averageBalance"))
      .distinct()
      .as[customerAccountModel]


    customerAccountDataSet
  }

  /**generates the average balance for each customer
   *
   * @param spark  spark session
   * @param numberAccounts - dataset of the total number of accounts for each customer
   * @param totalBalance  - dataset of total balance for all accounts for each customer
   * @return average balance dataset
   */
  def getAverageBalance(implicit spark: SparkSession, numberAccounts: Dataset[Row], totalBalance: Dataset[Row]) = {
    import spark.implicits._
    val averageBalance = numberAccounts.join(totalBalance, Seq("customerID"), "inner")
      .withColumn("averageBalance", round($"totalBalance" / $"numberAccounts", 2).cast("double"))
      .select(("customerID"),("averageBalance"))

    averageBalance
  }

  /** gets all accounts linked to a customer and puts it in an array
   *
   * @param accountDataSet - dataset of accounts information
   * @return a dataset of all customer accounts in an array
   */
  def getAccountArray(accountDataSet: Dataset[accountModel]) = {
    val accountArray = accountDataSet
      .groupBy("customerID").agg(collect_list("accountID").as("accounts"))
      .select("customerID", "accounts")

    accountArray
  }

  /** gets the dataset of total balance for all accounts for each customer
   *
   * @param accountDataSet  - dataset of accounts information
   * @return  dataset of total balance for all accounts for each customer
   */
  def getTotalBalance(accountDataSet: Dataset[accountModel]): DataFrame = {
    val totalBalance = accountDataSet
      .groupBy("customerID").agg(round(sum("balance"), 2).alias("totalBalance"))
      .select("customerID", "totalBalance")

    totalBalance
  }

  /** gets dataset of the total number of accounts for each customer
   *
   * @param accountDataSet- dataset of accounts information
   * @return dataset of the total number of accounts for each customer
   */
  def getNumberAccounts(accountDataSet: Dataset[accountModel]) = {
    val numberAccounts = accountDataSet
      .groupBy("customerID").agg(count("accountID").alias("numberAccounts"))
      .select("customerID","numberAccounts")

    numberAccounts
  }

  /** joins the final customer document dataset
   *
   * @param spark  spark session
   * @param customerAccountDataSet  dataset of customer Account details
   * @param parsedAddressDataSet  dataset of address parsed in separate columns, see parsedAddressModel for more
   * @return the final customer document dataset
   */
   def joinCustomerDocumentDataSet(implicit spark: SparkSession, customerAccountDataSet: Dataset[customerAccountModel],
                                          parsedAddressDataSet: Dataset[parsedAddressModel]) = {
    import spark.implicits._
    val customerDocumentDataSet = parsedAddressDataSet.join(customerAccountDataSet, Seq("customerID"))
      .select(parsedAddressDataSet("customerID"), customerAccountDataSet("forename"), customerAccountDataSet("surname"),
        customerAccountDataSet("accounts"), parsedAddressDataSet("streetNumber"), parsedAddressDataSet("streetName"),
        parsedAddressDataSet("city"), parsedAddressDataSet("country"))
      .as[customerDocumentModel]
     customerDocumentDataSet
  }

  /** parses the address field into a new dataset
   *
   * @param spark  spark session
   * @param addressDataSet  dataset of unparsed address
   * @return  dataset of address parsed in separate columns, see parsedAddressModel for more
   */
   def getParsedAddress(implicit spark: SparkSession, addressDataSet: Dataset[addressModel])   = {
    import spark.implicits._
    val parsedAddressDataSet = addressDataSet
      .select("address", "customerID")
      .withColumn("streetNumber", split(col("address"), ",")(0))
      .withColumn("streetName", split(col("address"), ",")(1))
      .withColumn("city", split(col("address"), ",")(2))
      .withColumn("country", split(col("address"), ",")(3))
      .select("customerID", "streetNumber", "streetName","city", "country" )
      .as[parsedAddressModel]
    parsedAddressDataSet
  }
}