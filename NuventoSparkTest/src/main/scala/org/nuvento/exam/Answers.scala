package org.nuvento.exam

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.nuvento.exam.model.{accountModel, addressModel, customerAccountModel, customerDocumentModel, customerModel, parsedAddressModel}
/** Loads in filepath from config file and runs methods from QuestionFunctions class to give final answers
 *
 * @author Ahmed Sher Zaman
 * @version scala version 2.12.10
 */

object Answers {
  def main(args: Array[String]): Unit = {
    val helperFunc = new QuestionFunctions()
    val config: Config = ConfigFactory.load()

    // Loads variables from config file, located in resources/application.conf
    val accountDataPath: String = config.getString("accountDataPath")
    val customerDataPath: String = config.getString("customerDataPath")
    val customerAccountDataPath: String = config.getString("customerAccountDataPath")
    val addressDataPath: String = config.getString("addressDataPath")
    val customerDocumentPath: String = config.getString("customerDocumentPath")
    Logger.getLogger("org").setLevel(Level.ERROR)


    // Create a SparkSession using every core of the local machine
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("Answers")
      .master("local[*]")
      .getOrCreate()


    Logger.getLogger("org").log(Level.DEBUG, "Starting Question 1")
    val question1Variables = (spark, accountDataPath, customerDataPath, customerAccountDataPath, helperFunc)
    executeQuestion1(question1Variables)
    Logger.getLogger("org").log(Level.DEBUG, "Finishing Question 1")

    Logger.getLogger("org").log(Level.DEBUG, "Starting Question 2")
    val question2Variables = (spark, customerAccountDataPath, addressDataPath, customerDocumentPath, helperFunc)
    executeQuestion2(question2Variables)
    Logger.getLogger("org").log(Level.DEBUG, "Finishing Question 2")

    spark.close()
  }

  /**Question 1 driver method
   *
   * @param question1Variables  curried variable consisting of spark session, file paths, and helper function class
   *
   */
  def executeQuestion1(question1Variables: (SparkSession, String,  String,
                               String,QuestionFunctions)): Unit  = {
    val  (spark, accountDataPath, customerDataPath, customerAccountDataPath, helperFunc)= question1Variables
    import spark.implicits._

    implicit val implicitSpark: SparkSession = spark

    val accountDataSet = helperFunc.loadDataSet[accountModel](accountDataPath)
    val customerDataSet = helperFunc.loadDataSet[customerModel](customerDataPath)

    val numberAccounts: DataFrame = helperFunc.getNumberAccounts(accountDataSet)
    val totalBalance: DataFrame = helperFunc.getTotalBalance(accountDataSet)
    val accountArray: DataFrame = helperFunc.getAccountArray(accountDataSet)
    val averageBalance: DataFrame = helperFunc.getAverageBalance(spark: SparkSession, numberAccounts, totalBalance)

    val customerAccountDataSetVariables = (spark, accountDataSet, customerDataSet, numberAccounts, totalBalance, accountArray, averageBalance)
    val customerAccountDataSet:Dataset[customerAccountModel] = helperFunc.joinCustomerAccountDataSet(customerAccountDataSetVariables)

    helperFunc.saveDataSet(customerAccountDataPath, customerAccountDataSet)
  }

  /**Question 2 driver method
   *
   * @param question2Variables curried variable consisting of spark session, file paths, and helper function class
   */
  def executeQuestion2(question2Variables: (SparkSession, String, String, String, QuestionFunctions)): Unit = {
    val  (spark, customerAccountDataPath, addressDataPath, customerDocumentPath, helperFunc)= question2Variables
    import spark.implicits._
    implicit val implicitSpark: SparkSession = spark

    val customerAccountDataSet = spark.read.parquet(customerAccountDataPath).as[customerAccountModel]
    val addressDataSet = helperFunc.loadDataSet[addressModel](addressDataPath)

    val parsedAddressDataSet: Dataset[parsedAddressModel] = helperFunc.getParsedAddress(spark: SparkSession, addressDataSet)
    val customerDocumentDataSet: Dataset[customerDocumentModel] = helperFunc.joinCustomerDocumentDataSet(spark, customerAccountDataSet, parsedAddressDataSet)

    helperFunc.saveDataSet(customerDocumentPath, customerDocumentDataSet)
  }
}



