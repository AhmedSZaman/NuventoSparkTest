package version
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
/** Loads in filepath from config file and runs methods from QuestionFunctions class to give final answers
 *
 * WARNING: Build as sbt project
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


    println("STARTING QUESTION 1")
    import spark.implicits._
    val accountDataFrame = helperFunc.loadDataSet[accountData](accountDataPath)
    val customerDataFrame = helperFunc.loadDataSet[customerData](customerDataPath)
    val CustomerAccountOutput = helperFunc.createCustomerAccountDataSet(spark, accountDataFrame, customerDataFrame)
    CustomerAccountOutput.show(truncate = false)
    helperFunc.saveDataSet(customerAccountDataPath, CustomerAccountOutput)
    println("FINISHING QUESTION 1")

    println("STARTING QUESTION 2")
    val customerAccountDataSet = spark.read.parquet(customerAccountDataPath).as[customerAccountData]
    val addressDataSet = helperFunc.loadDataSet[addressData](addressDataPath)
    val customerDocumentDataSet = helperFunc.createCustomerDocumentDataSet(spark, addressDataSet, customerAccountDataSet)
    customerDocumentDataSet.show(truncate = false)
    helperFunc.saveDataSet(customerDocumentPath, customerDocumentDataSet)
    println("FINISHING QUESTION 2")
    spark.close()
  }
}
