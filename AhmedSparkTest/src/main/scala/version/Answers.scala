package version
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Answers {
  def main(args: Array[String]): Unit = {
    val helperFunc = new QuestionFunctions()

    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkSession using every core of the local machine
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("Answers")
      .master("local[*]")
      .getOrCreate()

    println("STARTING QUESTION 1")
    import spark.implicits._
    val accountDataFrame = helperFunc.loadDataSet[accountData]("src/main/resources/data/input/account_data.txt")
    val customerDataFrame = helperFunc.loadDataSet[customerData]("src/main/resources/data/input/customer_data.txt")
    val CustomerAccountOutput = helperFunc.createCustomerAccountDataSet(spark, accountDataFrame, customerDataFrame)
    CustomerAccountOutput.show(truncate = false)
    helperFunc.saveDataSet("src/main/resources/data/output/Question1", CustomerAccountOutput)
    println("FINISHING QUESTION 1")

    println("STARTING QUESTION 2")
    val customerAccountDataSet = spark.read.parquet("src/main/resources/data/output/Question1").as[customerAccountData]
    val addressDataSet = helperFunc.loadDataSet[addressData]("src/main/resources/data/input/address_data.txt")
    val customerDocumentDataSet = helperFunc.createCustomerDocumentDataSet(spark, addressDataSet, customerAccountDataSet)
    customerDocumentDataSet.show(truncate = false)
    helperFunc.saveDataSet("src/main/resources/data/output/Question2", customerDocumentDataSet)
    spark.close()
  }
}
