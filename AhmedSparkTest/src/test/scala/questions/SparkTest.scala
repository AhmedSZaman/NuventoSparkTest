package questions
import org.apache.log4j.{Level, Logger}
import org.jetbrains.annotations.TestOnly
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import version._
import version.QuestionFunctions
import org.apache.spark.sql.Encoders._

import scala.collection.mutable
class SparkTest extends AnyFunSuite with BeforeAndAfterAll {
  val helperFunc = new QuestionFunctions()

  @transient lazy val spark = SparkSession
    .builder()
    .appName("Quest2ion1")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.ERROR)
  override def afterAll(): Unit={
      spark.stop()
  }

  test("Load Data"){
    val sampleDF = helperFunc.loadDataSet[accountData]("src/test/resources/sampleAccountData.txt")(spark, product[accountData])
    val rCount = sampleDF.count()
    assert(rCount == 4, "record count should be 4")
  }
  test("Create Customer Account DataSet") {
    val sampleAccountDataFrame = helperFunc.loadDataSet[accountData]("src/test/resources/sampleAccountData.txt")(spark, product[accountData])
    val sampleCustomerDataFrame = helperFunc.loadDataSet[customerData]("src/test/resources/sampleCustomerData.txt")(spark, product[customerData])
    val sampleCustAccDF = helperFunc.createCustomerAccountDataSet(spark, sampleAccountDataFrame, sampleCustomerDataFrame)

    val expectedSchema = product[customerAccountData].schema
    val actualSchema = sampleCustAccDF.schema
    assert(expectedSchema.fieldNames.sameElements(actualSchema.fieldNames), "Names should match case class")
    for (fieldName <- expectedSchema.fieldNames) {
      assert(expectedSchema(fieldName).dataType.typeName == actualSchema(fieldName).dataType.typeName, "Datatypes do not match")
    }
  }
  test("Save Data"){
    val sampleAccountDataFrame = helperFunc.loadDataSet[accountData]("src/test/resources/sampleAccountData.txt")(spark, product[accountData])
    val sampleCustomerDataFrame = helperFunc.loadDataSet[customerData]("src/test/resources/sampleCustomerData.txt")(spark, product[customerData])
    val testData = helperFunc.createCustomerAccountDataSet(spark, sampleAccountDataFrame, sampleCustomerDataFrame)
    helperFunc.saveDataSet("src/test/resources/sampleCustomerAccountData", testData)(product[customerAccountData])
    val savedData = spark.read.parquet("src/test/resources/sampleCustomerAccountData").as[customerAccountData]

    for (fieldName <- testData.schema.fieldNames) {
      assert(testData.schema(fieldName).dataType.typeName == savedData.schema(fieldName).dataType.typeName, "Datatypes do not match")
    }
}
  test("Create Customer Account Document") {
    val customerAccountDataSet = spark.read.parquet("src/test/resources/sampleCustomerAccountData").as[customerAccountData]
    val addressDataSet = helperFunc.loadDataSet[addressData]("src/test/resources/sampleAddressData.txt")(spark, product[addressData])
    val customerDocumentDataSet = helperFunc.createCustomerDocumentDataSet(spark, addressDataSet, customerAccountDataSet)

    val expectedSchema = product[customerDocumentData].schema
    val actualSchema = customerDocumentDataSet.schema

    assert(expectedSchema.fieldNames.sameElements(actualSchema.fieldNames), "Names should match case class")
    for (fieldName <- expectedSchema.fieldNames) {
      assert(expectedSchema(fieldName).dataType.typeName == actualSchema(fieldName).dataType.typeName, "Datatypes do not match")
    }
  }
}
