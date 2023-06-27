package org.nuvento.exam

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.SparkSession
import org.nuvento.exam.model.{accountModel, addressModel, customerAccountModel, customerDocumentModel, customerModel, parsedAddressModel}
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class QuestionFunctionTest extends AnyFunSuite with BeforeAndAfterAll {
  val helperFunc = new QuestionFunctions()
  val accountDataSet = helperFunc.loadDataSet[accountModel]("src/test/resources/sampleAccountData.txt")(spark, product[accountModel])
  val customerDataSet = helperFunc.loadDataSet[customerModel]("src/test/resources/sampleCustomerData.txt")(spark, product[customerModel])
  Logger.getLogger("org").setLevel(Level.ERROR)

  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Question1")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._



  override def afterAll(): Unit = {
    spark.stop()
  }

  test("loadData") {
    val sampleDF = helperFunc.loadDataSet[accountModel]("src/test/resources/sampleAccountData.txt")(spark, product[accountModel])
    val rCount = sampleDF.count()
    assert(rCount == 4, "record count should be 4")
  }

  test("saveData") {
    helperFunc.saveDataSet("src/test/resources/sampleAccountData", accountDataSet)
    val savedData = spark.read.parquet("src/test/resources/sampleAccountData").as[accountModel]
    for (fieldName <- accountDataSet.schema.fieldNames) {
      assert(accountDataSet.schema(fieldName).dataType.typeName == savedData.schema(fieldName).dataType.typeName, "Data types do not match")
    }
  }

  test("joinCustomerAccountDataSet") {
    val numberAccounts = helperFunc.getNumberAccounts(accountDataSet)
    val totalBalance = helperFunc.getTotalBalance(accountDataSet)
    val accounts = helperFunc.getAccountArray(accountDataSet)
    val averageBalance = helperFunc.getAverageBalance(spark, totalBalance, numberAccounts)
    val customerAccountVariables = (spark, accountDataSet, customerDataSet, numberAccounts, totalBalance, accounts, averageBalance)
    val customerAccountDataSet = helperFunc.joinCustomerAccountDataSet(customerAccountVariables)

    val expectedSchema = product[customerAccountModel].schema
    val actualSchema = customerAccountDataSet.schema

    assert(expectedSchema.fieldNames.sameElements(actualSchema.fieldNames), "Names should match case class")
    for (fieldName <- expectedSchema.fieldNames) {
      assert(expectedSchema(fieldName).dataType.typeName == actualSchema(fieldName).dataType.typeName, "Data types do not match")
    }
  }

  test("joinCustomerDocumentDataSet") {
    val customerAccountDataSet = spark.read.parquet("src/test/resources/sampleCustomerAccountData").as[customerAccountModel]
    val addressDataSet = helperFunc.loadDataSet[addressModel]("src/test/resources/sampleAddressData.txt")(spark, product[addressModel])
    val parsedAddress = helperFunc.getParsedAddress(spark, addressDataSet)
    val customerDocumentDataSet = helperFunc.joinCustomerDocumentDataSet(spark, customerAccountDataSet, parsedAddress)

    val expectedSchema = product[customerDocumentModel].schema
    val actualSchema = customerDocumentDataSet.schema

    assert(expectedSchema.fieldNames.sameElements(actualSchema.fieldNames), "Names should match case class")
    for (fieldName <- expectedSchema.fieldNames) {
      assert(expectedSchema(fieldName).dataType.typeName == actualSchema(fieldName).dataType.typeName, "Data types do not match")
    }
  }

  test("getNumberAccounts") {
    val numberAccounts = helperFunc.getNumberAccounts(accountDataSet)

    val expectedColumns = Seq("customerID", "numberAccounts")
    val actualColumns = numberAccounts.columns

    assert(expectedColumns.sameElements(actualColumns), "Names should match case class")
  }

  test("getTotalBalance") {

    val totalBalance = helperFunc.getTotalBalance(accountDataSet)

    val expectedColumns = Seq("customerID", "totalBalance")
    val actualColumns = totalBalance.columns

    assert(expectedColumns.sameElements(actualColumns), "Names should match case class")
  }
  test("getAverageBalance") {
    val numberAccounts = helperFunc.getNumberAccounts(accountDataSet)
    val totalBalance = helperFunc.getTotalBalance(accountDataSet)
    val averageBalance = helperFunc.getAverageBalance(spark, totalBalance, numberAccounts)

    val expectedColumns = Seq("customerID", "averageBalance")
    val actualColumns = averageBalance.columns

    assert(expectedColumns.sameElements(actualColumns), "Names should match case class")
  }
  test("getAccountArray") {
    val accounts = helperFunc.getAccountArray(accountDataSet)

    val expectedColumns = Seq("customerID", "accounts")
    val actualColumns = accounts.columns

    assert(expectedColumns.sameElements(actualColumns), "Names should match case class")
  }
  test("getParsedAddress") {
    val addressDataSet = helperFunc.loadDataSet[addressModel]("src/test/resources/sampleAddressData.txt")(spark, product[addressModel])
    val parsedAddress = helperFunc.getParsedAddress(spark, addressDataSet)

    val expectedSchema = product[parsedAddressModel].schema
    val actualSchema = parsedAddress.schema

    assert(expectedSchema.fieldNames.sameElements(actualSchema.fieldNames), "Names should match case class")
    for (fieldName <- expectedSchema.fieldNames) {
      assert(expectedSchema(fieldName).dataType.typeName == actualSchema(fieldName).dataType.typeName, "Data types do not match")
    }
  }
}
