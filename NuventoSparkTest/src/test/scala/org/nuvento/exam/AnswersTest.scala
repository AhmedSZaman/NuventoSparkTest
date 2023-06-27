package org.nuvento.exam

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class AnswersTest extends AnyFunSuite with BeforeAndAfterAll {
  val helperFunc = new QuestionFunctions()
  Logger.getLogger("org").setLevel(Level.ERROR)

  val accountDataPath: String = "src/test/resources/sampleAccountData.txt"
  val customerDataPath: String = "src/test/resources/sampleCustomerData.txt"
  val customerAccountDataPath: String = "src/test/resources/sampleCustomerAccountData"
  val addressDataPath: String = "src/test/resources/sampleAddressData.txt"
  val customerDocumentPath: String = "src/test/resources/sampleCustomerDocument"

  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("AnswersTests")
    .master("local[*]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("executeQuestion1") {
    val question1Variables = (spark, accountDataPath, customerDataPath, customerAccountDataPath, helperFunc)
    Answers.executeQuestion1(question1Variables)
    val outputFile = new File(customerAccountDataPath)
    assert(outputFile.exists(), s"Output file '$customerAccountDataPath' does not exist")
  }


  test("executeQuestion2") {
    val question2Variables = (spark, customerAccountDataPath, addressDataPath, customerDocumentPath, helperFunc)
    Answers.executeQuestion2(question2Variables)
    val outputFile = new File(customerDocumentPath)
    assert(outputFile.exists(), s"Output file '$customerDocumentPath' does not exist")
  }
}