package questions
import org.jetbrains.annotations.TestOnly
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import version.{accountData, customerData, customerAccountData}
import version.QuestionFunctions
import org.apache.spark.sql.Encoders._

import scala.collection.mutable
class SparkTest extends AnyFunSuite with BeforeAndAfterAll {
  val helperFunc = new QuestionFunctions()
  /*
  @transient implicit var spark: SparkSession = _


  override def beforeAll(): Unit = {
     spark = SparkSession
      .builder
      .appName("Question1")
      .master("local[*]")
      .getOrCreate()
    val sparkSesh: SparkSession = spark
    import sparkSesh.implicits._
  }*/
  @transient lazy val spark = SparkSession
    .builder()
    .appName("Quest2ion1")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

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
    val sampleCustAccDF = helperFunc.createCustomerAccountDataSet(spark, sampleAccountDataFrame,sampleCustomerDataFrame)

    val expectedSchema = product[customerAccountData].schema
    val actualSchema = sampleCustAccDF.schema
    assert( expectedSchema.fieldNames.sameElements( actualSchema.fieldNames), "Names should match case class")
    for (fieldName <- expectedSchema.fieldNames){
      assert(expectedSchema(fieldName).dataType.typeName == actualSchema(fieldName).dataType.typeName, "Datatypes do not match")
    }
  }
}
