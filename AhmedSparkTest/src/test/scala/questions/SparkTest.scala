package questions
import org.jetbrains.annotations.TestOnly

import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import version.accountData
import version.Question1
class SparkTest extends AnyFunSuite with BeforeAndAfterAll {
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
    .appName("Question1")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  override def afterAll(): Unit={
      spark.stop()
  }

  test("Load Data"){
    val sampleDF = Question1.loadDataFrame[accountData]("src/test/resources/sampleAccountData.txt")(spark, org.apache.spark.sql.Encoders.product[accountData])
    val rCount = sampleDF.count()
    assert(rCount == 4, "record count should be 4")
  }
  test("Create Customer Account DataSet") {
    val sampleDF = Question1.loadDataFrame[accountData]("src/test/resources/sampleAccountData.txt")(spark, org.apache.spark.sql.Encoders.product[accountData])
    val rCount = sampleDF.count()
    assert(rCount == 3, "record count should be 3")
  }
}
