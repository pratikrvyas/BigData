/*----------------------------------------------------------
 * Created on  : 07/12/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : MFPFuelReasonTest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.model.Model._
import com.emirates.helix.RawCSVProcessor
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.junit.runner.RunWith

/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class MFPFuelReasonTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: RawCSVProcessor = _
  var in_df: DataFrame = _
  implicit lazy val sqlContext: SQLContext = new SQLContext(sc)
  import sqlContext.implicits._

  /**
    * Preparing the tests
    */
  before {
    processor = RawCSVProcessor(Args())
    in_df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true")
      .load("./src/test/resources/MFPFuelReasonMaster.csv")
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(in_df).count()
    assert(actual === 5L)
  }

  /**
    * Test case 2 : Check uniqueness of helix UUID
    */
  test("Test2 : Check uniqueness of helix UUID"){
    val actual = processor.processData(in_df).select($"HELIX_UUID").distinct().count()
    assert(actual === 5L)
  }

  /**
    * Test case 4 : Validate Reason code
    */
  test("Test4 : Validation of message fields"){
    val actual1 = processor.processData(in_df).select($"Reason_Code").take(5).last.get(0).toString
    val actual2 = processor.processData(in_df).select($"Reason_Master").take(2).last.get(0).toString
    assert(actual1 === "OTH")
    assert(actual2 === "WEATHER")
  }
}
