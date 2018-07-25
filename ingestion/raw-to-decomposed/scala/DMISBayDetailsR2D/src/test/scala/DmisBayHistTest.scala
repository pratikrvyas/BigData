/*----------------------------------------------------------
 * Created on  : 14/02/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DmisBayHistTest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.DmisBayHistoryR2DProcessor
import com.emirates.helix.model.Model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.runner.RunWith

/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DmisBayHistTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var in_df: DataFrame = _
  var processor: DmisBayHistoryR2DProcessor = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: SQLContext = new SQLContext(_sc)
  import _sqlContext.implicits._

  /**
    * Preparing the tests
    */
  before{
    processor = DmisBayHistoryR2DProcessor(XmlR2DArgs())
    in_df = _sqlContext.read.json(sc.textFile("./src/test/resources/part-r-00000.txt"))
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(in_df).count()
    assert(actual === 10L)
  }

  /**
    * Test case 2 : Check uniqueness of helix UUID
    */
  test("Test2 : Check uniqueness of helix UUID"){
    val actual: Long = processor.processData(in_df).select($"HELIX_UUID").distinct().count()
    assert(actual === 10L)
  }

  /**
    * Test case 3 : Validate TIBCO timestamp available in processed data
    */
  test("Test3 : Check for correct TIBCO timestamp value"){
    val actual: String = processor.processData(in_df).select($"tibco_messageTime").first().get(0).toString
    assert(actual === "2018-02-12T16:25:17.552+04:00")
  }

  /**
    * Test case 4 : Validate departure station in random message
    */
  test("Test4 baggage_details : Check for correct StaffNo"){
    val actual: String = processor.processData(in_df)
      .select($"route").take(4).last.get(0).toString
    assert(actual === "PER")
  }
}
