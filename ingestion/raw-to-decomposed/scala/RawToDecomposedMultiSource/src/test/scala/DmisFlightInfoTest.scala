/*----------------------------------------------------------
 * Created on  : 08/02/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : MultiSourceR2DTest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.model.Model._
import com.emirates.helix.MultiSourceR2DProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.runner.RunWith

/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DmisFlightInfoTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: MultiSourceR2DProcessor = _
  var in_df: DataFrame = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: SQLContext = new SQLContext(sc)
  import _sqlContext.implicits._

  /**
    * Preparing the tests
    */
  before {
    processor = MultiSourceR2DProcessor(R2DArgs())
    in_df = processor.readValueFromHDFS("./src/test/resources/Helix_Flights_Data_2017.csv", "csv")
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(in_df).count()
    assert(actual === 195148L)
  }

  /**
    * Test case 2 : Check uniqueness of helix UUID
    */
  test("Test2 : Check uniqueness of helix UUID"){
    val actual: Long = processor.processData(in_df).select($"HELIX_UUID").distinct().count()
    assert(actual === 195148L)
  }

  /**
    * Test case 3 : Validate Gate number available in processed data
    */
  test("Test3 : Check for correct gate number value"){
    val actual: String = processor.processData(in_df).select($"GATENUMBER").first().get(0).toString
    assert(actual === "B11")
  }

  /**
    * Test case 4 : Validate handling terminal number available in processed data
    */
  test("Test4 : Check for correct aircraft handling terminal"){
    val actual: String = processor.processData(in_df).select($"HANDLINGTERMINAL").take(7).last.get(0).toString
    assert(actual === "T3")
  }
}
