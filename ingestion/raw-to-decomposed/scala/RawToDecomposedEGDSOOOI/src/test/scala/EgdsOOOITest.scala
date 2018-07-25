/*----------------------------------------------------------
 * Created on  : 21/12/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsOOOITest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.EgdsR2DProcessor
import com.emirates.helix.model.Model._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith

/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class EgdsOOOITest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var json_rdd: RDD[String] = _
  var processor: EgdsR2DProcessor = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: SQLContext = new SQLContext(_sc)
  import _sqlContext.implicits._

  /**
    * Preparing the tests
    */
  before{
    json_rdd = sc.textFile("./src/test/resources/part-r-00000")
    processor = EgdsR2DProcessor(Args())
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(json_rdd).count()
    assert(actual === 8L)
  }

  /**
    * Test case 2 : Check uniqueness of helix UUID
    */
  test("Test2 : Check uniqueness of helix UUID"){
    val actual: Long = processor.processData(json_rdd).select($"HELIX_UUID").distinct().count()
    assert(actual === 8L)
  }

  /**
    * Test case 3 : Validate TIBCO timestamp available in processed data
    */
  test("Test3 : Check for correct TIBCO timestamp value"){
    val actual: String = processor.processData(json_rdd).select($"tibco_messageTime").first().get(0).toString
    assert(actual === "2017-11-22T13:51:03.086+04:00")
  }

  /**
    * Test case 4 : Validate departure station in random message
    */
  test("Test4 baggage_details : Check for correct StaffNo"){
    val actual: String = processor.processData(json_rdd)
      .select($"dep_stn").take(4).last.get(0).toString
    assert(actual === "MEL")
  }
}
