/*----------------------------------------------------------
 * Created on  : 04/02/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AlteaLdmSparkTest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.emfg.model.Model._
import com.emirates.helix.emfg.AltealdmPrereqProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith

/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AlteaLdmPrereqTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: AltealdmPrereqProcessor = _
  var in_df: DataFrame = _
  implicit lazy val _spark : SparkSession =  SparkSession.builder().appName("AlteaLDMPrereqTest").getOrCreate()
  import _spark.implicits._
  override implicit def reuseContextIfPossible: Boolean = true

  /**
    * Preparing the tests
    */
  before {
    processor = AltealdmPrereqProcessor(AltLdmArgs())
    in_df = processor.readValueFromHDFS("./src/test/resources/part-00000.avro","avro")
  }

  /**
    * Test case 1 : Validate the record count for processed data
    */
  test("Test1 : Validate the output row count"){
    val actual: Long = processor.processData(in_df).count()
    assert(actual === 118L)
  }

  /**
    * Test case 2 : Validate TIBCO timestamp available in processed data
    */
  test("Test2 : Check for correct TIBCO timestamp value"){
    val actual: String = processor.processData(in_df).where($"flight_no" === "EK0795" && $"flight_date" === "2018-04-20" && $"dep_station" === "DXB" && $"arr_station" === "CKY")
      .select($"tibco_messageTime").first.getAs[String]("tibco_messageTime")
    assert(actual === "2018-04-20T08:54:04.534+04:00")
  }

  /**
    * Test case 3 : Validate load components Leg1 circular flight
    */
  test("Test3 : Validate load components Leg1 circular flight"){
    val out_df = processor.processData(in_df).where($"flight_no" === "EK0795" && $"flight_date" === "2018-04-20" && $"dep_station" === "DXB" && $"arr_station" === "CKY")
    val baggage_weight_actual: Double = out_df.first.getAs[Double]("baggage_weight")
    val cargo_weight_actual: Double = out_df.first.getAs[Double]("cargo_weight")
    val mail_weight_actual: Double = out_df.first.getAs[Double]("mail_weight")
    val transit_weight_actual: Double = out_df.first.getAs[Double]("transit_weight")
    val miscl_weight_actual: Double = out_df.first.getAs[Double]("miscl_weight")
    assert(baggage_weight_actual === 7991.0)
    assert(cargo_weight_actual === 10407.0)
    assert(mail_weight_actual === 488.0)
    assert(transit_weight_actual === 0.0)
    assert(miscl_weight_actual === 1794.0)
  }

  /**
    * Test case 4 : Validate load components Leg2 circular flight
    */
  test("Test4 : Validate load components Leg2 circular flight"){
    val out_df = processor.processData(in_df).where($"flight_no" === "EK0795" && $"flight_date" === "2018-04-20" && $"dep_station" === "CKY" && $"arr_station" === "DSS")
    val baggage_weight_actual: Double = out_df.first.getAs[Double]("baggage_weight")
    val cargo_weight_actual: Double = out_df.first.getAs[Double]("cargo_weight")
    val mail_weight_actual: Double = out_df.first.getAs[Double]("mail_weight")
    val transit_weight_actual: Double = out_df.first.getAs[Double]("transit_weight")
    val miscl_weight_actual: Double = out_df.first.getAs[Double]("miscl_weight")
    assert(baggage_weight_actual === 7835.0)
    assert(cargo_weight_actual === 8396.0)
    assert(mail_weight_actual === 488.0)
    assert(transit_weight_actual === 13110.0)
    assert(miscl_weight_actual === 1794.0)
  }

  /**
  * Test case 5 : Validate load components Leg3 circular flight
    */
  test("Test5 : Validate load components Leg3 circular flight"){
    val out_df = processor.processData(in_df).where($"flight_no" === "EK0795" && $"flight_date" === "2018-04-20" && $"dep_station" === "DSS" && $"arr_station" === "DXB")
    val baggage_weight_actual: Double = out_df.first.getAs[Double]("baggage_weight")
    val cargo_weight_actual: Double = out_df.first.getAs[Double]("cargo_weight")
    val mail_weight_actual: Double = out_df.first.getAs[Double]("mail_weight")
    val transit_weight_actual: Double = out_df.first.getAs[Double]("transit_weight")
    val miscl_weight_actual: Double = out_df.first.getAs[Double]("miscl_weight")
    assert(baggage_weight_actual === 5294.0)
    assert(cargo_weight_actual === 4434.0)
    assert(mail_weight_actual === 0.0)
    assert(transit_weight_actual === 6151.0)
    assert(miscl_weight_actual === 1739.0)
  }
}
