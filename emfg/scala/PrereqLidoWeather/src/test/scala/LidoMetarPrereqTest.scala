/*----------------------------------------------------------
 * Created on  : 01/22/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DmisIncrFormatTest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.emirates.helix.emfg.model.Model._
import com.emirates.helix.emfg.LidoMetarPrereqProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}

/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class LidoMetarPrereqTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: LidoMetarPrereqProcessor = _
  var in_df: DataFrame = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: SQLContext = new SQLContext(_sc)
  import _sqlContext.implicits._

  /**
    * Preparing the tests
    */
  before{
    processor = LidoMetarPrereqProcessor(LidoMetarPrereqArgs(in_format = "avro", in_path = "./src/test/resources/part-r-00000.avro"))
    in_df = processor.readValueFromHDFS("./src/test/resources/part-r-00000.avro","avro")
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(in_df).count()
    assert(actual === 1640L)
  }

  /**
    * Test case 2 : Validate helix uuid available in processed data
    */
  test("Test2 : Check for correct helix uuid value"){
    val actual: String = processor.processData(in_df).orderBy(asc("helix_uuid"))
      .select($"helix_uuid").first().get(0).toString
    assert(actual === "18bee652-a23e-4a3d-bb2e-5eeafc26be93")
  }

  /**
    * Test case 3 : Validate proper datetime is created from metar message
    */
  test("Test3 : Validate proper metar datetime") {
    val actual: String = processor.processData(in_df).orderBy(asc("helix_uuid")).where($"TimeofObservationPromulgation" === "200520" &&
      $"tibco_message_time" === "2017-11-22T10:51:12.854+04:00").select($"messagetime").first().get(0).toString
    assert(actual === "2017-11-20 05:20:00")
  }

  /**
    * Test case 3 : Validate proper message is created
    */
  test("Test3 : Validate proper metar message") {
    val actual1: String = processor.processData(in_df).select($"messagetext").take(10).last.get(0).toString
    val actual2: String = processor.processData(in_df).select($"messagetext").take(30).last.get(0).toString
    val actual3: String = processor.processData(in_df).select($"messagetext").take(55).last.get(0).toString
    assert(! actual1.endsWith("="))
    assert(! actual2.endsWith("="))
    assert(! actual3.endsWith("="))
  }
}
