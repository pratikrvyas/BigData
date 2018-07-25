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
import com.emirates.helix.model.Model._
import com.emirates.helix.DmisFormatIncrSchemaProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}

/**
* Test class file
*/
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DmisIncrFormatTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: DmisFormatIncrSchemaProcessor = _
  var in_df: DataFrame = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: SQLContext = new SQLContext(_sc)
  import _sqlContext.implicits._

  /**
    * Preparing the tests
    */
  before{
    processor = DmisFormatIncrSchemaProcessor(DMISIncrFrmtArgs(in_format = "avro", in_path = "./src/test/resources/part-r-00000.avro"))
    in_df = processor.readValueFromHDFS("./src/test/resources/part-r-00000.avro","avro",_sqlContext)
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(in_df).count()
    assert(actual === 779L)
  }

  /**
    * Test case 2 : Validate helix uuid available in processed data
    */
  test("Test2 : Check for correct helix uuid value"){
    val actual: String = processor.processData(in_df).orderBy(asc("HELIX_UUID"))
      .select($"HELIX_UUID").first().get(0).toString
    assert(actual === "01745248-d0da-43e6-b89d-3a5aa7ce8a80")
  }

  /**
    * Test case 3 : Validate exception is thrown for removed columns
    */
  test("Test3 : Verify exception for columns removed") {
    assertThrows[AnalysisException] {
      processor.processData(in_df).select($"FlightInfo.flightRemarks")
    }
    assertThrows[AnalysisException] {
      processor.processData(in_df).select($"FlightInfo.gateNumber._ATTRIBUTE_VALUE")
    }
  }
}
