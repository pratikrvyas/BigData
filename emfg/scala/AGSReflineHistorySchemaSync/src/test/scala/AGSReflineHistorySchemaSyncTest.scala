/*----------------------------------------------------------
 * Created on  : 01/04/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSReflineHistorySchemaSyncTest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.AgsReflineHistorySchemaSyncProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.runner.RunWith
import com.emirates.helix.agsreflineschemasyncarguments.AGSReflineHistorySchemaSyncArgs._
/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AGSReflineHistorySchemaSyncTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: AgsReflineHistorySchemaSyncProcessor = _
  var df_hist : DataFrame = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: SQLContext = new SQLContext(_sc)
  import _sqlContext.implicits._
  processor = AgsReflineHistorySchemaSyncProcessor(Args(ags_src_refline_history_location = "./src//test/resources/part-r-00001-6dac0fb4-7c3a-4fbd-8e5e-211784b4fd44.avro"))

  /**
    * Preparing the tests
    */
  before{
   df_hist = _sqlContext.read.format("com.databricks.spark.avro").load("./src//test/resources/part-r-00001-6dac0fb4-7c3a-4fbd-8e5e-211784b4fd44.avro")
  }

  /**
    * Test case 1 : Compare the
    */
  test("Test1 : Compare input and output count "){
    val actual: Long = processor.syncAGSRefline(df_hist).count()
    assert(actual === 4L)
  }

}
