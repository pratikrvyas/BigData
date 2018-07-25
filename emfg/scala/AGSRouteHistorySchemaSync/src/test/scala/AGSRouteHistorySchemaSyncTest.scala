/*----------------------------------------------------------
 * Created on  : 02/14/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSRouteHistorySchemaSyncTest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.AgsRouteHistorySchemaSyncProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import com.emirates.helix.agsrouteschemasyncarguments.AGSRouteHistorySchemaSyncArgs._
/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AGSRouteHistorySchemaSyncTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: AgsRouteHistorySchemaSyncProcessor = _
  var df_hist : DataFrame = _
  implicit lazy val _spark : SparkSession =  SparkSession.builder().appName("AgsRouteHistorySchemaSync").getOrCreate()
  import _spark.implicits._
  override implicit def reuseContextIfPossible: Boolean = true
  processor = AgsRouteHistorySchemaSyncProcessor(Args(ags_src_route_history_location = "./src//test/resources/part-r-00000-3130332e-5f3f-4180-9e94-90327696805b.avro"))

  /**
    * Preparing the tests
    */
  before{
    df_hist = _spark.read.format("com.databricks.spark.avro").load("./src//test/resources/part-r-00000-3130332e-5f3f-4180-9e94-90327696805b.avro")
  }

  /**
    * Test case 1 : Compare input and output count
    */
  test("Test1 : Compare input and output count "){
    val actual: Long = processor.syncAGSRoute(df_hist).count()
    assert(actual === 3617L)
  }

}
