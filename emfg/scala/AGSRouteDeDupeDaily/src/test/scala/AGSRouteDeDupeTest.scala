/*----------------------------------------------------------
 * Created on  : 02/19/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSRouteDeDupeTest.scala
 * Description : AGS Route DeDupe processing Test
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import com.emirates.helix.AgsRouteDedupeProcessor
import com.emirates.helix.agsroutededupearguments.AGSRouteDeDupeArgs._
/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AGSRouteDeDupeTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: AgsRouteDedupeProcessor = _
  var ags_df_union : DataFrame = _
  implicit lazy val _spark : SparkSession =  SparkSession.builder().appName("AgsRouteDedupeProcessor").getOrCreate()
  import _spark.implicits._
  override implicit def reuseContextIfPossible: Boolean = true
  processor = AgsRouteDedupeProcessor(Args
  (ags_src_route_dedupe_current_location = "./src//test/resources/part-r-00000-8e4ce4ef-aeb1-4f7f-bf48-7fafc502b96b.snappy.parquet",
    ags_src_route_incremental_location = "./src//test/resources/part-r-00000-8e4ce4ef-aeb1-4f7f-bf48-7fafc502b96b.snappy.parquet"))

  /**
    * Preparing the tests
    */
  /**
    * Preparing the tests
    */
  before{
    ags_df_union = _spark.read.format("parquet")
      .load("./src//test/resources/part-r-00000-8e4ce4ef-aeb1-4f7f-bf48-7fafc502b96b.snappy.parquet")
  }

  /**
    * Test case 1 : Compare the
    */
  test("Test1 : Compare input and output count "){
    val actual: Long = processor.deDupeAGSRoute(ags_df_union).count()
    assert(actual === 11278L)
  }

}
