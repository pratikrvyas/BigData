/*----------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSReflineDeDupeTest.scala
 * Description : AGS Refline DeDupe processing Test
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.AgsReflineDedupeProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.junit.runner.RunWith
import com.emirates.helix.agsreflinesdedupearguments.AGSReflineDeDupeArgs._
/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AGSReflineDeDupeTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: AgsReflineDedupeProcessor = _
  var ags_df_union : DataFrame = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: HiveContext = new HiveContext(sc)
  import _sqlContext.implicits._
  processor = AgsReflineDedupeProcessor(Args
  (ags_refline_dedupe_current_location = "./src//test/resources/part-r-00001-6dac0fb4-7c3a-4fbd-8e5e-211784b4fd44.avro",
    ags_src_refline_incremental_location = "./src//test/resources/part-r-00001-6dac0fb4-7c3a-4fbd-8e5e-211784b4fd44.avro"))

  /**
    * Preparing the tests
    */
  before{
   ags_df_union = _sqlContext.read.format("com.databricks.spark.avro")
     .load("./src//test/resources/part-r-00001-3cecbedd-103f-41e4-b8f2-f7c2c75dacf3.avro")
     .withColumnRenamed("Date","FltDate")
  }

  /**
    * Test case 1 : Compare the
    */
  test("Test1 : Compare input and output count "){
    val actual: Long = processor.deDupeAGSRefline(ags_df_union).count()
    assert(actual === 1L)
  }

}
