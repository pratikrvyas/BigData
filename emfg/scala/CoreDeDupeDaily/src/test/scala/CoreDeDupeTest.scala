/*----------------------------------------------------------
 * Created on  : 01/31/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : CoreDedupeProcessor.scala
 * Description : Core DeDupe processing Test
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.CoreDedupeProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.junit.runner.RunWith
import com.emirates.helix.corededupearguments.CoreDeDupeArgs._
/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class CoreDeDupeTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: CoreDedupeProcessor = _
  var core_df_union : DataFrame = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: HiveContext = new HiveContext(sc)
  import _sqlContext.implicits._
  processor = CoreDedupeProcessor(Args
  (core_dedupe_current_location = "./src//test/resources/part-r-00000-3dd17a69-f457-4469-917e-8fd6570dc6a2.avro",
    core_incremental_location = "./src//test/resources/part-r-00000-3dd17a69-f457-4469-917e-8fd6570dc6a2.avro"))

  /**
    * Preparing the tests
    */
  before{
   core_df_union = _sqlContext.read.format("com.databricks.spark.avro")
     .load("./src//test/resources/part-r-00001-3cecbedd-103f-41e4-b8f2-f7c2c75dacf3.avro")
  }

  /**
    * Test case 1 : Compare the
    */
  test("Test1 : Compare input and output count "){
    val actual: Long = processor.deDupeCore(core_df_union).count()
    assert(actual === 37L)
  }

}
