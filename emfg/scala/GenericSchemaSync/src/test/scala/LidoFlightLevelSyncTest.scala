/*----------------------------------------------------------
 * Created on  : 01/10/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : LidoFlightLevelSyncTest.scala
 * Description : File with unit test cases for validating processing logic
 * ----------------------------------------------------------
 */

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.emirates.helix.SchemaSyncProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.runner.RunWith
import com.emirates.helix.model.Model._
/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class LidoFlightLevelSyncTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: SchemaSyncProcessor = _
  var in_df: DataFrame = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: SQLContext = new SQLContext(_sc)
  import _sqlContext.implicits._

  /**
    * Preparing the tests
    */
  before{
    processor = SchemaSyncProcessor(SyncArgs(in_format = "avro", in_path = "./src/test/resources/part-r-00000.avro"))
    in_df = processor.readData
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(in_df,SparkSupport.col_mapping).count()
    assert(actual === 13475L)
  }


  /**
    * Test case 2 : Validate helix uuid available in processed data
    */
  test("Test2 : Check for correct helix uuid value"){
    val actual: String = processor.processData(in_df,SparkSupport.col_mapping).orderBy(asc("helix_uuid"))
      .select($"helix_uuid").first().get(0).toString
    assert(actual === "00015b30-d743-4abe-bd8f-45467f2a389b")
  }

  /**
    * Test case 3 : Validate RegOrAirFinNum in random message
    */
  test("Test3 : Check for correct FliNum"){
    val actual: String = processor.processData(in_df,SparkSupport.col_mapping)
      .orderBy(asc("helix_uuid")).select($"ConFue").take(3).last.get(0).toString
    assert(actual === "606")
  }

  /**
    * Test case 4 : Validate missing columns from historic data added part of sync process
    */
  test("Test3 : Check for missing columns"){
    val actual1: String = processor.processData(in_df, SparkSupport.col_mapping).select($"tibco_message_time").take(3)
      .last.get(0).asInstanceOf[String]
    val actual2: String = processor.processData(in_df,SparkSupport.col_mapping).select($"PreRouNam").take(3)
      .last.get(0).asInstanceOf[String]
    assert(actual1 === null)
    assert(actual2 === null)
  }
}
