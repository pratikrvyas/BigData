import com.holdenkarau.spark.testing.SharedSparkContext
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.emirates.helix.model.Model._
import com.emirates.helix.GenericColumnSplitProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}
/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class LidoFsumTest  extends FunSuite with BeforeAndAfter with SharedSparkContext{
  var processor: GenericColumnSplitProcessor = _
  var in_df: DataFrame = _
  implicit lazy val _sc: SparkContext = sc
  implicit lazy val _sqlContext: SQLContext = new SQLContext(_sc)
  import _sqlContext.implicits._

  /**
    * Preparing the tests
    */
  before{
    processor = GenericColumnSplitProcessor(Args(in_format = "avro", in_path = "./src/test/resources/part-r-00000.avro"))
    in_df = processor.readData
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(in_df,SparkSupport.col_str).count()
    assert(actual === 4L)
  }


  /**
    * Test case 2 : Validate helix uuid available in processed data
    */
  test("Test2 : Check for correct helix uuid value"){
    val actual: String = processor.processData(in_df,SparkSupport.col_str).select($"helix_uuid").first().get(0).toString
    assert(actual === "ffb8c962-8d25-4ae4-accc-c31154fd52c8")
  }

  /**
    * Test case 3 : Validate RegOrAirFinNum in random message
    */
  test("Test3 : Check for correct RegOrAirFinNum"){
    val actual: String = processor.processData(in_df,SparkSupport.col_str).select($"RegOrAirFinNum").take(3).last.get(0).toString
    assert(actual === "A6EDD    ")
  }

  /**
    * Test case 4 : Validate exception is thrown for removed columns
    */
  test("Test4 : Verify exception for columns removed") {
    assertThrows[AnalysisException] {
      processor.processData(in_df,SparkSupport.col_str).select($"Suitable_Airports")
    }
    assertThrows[AnalysisException] {
      processor.processData(in_df,SparkSupport.col_str).select($"Pets")
    }
    assertThrows[AnalysisException] {
      processor.processData(in_df,SparkSupport.col_str).select($"WayPoints")
    }
    assertThrows[AnalysisException] {
      processor.processData(in_df,SparkSupport.col_str).select($"Adequate_Airports")
    }
    assertThrows[AnalysisException] {
      processor.processData(in_df,SparkSupport.col_str).select($"EtopsAreas")
    }
  }
}
