/*----------------------------------------------------------
 * Created on  : 19/11/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.1
 * Project     : Helix-OpsEfficnecy
 * Filename    : MultiSourceR2DProcessor.scala
 * Description : Secondary class file for moving a csv/avro/parquet file from raw to decomposed layer
 * ----------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.model.Model._
import com.emirates.helix.util.SparkUtils
import com.emirates.helix.udf.UDF._
import org.apache.spark.sql.{DataFrame, SQLContext}
/**
  * Companion object for MultiSourceR2DProcessor class
  */
object MultiSourceR2DProcessor {
  /**
    * MultiSourceR2DProcessor instance creation
    * @param args user parameter instance
    * @return MultiSourceR2DProcessor instance
    */
  def apply(args: R2DArgs): MultiSourceR2DProcessor = {
    var processor = new MultiSourceR2DProcessor()
    processor.args = args
    processor
  }
}

/**
  * MultiSourceR2DProcessor class with methods to read, process and write the data
  */
class MultiSourceR2DProcessor extends SparkUtils{
  private var args: R2DArgs = _

  /**  Method which does the processing
    *
    *  @param in_df Dataframe holding input data
    *  @return DataFrame Spark dataframe for processed R2D data
    */
  def processData(in_df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    in_df.withColumn("HELIX_UUID", generateUUID())
      .withColumn("HELIX_TIMESTAMP", generateTimestamp())
  }

}
