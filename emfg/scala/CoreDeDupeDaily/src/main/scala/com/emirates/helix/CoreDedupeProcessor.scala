/*----------------------------------------------------------------------------
 * Created on  : 01/31/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : CoreDedupeProcessor.scala
 * Description : Secondary class file for processing core data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.corededupearguments.CoreDeDupeArgs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Companion object for CoreDedupeProcessor class
  */
object CoreDedupeProcessor {

  /**
    * CoreDedupeProcessor instance creation
    * @param args user parameter instance
    * @return CoreDedupeProcessor instance
    */
  def apply(args: Args): CoreDedupeProcessor = {
    var processor = new CoreDedupeProcessor()
    processor.args = args
    processor
  }
}

/**
  * CoreDedupeProcessor class will
  * read history and incremental data of Core and perform dedupe.
  */
class CoreDedupeProcessor {
  private var args: Args = _

   /**  Method to read the Core historic sync and incremental data and
    *   union the result set
    *  @return DataFrame
    */
  def readCore(implicit sqlContext: HiveContext): DataFrame = {
    val core_curr_df = sqlContext.read.format("parquet")
      .load(args.core_dedupe_current_location)

    val core_incr_df = sqlContext.read.format("com.databricks.spark.avro")
      .load(args.core_incremental_location).drop("HELIX_UUID").drop("HELIX_TIMESTAMP")

    val core_union_df = core_incr_df.unionAll(core_curr_df)

    return core_union_df
  }

  /**  Method to DeDupe the Core historical and incremental data
    *
    *  @return DataFrame
    */
  def deDupeCore(dataFrame: DataFrame)(implicit sqlContext: HiveContext): DataFrame = {

    import sqlContext.implicits._

    val core_union_uniq_df = dataFrame.dropDuplicates
    return core_union_uniq_df
  }


  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeCoreDeDupe(out_df: DataFrame,  coalesce_value : Int,
                      core_dedupe_tgt_current_location : String) (implicit sqlContext: HiveContext) : Unit = {

       out_df.coalesce(coalesce_value).write.format("parquet").save(args.core_dedupe_tgt_current_location)
  }
}
