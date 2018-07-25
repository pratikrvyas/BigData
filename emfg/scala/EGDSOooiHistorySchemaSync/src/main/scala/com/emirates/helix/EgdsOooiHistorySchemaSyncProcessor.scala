/*----------------------------------------------------------------------------
 * Created on  : 04/16/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsOooiHistorySchemaSyncProcessor.scala
 * Description : Secondary class file for processing EGDS Oooi data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.egdsoooischemasyncarguments.EGDSOooiHistorySchemaSyncArgs._
import com.emirates.helix.udf.UDF.{getFormattedMessage}
import com.emirates.helix.util.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Companion object for EgdsOooiHistorySchemaSyncProcessor class
  */
object EgdsOooiHistorySchemaSyncProcessor {

  /**
    * EgdsOooiHistorySchemaSyncProcessor instance creation
    * @param args user parameter instance
    * @return Processor instance
    */
  def apply(args: Args): EgdsOooiHistorySchemaSyncProcessor = {
    var processor = new EgdsOooiHistorySchemaSyncProcessor()
    processor.args = args
    processor
  }
}

/**
  * EgdsOooiHistorySchemaSyncProcessor class with methods to
  * read history data of EGDS Oooi data and perform schema sync.
  */
class EgdsOooiHistorySchemaSyncProcessor extends SparkUtils{
  private var args: Args = _

   /**  Method to read the EGDS Oooi historic data from HDFS
    *   and sync to incremental schema
    *  @return DataFrame
    */
  def readEGDSOooiHistory(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val egds_oooi_hist_df1 = readValueFromHDFS(args.egds_oooi_src_history_location,"avro",spark)
      .select($"CREATED_ON".as("tibco_messageTime"),$"HELIX_UUID",$"HELIX_TIMESTAMP",
      getFormattedMessage($"MESSAGE_CONTENT").as("message_list"))

    val egds_oooi_hist_df2 = egds_oooi_hist_df1.select($"tibco_messageTime",
      $"message_list"(0).as("flightstate"),
      $"message_list"(1).as("acreg_no"),
      $"message_list"(2).as("flight_no"),
      $"message_list"(3).as("flight_date"),
      $"message_list"(4).as("message_date"),
      $"message_list"(5).as("time_of_message_received"),
      $"message_list"(6).as("dep_stn"),
      $"message_list"(7).as("arr_stn"),
      $"message_list"(8).as("time_of_message_delivered"),
      $"message_list"(9).as("fob"),
      $"message_list"(10).as("zfw"),
      $"HELIX_UUID",
      $"HELIX_TIMESTAMP")

    return egds_oooi_hist_df2
  }


  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeEGDSOooiHistorySync(out_df: DataFrame, coalesce_value : Int): Unit = {
    writeToHDFS(out_df,args.egds_oooi_tgt_history_sync_location,"avro",Some("error"),Some(coalesce_value))
  }

}
