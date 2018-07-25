/*----------------------------------------------------------------------------
 * Created on  : 04/16/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsOooiDedupeProcessor.scala
 * Description : Secondary class file for processing EGDS Oooi data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.egdsoooidedupearguments.EGDSOooiDeDupeArgs._
import com.emirates.helix.util.SparkUtils
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


/**
  * Companion object for EgdsOooiDedupeProcessor class
  */
object EgdsOooiDedupeProcessor {

  /**
    * EgdsOooiDedupeProcessor instance creation
    * @param args user parameter instance
    * @return EgdsOooiDedupeProcessor instance
    */
  def apply(args: Args): EgdsOooiDedupeProcessor = {
    var processor = new EgdsOooiDedupeProcessor()
    processor.args = args
    processor
  }
}

/**
  * EgdsOooiDedupeProcessor class will
  * read history and incremental data of EGDS Oooi and perform dedupe.
  */
class EgdsOooiDedupeProcessor extends SparkUtils{
  private var args: Args = _

   /**  Method to read the EGDS Oooi historic sync and incremental data and
    *   union the result set
    *  @return DataFrame
    */
  def readEGDSOooi(implicit spark: SparkSession): DataFrame = {

    val egds_oooi_hist_sync_df = readValueFromHDFS(args.egds_oooi_src_dedupe_current_location,"parquet",spark)

    val egds_oooi_incr_df = readValueFromHDFS(args.egds_oooi_src_incremental_location,"parquet",spark)
    egds_oooi_incr_df.createOrReplaceTempView("egds_oooi_incr_df_tab")

    // changed the format of time
    val egds_oooi_incr_formatted_df = spark.sql("select acreg_no, flight_no, flight_date, dep_stn, arr_stn, zfw, " +
      " case when tibco_message_timestamp_FUEL_OUT is null then null else " +
      "cast(cast(substr(trim(tibco_message_timestamp_FUEL_OUT),1,23) as timestamp) as string) end " +
      "as tibco_message_timestamp_FUEL_OUT, case when time_of_message_received_FUEL_OUT is null or " +
      "trim(time_of_message_received_FUEL_OUT) = '' then null else " +
      "concat(substr(trim(time_of_message_received_FUEL_OUT),1,2),':',substr(trim(time_of_message_received_FUEL_OUT),3,2),':00') " +
      "end as time_of_message_received_FUEL_OUT, case when msgDeliveredDateTime_FUEL_OUT is null " +
      "then null else trim(msgDeliveredDateTime_FUEL_OUT) end as msgDeliveredDateTime_FUEL_OUT, OOOI_FUEL_OUT, DRVD_FUEL_OUT, FUEL_OUT, " +
      "case when tibco_message_timestamp_FUEL_OFF is null then null else " +
      "cast(cast(substr(trim(tibco_message_timestamp_FUEL_OFF),1,23) as timestamp) as string) end " +
      "as tibco_message_timestamp_FUEL_OFF, case when time_of_message_received_FUEL_OFF is null or " +
      "trim(time_of_message_received_FUEL_OFF) = '' then null else " +
      "concat(substr(trim(time_of_message_received_FUEL_OFF),1,2),':',substr(trim(time_of_message_received_FUEL_OFF),3,2),':00') " +
      "end as time_of_message_received_FUEL_OFF, case when msgDeliveredDateTime_FUEL_OFF is null then null " +
      "else trim(msgDeliveredDateTime_FUEL_OFF) end as msgDeliveredDateTime_FUEL_OFF, OOOI_FUEL_OFF, DRVD_FUEL_OFF, FUEL_OFF, " +
      "case when tibco_message_timestamp_FUEL_ON is null then null else " +
      "cast(cast(substr(trim(tibco_message_timestamp_FUEL_ON),1,23) as timestamp) as string) end " +
      "as tibco_message_timestamp_FUEL_ON, case when time_of_message_received_FUEL_ON is null or " +
      "trim(time_of_message_received_FUEL_ON) = '' then null else " +
      "concat(substr(trim(time_of_message_received_FUEL_ON),1,2),':',substr(trim(time_of_message_received_FUEL_ON),3,2),':00') " +
      "end as time_of_message_received_FUEL_ON, case when msgDeliveredDateTime_FUEL_ON is null then null " +
      "else trim(msgDeliveredDateTime_FUEL_ON) end as msgDeliveredDateTime_FUEL_ON, OOOI_FUEL_ON, DRVD_FUEL_ON, FUEL_ON, " +
      "case when tibco_message_timestamp_FUEL_IN is null then null else " +
      "cast(cast(substr(trim(tibco_message_timestamp_FUEL_IN),1,23) as timestamp) as string) end " +
      "as tibco_message_timestamp_FUEL_IN, case when time_of_message_received_FUEL_IN is null or " +
      "trim(time_of_message_received_FUEL_IN) = '' then null else " +
      "concat(substr(trim(time_of_message_received_FUEL_IN),1,2),':',substr(trim(time_of_message_received_FUEL_IN),3,2),':00') " +
      "end as time_of_message_received_FUEL_IN, case when msgDeliveredDateTime_FUEL_IN is null then null else " +
      "trim(msgDeliveredDateTime_FUEL_IN) end as msgDeliveredDateTime_FUEL_IN, OOOI_FUEL_IN, DRVD_FUEL_IN, FUEL_IN, processing_date from egds_oooi_incr_df_tab")

    val egds_oooi_hist_incr_df = egds_oooi_hist_sync_df.union(egds_oooi_incr_formatted_df)
    return egds_oooi_hist_incr_df
  }

  /**  Method to DeDupe the Egds Oooi historical and incremental data
    *
    *  @return DataFrame
    */
  def deDupeEGDSOooi(dataFrame: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.sqlContext.implicits._
    dataFrame.dropDuplicates.createOrReplaceTempView("egds_oooi_hist_incr_df_tab")

    val egds_oooi_deduped_df = spark.sql(" select acreg_no, flight_no, flight_date, dep_stn, arr_stn, zfw, " +
      "tibco_message_timestamp_FUEL_OUT, time_of_message_received_FUEL_OUT, msgDeliveredDateTime_FUEL_OUT, OOOI_FUEL_OUT, DRVD_FUEL_OUT, FUEL_OUT, " +
      "tibco_message_timestamp_FUEL_OFF, time_of_message_received_FUEL_OFF, msgDeliveredDateTime_FUEL_OFF, OOOI_FUEL_OFF, DRVD_FUEL_OFF, FUEL_OFF, " +
      "tibco_message_timestamp_FUEL_ON, time_of_message_received_FUEL_ON, msgDeliveredDateTime_FUEL_ON, OOOI_FUEL_ON, DRVD_FUEL_ON, FUEL_ON, " +
      "tibco_message_timestamp_FUEL_IN, time_of_message_received_FUEL_IN, msgDeliveredDateTime_FUEL_IN, OOOI_FUEL_IN, DRVD_FUEL_IN, FUEL_IN, processing_date " +
      "from ( select acreg_no, flight_no, flight_date, dep_stn, arr_stn, zfw, tibco_message_timestamp_FUEL_OUT, " +
      "time_of_message_received_FUEL_OUT, msgDeliveredDateTime_FUEL_OUT, OOOI_FUEL_OUT, DRVD_FUEL_OUT, FUEL_OUT, tibco_message_timestamp_FUEL_OFF, " +
      "time_of_message_received_FUEL_OFF, msgDeliveredDateTime_FUEL_OFF, OOOI_FUEL_OFF, DRVD_FUEL_OFF, FUEL_OFF, tibco_message_timestamp_FUEL_ON, " +
      "time_of_message_received_FUEL_ON, msgDeliveredDateTime_FUEL_ON, OOOI_FUEL_ON, DRVD_FUEL_ON, FUEL_ON, tibco_message_timestamp_FUEL_IN, " +
      "time_of_message_received_FUEL_IN, msgDeliveredDateTime_FUEL_IN, OOOI_FUEL_IN, DRVD_FUEL_IN, FUEL_IN, processing_date, ROW_NUMBER() over(partition by acreg_no," +
      " flight_no, flight_date, dep_stn, arr_stn order by processing_date) as ranked " +
      "from egds_oooi_hist_incr_df_tab ) A where ranked = 1 ")

    return egds_oooi_deduped_df
  }


  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeEGDSOooiDeDupe(out_df: DataFrame, lookback_months : Int, coalesce_value : Int,
                     egds_oooi_tgt_dedupe_current_location : String)
                    (implicit spark: SparkSession) : Unit = {

    writeToHDFS(out_df,args.egds_oooi_tgt_dedupe_current_location,"parquet",
      Some("overwrite"),Some(coalesce_value))

  }
}