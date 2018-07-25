/*----------------------------------------------------------------------------
 * Created on  : 01/04/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AgsReflineProcessor.scala
 * Description : Secondary class file for processing AGS Refline data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.agsreflineschemasyncarguments.AGSReflineHistorySchemaSyncArgs._
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Companion object for AgsReflineHistorySchemaSyncProcessor class
  */
object AgsReflineHistorySchemaSyncProcessor {

  /**
    * AgsReflineProcessor instance creation
    * @param args user parameter instance
    * @return EgdsR2DProcessor instance
    */
  def apply(args: Args): AgsReflineHistorySchemaSyncProcessor = {
    var processor = new AgsReflineHistorySchemaSyncProcessor()
    processor.args = args
    processor
  }
}

/**
  * AgsReflineProcessor class with methods to
  * read history data of AGS Refline and perform schema sync.
  */
class AgsReflineHistorySchemaSyncProcessor {
  private var args: Args = _

   /**  Method to read the AGS Refline historic data from HDFS
    *
    *  @return DataFrame
    */
  def readAGSReflineHistory(implicit sqlContext: SQLContext): DataFrame = {
    //sc.sequenceFile[LongWritable, BytesWritable](args.in_path).map(x => new String(x._2.copyBytes(), "utf-8"))
    sqlContext.read.format("com.databricks.spark.avro").load(args.ags_src_refline_history_location)
  }


  /**  Method to sync the AGS Refline historical data
    *
    *  @return DataFrame
    */
  def syncAGSRefline(dataFrame: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    dataFrame.registerTempTable("ags_hist_df_tab")
    // import sqlContext.implicits._
    val ags_hist_df = sqlContext.sql("select 'Refline' as Refline, FLTDATE as FltDate, FLT_NUMBER as FlightNumber," +
      "AC_TAIL as Tail, ORIGIN as Origin, DESTINATION as Destination,ENG_START_TIME as EngStartTime," +
      "ENG_STOP_TIME as EngStopTime,V1,V2,VR,ASSM_TEMP as AssumedTemperature,TO_FLAP, ZFW, RWY_TO, BAROMB_TO, " +
      "SAT_TO, WINDCOMP_TO as WC_TO, WINDDIR_TO as WD_TO, WINDSPD_TO as WS_TO,ANTI_ICE_STATUS_TO as AI_TO,RWY_LD, " +
      "BAROMB_LD, SAT_LD,  WINDCOMP_LD as WC_LD, WINDDIR_LD as WD_LD, WINDSPD_LD as WS_LD, VAPP, FLAP_LD, APU_FUEL, " +
      "FUEL_OFF, FUEL_ON, FILENAME as FileName, 'Actual_01011900000000' as FolderName, HELIX_UUID, HELIX_TIMESTAMP from ags_hist_df_tab")
    return ags_hist_df
  }


  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeAGSHistorySync(out_df: DataFrame, coalesce_value : Int): Unit = {
     out_df.coalesce(coalesce_value).write.format("com.databricks.spark.avro").save(args.ags_tgt_refline_history_sync_location)
  }

}
