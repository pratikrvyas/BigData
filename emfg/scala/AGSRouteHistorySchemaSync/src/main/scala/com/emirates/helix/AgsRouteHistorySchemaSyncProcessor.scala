/*----------------------------------------------------------------------------
 * Created on  : 02/14/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AgsRouteHistorySchemaSyncProcessor.scala
 * Description : Secondary class file for processing AGS Route data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.agsrouteschemasyncarguments.AGSRouteHistorySchemaSyncArgs._
import com.emirates.helix.util.SparkUtils
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Companion object for AgsRouteHistorySchemaSyncProcessor class
  */
object AgsRouteHistorySchemaSyncProcessor {


  /**
    * AgsRouteHistorySchemaSyncProcessor instance creation
    * @param args user parameter instance
    * @return Processor instance
    */
  def apply(args: Args): AgsRouteHistorySchemaSyncProcessor = {
    var processor = new AgsRouteHistorySchemaSyncProcessor()
    processor.args = args
    processor
  }
}

/**
  * AgsRouteHistorySchemaSyncProcessor class with methods to
  * read history data of AGS Route data and perform schema sync.
  */
class AgsRouteHistorySchemaSyncProcessor extends SparkUtils{
  private var args: Args = _

   /**  Method to read the AGS Route historic data from HDFS
    *
    *  @return DataFrame
    */
  def readAGSRouteHistory(implicit spark: SparkSession): DataFrame = {
    // sqlContext.read.format("com.databricks.spark.avro").load(args.ags_src_route_history_location)
    val ags_route_df = readValueFromHDFS(args.ags_src_route_history_location,"avro",spark)
    ags_route_df.createOrReplaceTempView("ags_route_df_tab")
    val ags_refline_df = readValueFromHDFS(args.ags_src_refline_history_location,"avro",spark)
    ags_refline_df.createOrReplaceTempView("ags_refline_df_tab")
    val return_df = spark.sql("select  s.*,t.FILENAME from ags_route_df_tab s left outer join " +
      "ags_refline_df_tab t on s.FILE_NO = t.FILENUMBER").drop("FILE_NO")
    return return_df
  }


  /**  Method to sync the AGS Route historical data as per incremental schema
    *
    *  @return DataFrame
    */
  def syncAGSRoute(dataFrame: DataFrame)(implicit spark: SparkSession): DataFrame = {

    dataFrame.createOrReplaceTempView("ags_route_hist_df_tab")

    val ags_hist_df = spark.sql("select trim(UTC) as UTC, substr(trim(LEG_DATE),1,10) as LEG_DATE, " +
      "trim(ALT_STD) as AltStandard, trim(ALT_QNH) as AltQNH, trim(CAS) as CAS, trim(PHASE) as FlightPhase, " +
      "trim(TAS) as TAS, trim(MACH) as MACH, '0.0' as TrackDistance, trim(N11) as N11, trim(N12) as N12, " +
      "trim(N13) as N13, trim(N14) as N14, trim(IVV) as IVV, trim(GRS_WEIGHT) as GrossWeight, " +
      "trim(FUEL_BURNED) as FuelBurned,  '0.0' as LATPC, '0.0' as LONPC, trim(AIR_GRND_DISCRETE) as AirGroundDiscrete," +
      " trim(TRAN_ALT_DISCRETE) as TransitionAltitudeDiscrete, trim(HEADING_TRUE) as HeadingTrue, " +
      "trim(BARO_SETTING) as BaroSetting, trim(CG) as CG, trim(ISA_DEV) as ISADEV, '0.0' as WINDDirection, " +
      "trim(WIND_SPEED) as WindSpeed, '0' as WindComponent, trim(TAT) as TAT, trim(SAT) as SAT, " +
      "trim(CR_WIND_COMPONENT) as CrossWindComponent, trim(CR_WIND_VECTOR) as CrossWindDriftVector, " +
      "trim(FOB) as FOB, trim(FOB_T1) as FOB_T1, trim(FOB_T2) as FOB_T2, trim(FOB_T3) as FOB_T3, " +
      "trim(FOB_T4) as FOB_T4, trim(FOB_T5) as FOB_T5, trim(FOB_T6) as FOB_T6, trim(FOB_T7) as FOB_T7, " +
      "trim(FOB_T8) as FOB_T8, trim(FOB_T9) as FOB_T9, trim(FOB_T10) as FOB_T10, trim(FOB_T11) as FOB_T11, " +
      "trim(FOB_T12) as FOB_T12, trim(HEIGHT) as Height, trim(FILE_SEQNO) as RowNumber, " +
      "substr(trim(FLT_DATE),1,10) as PK_Date , trim(FLT_NUMBER) as PK_FlNum, trim(AC_TAIL) as PK_Tail, " +
      "trim(ORIGIN) as PK_Origin, trim(DESTINATION) as PK_Dest, trim(FILENAME) as FileName, 'Actual_01011900000000' as FolderName," +
      " trim(ALTITUDE) as Altitude, trim(LATPC) as Latitude, trim(LONPC) as Longitude," +
      " trim(WIND_DIRECTION) as WindDirectionAmmended, trim(WIND_COMPONENT_AGS) as WindComponentAGS, " +
      "trim(WIND_COMPONENT_NAV) as WindComponentNAV, trim(GROUND_SPEED) as GroundSpeed,  " +
      "concat(substr(trim(LEG_DATE),1,10),' ', trim(UTC)) as TimeStamp, trim(TRACK_DIST) as TrackDistanceAmended," +
      " trim(AIR_DIST) as AirDist,  trim(ACC_AIR_DIST) as AccAirDist, " +
      " trim(ACC_TIME) as AccTime from ags_route_hist_df_tab")

    return ags_hist_df
  }


  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeAGSRouteHistorySync(out_df: DataFrame, coalesce_value : Int): Unit = {
    writeToHDFS(out_df,args.ags_tgt_route_history_sync_location,"avro",Some("error"),Some(coalesce_value))
  }

}
