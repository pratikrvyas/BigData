/*----------------------------------------------------------------------------
 * Created on  : 02/19/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AgsRouteDedupeProcessor.scala
 * Description : Secondary class file for processing AGS Route data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.agsroutededupearguments.AGSRouteDeDupeArgs._
import com.emirates.helix.util.SparkUtils
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Companion object for AgsRouteDedupeProcessor class
  */
object AgsRouteDedupeProcessor {

  /**
    * AgsRouteDedupeProcessor instance creation
    * @param args user parameter instance
    * @return AgsRouteDedupeProcessor instance
    */
  def apply(args: Args): AgsRouteDedupeProcessor = {
    var processor = new AgsRouteDedupeProcessor()
    processor.args = args
    processor
  }
}

/**
  * AgsRouteDedupeProcessor class will
  * read dedupe current and incremental data of AGS Route and perform dedupe.
  */
class AgsRouteDedupeProcessor extends SparkUtils{
  private var args: Args = _

   /**  Method to read the AGS Route dedupe current and incremental data,
    *   format the incremental data and union both the dataframes
    *  @return DataFrame
    */
  def readAGSRoute(implicit spark: SparkSession): DataFrame = {

    val ags_dedupe_current_df = readValueFromHDFS(args.ags_src_route_dedupe_current_location,"parquet",spark)

    val ags_route_incr_df = readValueFromHDFS(args.ags_src_route_incremental_location,"parquet",spark)
    ags_route_incr_df.createOrReplaceTempView("ags_route_incr_df_tab")

    val ags_route_incr_formatted_df = spark.sql("select trim(UTC) as UTC, " +
      "substr(from_unixtime(unix_timestamp(trim(Date),'dd/MM/yy')),1,10) as LEG_DATE, " +
      "trim(AltStandard) as AltStandard, trim(AltQNH) as AltQNH, trim(CAS) as CAS, " +
      "trim(FlightPhase) as FlightPhase, trim(TAS) as TAS, trim(MACH) as MACH, " +
      "trim(TrackDistance) as TrackDistance, trim(N11) as N11, trim(N12) as N12, " +
      "trim(N13) as N13, trim(N14) as N14, trim(IVV) as IVV, trim(GrossWeight) as GrossWeight, " +
      "trim(FuelBurned) as FuelBurned, trim(LATPC) as LATPC, trim(LONPC) as LONPC, " +
      "trim(AirGroundDiscrete) as AirGroundDiscrete, trim(TransitionAltitudeDiscrete) as TransitionAltitudeDiscrete," +
      " trim(HeadingTrue) as HeadingTrue, trim(BaroSetting) as BaroSetting, trim(CG) as CG, trim(ISADEV) as ISADEV," +
      " trim(WINDDirection) as WINDDirection, trim(WindSpeed) as WindSpeed, trim(WindComponent) as WindComponent," +
      " trim(TAT) as TAT, trim(SAT) as SAT, trim(CrossWindComponent) as CrossWindComponent," +
      " trim(CrossWindDriftVector) as CrossWindDriftVector, trim(FOB) as FOB, trim(FOB_T1) as FOB_T1," +
      " trim(FOB_T2) as FOB_T2, trim(FOB_T3) as FOB_T3, trim(FOB_T4) as FOB_T4, trim(FOB_T5) as FOB_T5," +
      " trim(FOB_T6) as FOB_T6, trim(FOB_T7) as FOB_T7, trim(FOB_T8) as FOB_T8, trim(FOB_T9) as FOB_T9," +
      " trim(FOB_T10) as FOB_T10, trim(FOB_T11) as FOB_T11, trim(FOB_T12) as FOB_T12, trim(Height) as Height," +
      " cast(trim(RowNumber) as string) as RowNumber," +
      " coalesce(substr(from_unixtime(unix_timestamp(trim(PK_Date), 'dd/MM/yy')), 1, 10), null, '1900-01-01') as PK_Date," +
      " LPAD(translate(translate(trim(PK_FlNum), ' ', ''),'UAE',''),4,'0') as PK_FlNum," +
      " translate(translate(translate(trim(PK_Tail), ' ', ''), '-', ''), '.', '') as PK_Tail, trim(PK_Origin) as PK_Origin," +
      " trim(PK_Dest) as PK_Dest, split(trim(FileName), '.gz')[0] as FileName, trim(FolderName) as FolderName, trim(Altitude) as Altitude," +
      " trim(Latitude) as Latitude, trim(Longitude) as Longitude," +
      " cast(WindDirectionAmmended as string) as WindDirectionAmmended," +
      " cast(WindComponentAGS as string) as WindComponentAGS, cast(WindComponentNAV as string) as  WindComponentNAV," +
      " cast(GroundSpeed as string) as GroundSpeed," +
      " substr(trim(Timestamp),1,19) as Timestamp," +
      " cast(TrackDistanceAmended as string) as TrackDistanceAmended, cast(AirDist as string) as AirDist," +
      " cast(AccAirDist as string) as AccAirDist, cast(AccTime as string) as AccTime from ags_route_incr_df_tab ")

    val ags_route_hist_incr_df = ags_dedupe_current_df.union(ags_route_incr_formatted_df)
    return ags_route_hist_incr_df
  }

  /**  Method to DeDupe the AGS Route data
    *
    *  @return DataFrame
    */
  def deDupeAGSRoute(dataFrame: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.sqlContext.implicits._
    val deduped_df = dataFrame.dropDuplicates()

    val agsGrp = Window.partitionBy($"PK_Date",$"PK_FlNum",$"PK_Tail",$"PK_Origin",$"PK_Dest")

    val temp_df1 = deduped_df.withColumn("MAX_FileName", max($"FileName").over(agsGrp)).filter($"MAX_FileName" === $"FileName").drop("MAX_FileName")

    val temp_df2 = temp_df1.withColumn("MAX_FolderName", max($"FolderName").over(agsGrp)).filter($"MAX_FolderName" === $"FolderName").drop("MAX_FolderName")

    return temp_df2

  }


  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeAGSDeDupe(out_df: DataFrame, coalesce_value : Int,
                     ags_tgt_route_dedupe_current_location : String) (implicit spark: SparkSession) : Unit = {

    writeToHDFS(out_df,args.ags_tgt_route_dedupe_current_location,"parquet",
      Some("error"),Some(coalesce_value))

  }
}
