/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AgsReflineDedupeProcessor.scala
 * Description : Secondary class file for processing AGS Refline data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.agsreflinesdedupearguments.AGSReflineDeDupeArgs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.hive.HiveContext


/**
  * Companion object for AgsReflineDedupeProcessor class
  */
object AgsReflineDedupeProcessor {

  /**
    * AgsReflineProcessor instance creation
    * @param args user parameter instance
    * @return AgsReflineDedupeProcessor instance
    */
  def apply(args: Args): AgsReflineDedupeProcessor = {
    var processor = new AgsReflineDedupeProcessor()
    processor.args = args
    processor
  }
}

/**
  * AgsReflineDedupeProcessor class will
  * read history and incremental data of AGS Refline and perform dedupe.
  */
class AgsReflineDedupeProcessor {
  private var args: Args = _

   /**  Method to read the AGS Refline historic sync and incremental data and
    *   union the result set
    *  @return DataFrame
    */
  def readAGSRefline(implicit sqlContext: HiveContext): DataFrame = {
    val ags_hist_sync_df = sqlContext.read.format("parquet")
      .load(args.ags_refline_dedupe_current_location)
    val ags_incr_df = sqlContext.read.format("com.databricks.spark.avro")
      .load(args.ags_src_refline_incremental_location).withColumnRenamed("Date","FltDate")
    ags_incr_df.registerTempTable("ags_incr_df_tab")
    val ags_incr_formatted_df = sqlContext.sql("select Refline," +
      "concat(cast(from_unixtime(unix_timestamp(trim(FltDate),'dd/MM/yy')) as String),'.0') as FltDate, " +
      "LPAD(SPLIT(trim(FlightNumber),' ')[1],4,'0') as FlightNumber, " +
      "translate(translate(translate(trim(Tail), ' ', ''),'-',''),'.','') as Tail,trim(Origin) as Origin, " +
      "trim(Destination) as Destination,trim(EngStartTime) as EngStartTime," +
      "trim(EngStopTime) as EngStopTime,trim(V1) as V1,trim(V2) as V2,trim(VR) as VR," +
      "trim(AssumedTemperature) as AssumedTemperature, " +
      "trim(TO_FLAP) as TO_FLAP,trim(ZFW) as ZFW, trim(RWY_TO) as RWY_TO, " +
      "trim(BAROMB_TO) as BAROMB_TO, trim(SAT_TO) as SAT_TO, trim(WC_TO) as WC_TO, " +
      "trim(WD_TO) as WD_TO, trim(WS_TO) as WS_TO, trim(AI_TO) as AI_TO, trim(RWY_LD) as RWY_LD, " +
      "trim(BAROMB_LD) as BAROMB_LD, trim(SAT_LD) as SAT_LD, trim(WC_LD) as WC_LD, " +
      "trim(WD_LD) as WD_LD, trim(WS_LD) as WS_LD, trim(VAPP) as VAPP, trim(FLAP_LD) as FLAP_LD, " +
      "trim(APU_FUEL) as APU_FUEL, cast(trim(FUEL_OFF) as int)*1000 as FUEL_OFF, " +
      "cast(trim(FUEL_ON) as int)*1000 as FUEL_ON, trim(FileName) as FileName, trim(FolderName) as FolderName from ags_incr_df_tab")

    val ags_hist_incr_df = ags_incr_formatted_df.unionAll(ags_hist_sync_df)
    return ags_hist_incr_df
  }

  /**  Method to DeDupe the AGS Refline historical and incremental data
    *
    *  @return DataFrame
    */
  def deDupeAGSRefline(dataFrame: DataFrame)(implicit sqlContext: HiveContext): DataFrame = {

    import sqlContext.implicits._

    val deduped_df = dataFrame.dropDuplicates()

    val agsGrp = Window.partitionBy($"FltDate",$"FlightNumber",$"Tail",$"Origin",$"Destination")

    val temp_df1 = deduped_df.withColumn("MAX_FileName", max($"FileName").over(agsGrp)).filter($"MAX_FileName" === $"FileName").drop("MAX_FileName")

    val temp_df2 = temp_df1.withColumn("MAX_FolderName", max($"FolderName").over(agsGrp)).filter($"MAX_FolderName" === $"FolderName").drop("MAX_FolderName")

    return temp_df2
  }


  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeAGSDeDupe(out_df: DataFrame, coalesce_value : Int,
                     ags_tgt_refline_dedupe_current_location : String) (implicit sqlContext: HiveContext) : Unit = {

    out_df.coalesce(coalesce_value).write.format("parquet").save(args.ags_tgt_refline_dedupe_current_location)

  }

}
