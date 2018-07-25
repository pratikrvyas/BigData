/*----------------------------------------------------------------------------
 * Created on  : 04/01/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : LidoWeatherDedupProcessor.scala
 * Description : Secondary class file for lido whether dedup
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix.emfg

import com.emirates.helix.emfg.model.Model._
import com.emirates.helix.emfg.util.SparkUtils
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Companion object for LidoWeatherDedupProcessor class
  */
object LidoWeatherDedupProcessor  {

  /**
    * LidoWeatherDedupProcessor instance creation
    * @param args user parameter instance
    * @return LidoWeatherDedupProcessor instance
    */
  def apply(args: LidoWeatherDedupArgs): LidoWeatherDedupProcessor  = {
    var processor = new LidoWeatherDedupProcessor()
    processor.args = args
    processor
  }
}

/**
  * LidoWeatherDedupProcessor class with methods to
  * read data from raw layer and move to decomposed.
  */
class LidoWeatherDedupProcessor extends SparkUtils{
   private var args: LidoWeatherDedupArgs = _
  /**  Method for performing the processing which in this case the dedup operation
    *
    *  @param incr_df Dataframe represents incremental data
    *  @param hist_df Dataframe represents history data
    *  @return DataFrame Spark dataframe for deduped data
    */

  def processData(incr_df: DataFrame, hist_df: DataFrame)(implicit spark: SparkSession): (DataFrame,Option[DataFrame]) = {
    import spark.implicits._
    val hist_df_updated = args.init match {
      case true => hist_df.withColumn("tibco_message_time", concat($"tibco_message_time",lit("+04:00")))
      case false => hist_df
    }
    val cols = incr_df.columns.toSet.intersect(hist_df_updated.columns.toSet).map(col).toSeq
    val df = incr_df.select(cols: _*).union(hist_df_updated.select(cols: _*)).where($"FirstAirportCodeIATA".isNotNull)
    var current_data: DataFrame = null
    var hist_data: Option[DataFrame] = None

    val window = Window.partitionBy($"messagetext", $"messagetime", $"FirstAirportCodeIATA", $"FirstAirportCodeICAO")
    val clean_records = df.distinct().withColumn("max_tibcotime",max($"tibco_message_time".cast(TimestampType).cast(LongType)).over(window))
      .where($"tibco_message_time".cast(TimestampType).cast(LongType) === $"max_tibcotime")
      .select($"messagetext", $"messagetime", $"FirstAirportCodeIATA", $"FirstAirportCodeICAO", $"tibco_message_time")

    val max_date = clean_records
      .select(max(to_date(concat($"messagetime",lit("+0:00")).cast(TimestampType)))).first().getDate(0)
    val final_clean_records = clean_records.withColumn("max_flightdate",lit(max_date))
      .cache()

    args.init match {
      case true => {        // Logic for first day load. Data will be splitted to history and current based on months parameter
        current_data = final_clean_records.where(to_date(concat($"messagetime",lit("+0:00")).cast(TimestampType)) >=
          date_sub($"max_flightdate", args.days)).drop($"max_flightdate")
        hist_data = Some(final_clean_records.where(to_date(concat($"messagetime",lit("+0:00")).cast(TimestampType)) <
          date_sub($"max_flightdate", args.days)).drop($"max_flightdate"))
      }
      case false => current_data = final_clean_records.drop($"max_flightdate")
    }
    (current_data,hist_data)
  }

  /**
    *  Overwritten method for super class
    */
  override def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String] = Some("overwrite"),
                           coalesceValue: Option[Int] = Some(args.part) ): Unit = super.writeToHDFS(output, hdfsPath,
    outputFormat,saveMode,coalesceValue)
}
