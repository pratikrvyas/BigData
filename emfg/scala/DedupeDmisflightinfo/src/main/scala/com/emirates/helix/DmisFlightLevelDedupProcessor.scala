/*----------------------------------------------------------------------------
 * Created on  : 01/09/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DmisDedupProcessor.scala
 * Description : Secondary class file for deduping dmis data.
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.model.Model._
import com.emirates.helix.util.SparkUtils
import com.emirates.helix.udf.UDF._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window
/**
  * Companion object for DmisDedupProcessor class
  */
object DmisFlightLevelDedupProcessor {

  /**
    * DmisDedupProcessor instance creation
    * @param args user parameter instance
    * @return DmisDedupProcessor instance
    */
  def apply(args: DMISDedupArgs): DmisFlightLevelDedupProcessor = {
    var processor = new DmisFlightLevelDedupProcessor()
    processor.args = args
    processor
  }
}

/**
  * DmisDedupProcessor class with methods to
  * read data, perform dedup and write to hdfs.
  */
class DmisFlightLevelDedupProcessor extends SparkUtils {
  private var args: DMISDedupArgs = _

  def processData(incr_df: DataFrame, hist_df: DataFrame)(implicit hiveContext: HiveContext): (DataFrame,Option[DataFrame]) = {
    import hiveContext.implicits._

    val hist_df_updated = args.init match {                     // Logic to get arrival and departure station from historic data
      case true => {
        hist_df.withColumn("arrivalStation", getArrivalStation($"arrivalDepartureFlag",$"viaRoutes"))
          .withColumn("departureStation",getDepartureStation($"arrivalDepartureFlag",$"viaRoutes"))
          .withColumn("actualDate", getFormattedDate($"actualDate"))
          .withColumn("scheduledDate", getFormattedDate($"scheduledDate"))
          .withColumn("flightUpdatedDate", getFormattedDate($"flightUpdatedDate"))
          .withColumn("flightcreatedDate", getFormattedDate($"flightcreatedDate"))
          .withColumn("tibco_messageTime", $"HELIX_TIMESTAMP".cast(LongType).cast(TimestampType))
      }
      case false => hist_df
    }
    val incr_df_updated = incr_df.withColumn("tibco_messageTime", $"tibco_messageTime".cast(TimestampType))

    val cols = incr_df_updated.columns.toSet.intersect(hist_df_updated.columns.toSet).map(col).toSeq
    val df = incr_df_updated.select(cols: _*).unionAll(hist_df_updated.select(cols: _*))
    var current_data: DataFrame = null
    var hist_data: Option[DataFrame] = None

    val window = Window.partitionBy($"airlineCode", $"flightNumber", $"actualDate", $"arrivalDepartureFlag", $"registrationNumber")
    val input = df.select($"airlineCode", $"actualDate", $"flightNumber", $"scheduledDate", $"arrivalDepartureFlag",
      $"registrationNumber", $"departureStation", $"arrivalStation", $"baggageBeltNumber", $"gateNumber", $"bayNumber",
      $"handlingTerminal", $"flightUpdatedDate", $"flightcreatedDate", $"viaRoutes", $"tibco_messageTime")
      .where(lower($"airlineCode") === "ek")
    val clean_records = input.where($"airlineCode".isNotNull && $"flightNumber".isNotNull &&
      $"registrationNumber".isNotNull && $"actualDate".isNotNull)
      .withColumn("date_time",coalesce($"flightUpdatedDate",$"flightcreatedDate").cast(TimestampType))
      .withColumn("max_datetime", max($"date_time").over(window))
      .where($"max_datetime" === $"date_time")
      .distinct()
      .where(((lower($"arrivalDepartureFlag") === "a") &&  ($"departureStation".isNotNull)) ||
        ((lower($"arrivalDepartureFlag") === "d") &&  ($"arrivalStation".isNotNull)))
      .withColumn("max_tibcotime",max($"tibco_messageTime").over(window))
      .where($"max_tibcotime" === $"tibco_messageTime")
      .distinct()

    val max_date = clean_records
      .select(max(to_date(to_utc_timestamp($"actualDate".cast(TimestampType),"Asia/Dubai")))).first().getDate(0)

    val final_clean_records = clean_records
      .select($"airlineCode",$"actualDate",$"flightNumber", $"scheduledDate", $"arrivalDepartureFlag",
      $"registrationNumber", $"departureStation", $"arrivalStation", $"baggageBeltNumber", $"gateNumber", $"bayNumber",
      $"handlingTerminal", $"flightUpdatedDate", $"flightcreatedDate", $"viaRoutes", $"tibco_messageTime",
        lit(max_date).alias("max_flightdate"))
      .cache()

    val rejected_records = input.where($"airlineCode".isNull || $"flightNumber".isNull || $"registrationNumber".isNull
    || $"actualDate".isNull )

    args.init match {
      case true => {        // Logic for first day load. Data will be splitted to history and current based on months parameter
        current_data = final_clean_records.where(to_date(to_utc_timestamp($"actualDate","Asia/Dubai")) >=
          add_months($"max_flightdate", (-1 * args.months))).drop($"max_flightdate")
        hist_data = Some(final_clean_records.where(to_date(to_utc_timestamp($"actualDate","Asia/Dubai")) <
          add_months($"max_flightdate", (-1 * args.months))).drop($"max_flightdate"))
      }
      case false => current_data = final_clean_records.drop($"max_flightdate")
    }
    (current_data,hist_data)
  }

  /**  Overwritten method for super class
    */
    override def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String] = Some("overwrite"),
                             coalesceValue: Option[Int] = Some(args.part) ): Unit = super.writeToHDFS(output, hdfsPath,
      outputFormat,saveMode,coalesceValue)

}