/*----------------------------------------------------------
 * Created on  : 04/22/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AltealdmPrereqProcessor.scala
 * Description : Scala application main class file for Altea LDM message load distribution calculation
 * ----------------------------------------------------------
 */
package com.emirates.helix.emfg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, LongType}
import com.emirates.helix.emfg.model.Model._
import com.emirates.helix.emfg.util.SparkUtils
import org.apache.spark.sql.expressions.Window
import com.emirates.helix.emfg.udf._

/**
  * Companion object for AltealdmPrereqProcessor class
  */
object AltealdmPrereqProcessor {
  /**
    * AltealdmPrereqProcessor instance creation
    * @param args user parameter instance
    * @return AltealdmPrereqProcessor instance
    */
  def apply(args: AltLdmArgs): AltealdmPrereqProcessor  = {
    var processor = new AltealdmPrereqProcessor()
    processor.args = args
    processor
  }
}

/**
  * AltealdmPrereqProcessor class with methods to
  * process altea ldm decomposed layer data.
  */
class AltealdmPrereqProcessor extends SparkUtils {
  private var args: AltLdmArgs = _

  /**
    * @param in_df input dataframe
    * @param spark Spark session object
    * @return DataFrame
    */
  def processData(in_df: DataFrame,rej_path: String,rej_format: String,part: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val window_flight = Window.partitionBy($"flight_no", $"flight_date").orderBy($"tibco_timestamp")
    val window_leg = Window.partitionBy($"flight_no", $"flight_date").orderBy($"flight_leg_number".cast("Int"))
      .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    val window_dedup = Window.partitionBy($"flight_no", $"flight_date", $"aircraft_reg", $"dep_station", $"arr_station")
    val ldm_upd = in_df.where($"flight_no".rlike("^EK.*$"))
      .withColumn("tibco_timestamp", $"tibco_messageTime".cast(TimestampType).cast(LongType))
      .withColumn("max_time", max($"tibco_timestamp").over(window_dedup))
      .where($"tibco_timestamp" === $"max_time")
      .withColumn("flight_leg_number", row_number().over(window_flight))
      .drop("tibco_timestamp", "max_time")
      .select($"flight_no", $"flight_date", $"aircraft_reg", $"aircraft_ver", $"dep_station",
        $"arr_station", $"flight_leg_number", explode($"flight_baggage_weight").as("flight_baggage_weight"),
        $"tibco_messageTime")

    val ldm_rjtd = ldm_upd.where($"flight_baggage_weight.baggage_weight".isNull || $"flight_baggage_weight.cargo_weight".isNull || $"flight_baggage_weight.mail_weight".isNull || $"flight_baggage_weight.transit_weight".isNull ||
      $"flight_baggage_weight.miscl_weight".isNull)

    writeToHDFS(ldm_rjtd, rej_path,  rej_format, Some("overwrite"), Some(part))

    val ldm_correct = ldm_upd.except(ldm_rjtd)

    val out_df = ldm_correct
      .withColumn("baggage_weight", when($"flight_leg_number".cast("Int") === 1, sum($"flight_baggage_weight.baggage_weight").over(window_leg))
        otherwise UDF.custom_sum("baggage_weight")(collect_list(struct($"flight_leg_number".cast("Int").as("flight_leg_number"), $"flight_baggage_weight.off_point", $"flight_baggage_weight.baggage_weight"))
        .over(window_leg), $"arr_station"))
      .withColumn("cargo_weight", when($"flight_leg_number".cast("Int") === 1, sum($"flight_baggage_weight.cargo_weight").over(window_leg))
        otherwise UDF.custom_sum("cargo_weight")(collect_list(struct($"flight_leg_number".cast("Int").as("flight_leg_number"), $"flight_baggage_weight.off_point", $"flight_baggage_weight.cargo_weight"))
        .over(window_leg), $"arr_station"))
      .withColumn("mail_weight", when($"flight_leg_number".cast("Int") === 1, sum($"flight_baggage_weight.mail_weight").over(window_leg))
        otherwise UDF.custom_sum("mail_weight")(collect_list(struct($"flight_leg_number".cast("Int").as("flight_leg_number"), $"flight_baggage_weight.off_point", $"flight_baggage_weight.mail_weight"))
        .over(window_leg), $"arr_station"))
      .withColumn("transit_weight", when($"flight_leg_number".cast("Int") === 1, sum($"flight_baggage_weight.transit_weight").over(window_leg))
        otherwise UDF.custom_sum("transit_weight")(collect_list(struct($"flight_leg_number".cast("Int").as("flight_leg_number"), $"flight_baggage_weight.off_point", $"flight_baggage_weight.transit_weight"))
        .over(window_leg), $"arr_station"))
      .withColumn("miscl_weight", when($"flight_leg_number".cast("Int") === 1, sum($"flight_baggage_weight.miscl_weight").over(window_leg))
        otherwise UDF.custom_sum("miscl_weight")(collect_list(struct($"flight_leg_number".cast("Int").as("flight_leg_number"), $"flight_baggage_weight.off_point", $"flight_baggage_weight.miscl_weight"))
        .over(window_leg), $"arr_station"))
      .where($"arr_station" === $"flight_baggage_weight.off_point")
      .withColumn("latest_status", when(datediff(current_date(), to_date($"tibco_messageTime")) <= args.date_offset, "Y") otherwise "N")
      .drop("flight_baggage_weight", "flight_leg_number")
    out_df
  }
}
