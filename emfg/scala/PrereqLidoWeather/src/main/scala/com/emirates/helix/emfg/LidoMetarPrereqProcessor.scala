/*----------------------------------------------------------------------------
 * Created on  : 03/26/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : LidoMetarPrereqProcessor.scala
 * Description : Secondary class file for formatting Lido Metar incremental data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix.emfg

import com.emirates.helix.emfg.model.Model._
import com.emirates.helix.emfg.util.SparkUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Companion object for LidoMetarPrereqProcessor class
  */
object LidoMetarPrereqProcessor {
  /**
    * LidoMetarPrereqProcessor instance creation
    * @param args user parameter instance
    * @return LidoMetarPrereqProcessor instance
    */
  def apply(args: LidoMetarPrereqArgs): LidoMetarPrereqProcessor = {
    var processor = new LidoMetarPrereqProcessor()
    processor.args = args
    processor
  }
}
/**
  * LidoMetarPrereqProcessor class with methods to
  * generate schema sync prerequisite data for Lido Metar incremental data.
  */
class LidoMetarPrereqProcessor extends SparkUtils {
  private var args: LidoMetarPrereqArgs = _

  /**  Method performing Lido Metar incremental data formatting
    *
    *  @return DataFrame
    */
  def processData(in_df: DataFrame)(implicit sqlContext:SQLContext):  DataFrame = {
    import sqlContext.implicits._
    in_df.select($"MessageIdent", $"NoofWeatherMessages", $"Seperator", explode($"WeatherInfo").as("WeatherInfo"), $"tibco_message_time", $"helix_uuid", $"helix_timestamp")
      .select($"MessageIdent", $"NoofWeatherMessages", $"Seperator",$"WeatherInfo.EndofValidity", $"WeatherInfo.FirstAirportCodeIATA",
        $"WeatherInfo.FirstAirportCodeICAO", $"WeatherInfo.InputOffice", $"WeatherInfo.Remark", $"WeatherInfo.StartofValidity",
        $"WeatherInfo.TimeofObservationPromulgation", $"WeatherInfo.WeatherIdent", $"WeatherInfo.WeatherText",$"tibco_message_time", $"helix_uuid", $"helix_timestamp")
      .withColumn("tibcodate", to_date(to_utc_timestamp($"tibco_message_time", "Asia/Dubai")))
      .withColumn("tibco_d", dayofmonth($"tibcodate"))
      .withColumn("observation_d", substring($"TimeofObservationPromulgation",1,2))
      .withColumn("weatherdate", when($"tibco_d" >= $"observation_d".cast("Int"), $"tibcodate") otherwise add_months($"tibcodate",-1))
      .withColumn("messagetime", concat_ws(" ", concat_ws("-",date_format($"weatherdate", "yyyy-MM"), $"observation_d"),
        concat_ws(":",substring($"TimeofObservationPromulgation",3,2), substring($"TimeofObservationPromulgation",5,2), lit("00"))))
      .withColumn("messagetext", concat($"FirstAirportCodeICAO", $"FirstAirportCodeIATA", $"WeatherIdent", $"TimeofObservationPromulgation",
      $"Remark", $"InputOffice", $"StartofValidity", $"EndofValidity", regexp_replace($"WeatherText", "=", "")))
      .select($"MessageIdent", $"NoofWeatherMessages", $"Seperator", $"EndofValidity", $"FirstAirportCodeIATA", $"FirstAirportCodeICAO",
        $"InputOffice", $"Remark", $"StartofValidity", $"TimeofObservationPromulgation", $"WeatherIdent",
        $"WeatherText",$"messagetime", $"messagetext", $"tibco_message_time", $"helix_uuid", $"helix_timestamp")
      .withColumn("effective_to", lit(""))
  }
}
