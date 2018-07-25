/*----------------------------------------------------------------------------
 * Created on  : 04/05/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : SatelliteFields.scala
 * Description : Class file with method for constructing the final satellite dataframe
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix.emfg.lido

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.emirates.helix.emfg.udf.UDF._

/**
  * SatelliteFields class with processing methods
  */
object SatelliteFields {

  /** Method for creating the final dataframe for planned flight details by doing the joins with different sources
    *
    * @param fm Dataframe represents flight master data
    * @param flightlevel Dataframe represents lido flight deduped data
    * @param routelevel Dataframe represents lido route deduped data
    * @param weather Dataframe represents lido weather deduped data
    * @param spark Spark session
    * @return DataFrame Spark dataframe for satellite table
    */
  def getFlightdetals(fm: DataFrame, flightlevel: DataFrame, routelevel: DataFrame, weather: DataFrame)(implicit spark: SparkSession) : DataFrame = {

    import spark.implicits._
    val dep_airport = fm.select($"act_dep_iata_station").distinct().collect.map(_.getAs[String]("act_dep_iata_station")).toList
    val arr_airport = fm.select($"act_arr_iata_station").distinct().collect.map(_.getAs[String]("act_arr_iata_station")).toList
    val airport_list = (dep_airport ++ arr_airport).distinct
    val weather_upd = weather.filter(trim($"FirstAirportCodeIATA").isin(airport_list:_*)).where(trim($"FirstAirportCodeIATA") =!= "XXX").cache()
    //First join - lido flight level and lido route level
    val df_fl = flightlevel.alias("flightlevel").join(routelevel.alias("routelevel"), flightlevel("FliNum") === routelevel("FliNum") &&
      flightlevel("DepAirIat") === routelevel("DepAirIat") && flightlevel("DatOfOriUtc") === routelevel("DatOfOriUtc") &&
      flightlevel("DesAirIat") === routelevel("DesAirIat") && flightlevel("RegOrAirFinNum") === routelevel("RegOrAirFinNum") &&
      flightlevel("msg_received_datetime") === routelevel("msg_received_datetime") && flightlevel("lido_version_num") === routelevel("lido_version_num"), "inner")
      .select($"flightlevel.FliNum", $"flightlevel.DepAirIat", $"flightlevel.DatOfOriUtc", $"flightlevel.DesAirIat", $"flightlevel.RegOrAirFinNum",
        struct($"flightlevel.pln_block_time", $"flightlevel.est_flight_time", $"flightlevel.pln_trip_time", $"flightlevel.pln_contingency_time",
          $"flightlevel.pln_altn_time", $"flightlevel.pln_frsv_time", $"flightlevel.pln_additional_time", $"flightlevel.pln_taxi_out_time", $"flightlevel.pln_taxi_in_time",
          $"flightlevel.pln_ramp_time", $"flightlevel.est_time_mins", $"flightlevel.altn_1_time", $"flightlevel.altn_2_time", $"flightlevel.altn_3_time",
          $"flightlevel.altn_4_time", $"flightlevel.extra_1_time", $"flightlevel.extra_2_time", $"flightlevel.extra_3_time", $"flightlevel.extra_4_time",
          $"flightlevel.lido_version_num", $"flightlevel.msg_received_datetime",
          (when($"flightlevel.msg_received_datetime".rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"),$"flightlevel.msg_received_datetime"
            .cast(TimestampType).cast(LongType)) otherwise $"flightlevel.msg_received_datetime".cast(LongType)).as("timestamp")).as("flight_pln_time_details"),
        struct($"flightlevel.touch_down_fuel", $"flightlevel.trip_fuel", $"flightlevel.contingency_fuel", $"flightlevel.altn_fuel",
          $"flightlevel.frsv_fuel", $"flightlevel.additional_fuel", $"flightlevel.taxi_out_fuel", $"flightlevel.taxi_in_fuel", $"flightlevel.block_fuel",
          $"flightlevel.t_o_fuel", $"flightlevel.ramp_fuel", $"flightlevel.altn_1_fuel", $"flightlevel.altn_2_fuel", $"flightlevel.altn_3_fuel",
          $"flightlevel.altn_4_fuel", $"flightlevel.addl_1_fuel", $"flightlevel.rsn_addl_1_fuel", $"flightlevel.addl_2_fuel", $"flightlevel.rsn_addl_2_fuel",
          $"flightlevel.addl_3_fuel", $"flightlevel.rsn_addl_3_fuel", $"flightlevel.addl_4_fuel", $"flightlevel.rsn_addl_4_fuel",
          $"flightlevel.econ_fuel", $"flightlevel.fuel_over_destn", $"flightlevel.tank_capacity", $"flightlevel.saving_usd", $"flightlevel.perf_correction",
          $"flightlevel.contingency_coverage", $"flightlevel.contingency_percent", $"flightlevel.enr_fuel_altn", $"flightlevel.enr_altn_prec", $"flightlevel.tripfuel_fl_blw",
          $"flightlevel.zfw_1000_minus", $"flightlevel.zfw_1000_plus", $"flightlevel.lido_version_num", $"flightlevel.msg_received_datetime",
          (when($"flightlevel.msg_received_datetime".rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"),$"flightlevel.msg_received_datetime"
            .cast(TimestampType).cast(LongType)) otherwise $"flightlevel.msg_received_datetime".cast(LongType)).as("timestamp")).as("flight_pln_fuel_details"),
        struct($"flightlevel.air_dist_nm", $"flightlevel.alt_toc", $"flightlevel.alt_tod", $"flightlevel.great_circle_dist_nm",
          $"flightlevel.grnd_dist", $"flightlevel.sid_dist_nm", $"flightlevel.star_dist_nm", $"flightlevel.route_optimiz", $"flightlevel.cruize_Deg",
          $"flightlevel.cruize", $"routelevel.cruize_temp", $"routelevel.TOCTROP", $"routelevel.AVTRK", $"flightlevel.AVWC",
          $"routelevel.AVISA", $"flightlevel.CID", $"flightlevel.gain_loss", $"flightlevel.SID", $"flightlevel.SID_exitpoint",
          $"flightlevel.STAR", $"flightlevel.STAR_entrypoint", $"flightlevel.no_altn_stations", $"flightlevel.lido_version_num", $"flightlevel.msg_received_datetime",
          (when($"flightlevel.msg_received_datetime".rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"),$"flightlevel.msg_received_datetime"
            .cast(TimestampType).cast(LongType)) otherwise $"flightlevel.msg_received_datetime".cast(LongType)).as("timestamp")).
          as("flight_pln_distance_details"),
        struct($"flightlevel.altn_1_icao_code", $"flightlevel.altn_1_iata_code", $"flightlevel.altn_1_dist", $"flightlevel.altn_1_time",
          $"flightlevel.altn_1_fuel", $"flightlevel.altn_2_icao_code", $"flightlevel.altn_2_iata_code", $"flightlevel.altn_2_dist", $"flightlevel.altn_2_time",
          $"flightlevel.altn_2_fuel", $"flightlevel.altn_3_icao_code", $"flightlevel.altn_3_iata_code", $"flightlevel.altn_3_dist", $"flightlevel.altn_3_time",
          $"flightlevel.altn_3_fuel", $"flightlevel.altn_4_icao_code", $"flightlevel.altn_4_iata_code", $"flightlevel.altn_4_dist", $"flightlevel.altn_4_time",
          $"flightlevel.altn_4_fuel", $"flightlevel.enr_altn_prec", $"flightlevel.enr_altn_fuel", $"flightlevel.lido_version_num", $"flightlevel.msg_received_datetime",
          (when($"flightlevel.msg_received_datetime".rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"),$"flightlevel.msg_received_datetime"
            .cast(TimestampType).cast(LongType)) otherwise $"flightlevel.msg_received_datetime".cast(LongType)).as("timestamp"))
          .as("flight_pln_alternate_details"))
      .groupBy($"FliNum", $"DepAirIat", $"DatOfOriUtc", $"DesAirIat", $"RegOrAirFinNum")
      .agg(collect_set($"flight_pln_time_details").as("flight_pln_time_details"), collect_set($"flight_pln_fuel_details").as("flight_pln_fuel_details"),
        collect_set($"flight_pln_distance_details").as("flight_pln_distance_details"), collect_set($"flight_pln_alternate_details").as("flight_pln_alternate_details"))

    //Joining the flight level columns from previous step from lido with flight master
    val out_join1 = fm.alias("fm").join(df_fl.alias("df_fl"), fm("flight_number") === df_fl("FliNum") && fm("pln_dep_iata_station") === df_fl("DepAirIat") &&
      fm("flight_date") === df_fl("DatOfOriUtc") && fm("latest_pln_arr_iata_station") === df_fl("DesAirIat") &&
      fm("actual_reg_number") === df_fl("RegOrAirFinNum"), "left_outer")
      .select($"fm.*", $"df_fl.flight_pln_time_details", $"df_fl.flight_pln_fuel_details", $"df_fl.flight_pln_distance_details",
        $"df_fl.flight_pln_alternate_details")

    //Joining the arriving airport weather details
    val out_join2 = out_join1.alias("out_join1").join(weather_upd.alias("weather_upd"), out_join1("act_arr_iata_station") === trim(weather_upd("FirstAirportCodeIATA")) &&
      weather_upd("messagetime").cast(TimestampType).cast(LongType) >= out_join1("act_arr_datetime_utc").cast(TimestampType).cast(LongType) - 3600 &&
      weather_upd("messagetime").cast(TimestampType).cast(LongType) <= out_join1("act_arr_datetime_utc").cast(TimestampType).cast(LongType), "left_outer")
      .select($"out_join1.*", struct($"weather_upd.messagetext".as("arr_metar_message"), date_format($"weather_upd.messagetime","yyyy-MM-dd HH:mm:ss").as("arr_metar_message_time"),
      $"weather_upd.FirstAirportCodeIATA".as("arr_metar_station_iata_code"), $"weather_upd.FirstAirportCodeICAO".as("arr_metar_station_icao_code"),
      date_format($"weather_upd.tibco_message_time","yyyy-MM-dd HH:mm:ss").as("arr_metar_received_time"),
      $"weather_upd.tibco_message_time".cast(TimestampType).cast(LongType).as("timestamp")).as("flight_arr_weather_message"))

    val out_final = out_join2.alias("out_join2").join(weather_upd.alias("weather_upd"), out_join2("act_dep_iata_station") === trim(weather_upd("FirstAirportCodeIATA")) &&
      weather_upd("messagetime").cast(TimestampType).cast(LongType) >= out_join2("act_dep_datetime_utc").cast(TimestampType).cast(LongType) - 3600 &&
      weather_upd("messagetime").cast(TimestampType).cast(LongType) <= out_join2("act_dep_datetime_utc").cast(TimestampType).cast(LongType), "left_outer")
      .select($"out_join2.*", struct($"weather_upd.messagetext".as("dep_metar_message"), date_format($"weather_upd.messagetime", "yyyy-MM-dd HH:mm:ss").as("dep_metar_message_time"),
        $"weather_upd.FirstAirportCodeIATA".as("dep_metar_station_iata_code"), $"weather_upd.FirstAirportCodeICAO".as("dep_metar_station_icao_code"),
        date_format($"weather_upd.tibco_message_time","yyyy-MM-dd HH:mm:ss").as("dep_metar_received_time"),
        $"weather_upd.tibco_message_time".cast(TimestampType).cast(LongType).as("timestamp")).as("flight_dep_weather_message"))
      .groupBy($"flight_identifier", $"flight_number", $"pln_dep_iata_station", $"flight_date", $"latest_pln_arr_iata_station", $"actual_reg_number", $"airline_code")
      .agg(first($"flight_pln_time_details",true).as("flight_pln_time_details"), first($"flight_pln_fuel_details",true).as("flight_pln_fuel_details"),
        first($"flight_pln_distance_details",true).as("flight_pln_distance_details"),
        first($"flight_pln_alternate_details",true).as("flight_pln_alternate_details"),
        collect_set($"flight_arr_weather_message").as("flight_arr_weather_message"),
        collect_set($"flight_dep_weather_message").as("flight_dep_weather_message"))
      .where((datediff($"flight_date", to_date(to_utc_timestamp(current_timestamp(),"Asia/Dubai")))) <= 0 )
      .where($"flight_pln_time_details".isNotNull || $"flight_pln_fuel_details".isNotNull || $"flight_pln_distance_details".isNotNull || $"flight_pln_alternate_details".isNotNull ||
      $"flight_arr_weather_message".isNotNull || $"flight_dep_weather_message".isNotNull)
      .withColumn("latest_flight_pln_time_details", when($"flight_pln_time_details".isNotNull, getLatestLidoTime($"flight_pln_time_details"))
        otherwise lit(null))
      .withColumn("latest_flight_pln_fuel_details", when($"flight_pln_fuel_details".isNotNull, getLatestLidoFuel($"flight_pln_fuel_details"))
        otherwise lit(null))
      .withColumn("latest_flight_pln_distance_details", when($"flight_pln_distance_details".isNotNull, getLatestLidoDistance($"flight_pln_distance_details"))
        otherwise lit(null))
      .withColumn("latest_flight_pln_alternate_details", when($"flight_pln_alternate_details".isNotNull, getLatestLidoWpAlt($"flight_pln_alternate_details"))
        otherwise lit(null))
      .withColumn("latest_flight_arr_weather_message", when($"flight_arr_weather_message".isNotNull, getLatestWeatherArr($"flight_arr_weather_message"))
        otherwise lit(null))
      .withColumn("latest_flight_dep_weather_message", when($"flight_dep_weather_message".isNotNull, getLatestWeatherDep($"flight_dep_weather_message"))
        otherwise lit(null))
    out_final
  }
}
