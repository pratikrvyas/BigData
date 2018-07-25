/*----------------------------------------------------------------------------
 * Created on  : 04/04/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DataFrameUtils.scala
 * Description : Extention for DataFrame class
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix.emfg.dfutils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Container class implementing the DataFrame extension
  */
object DataFrameUtils {

  /**
    * Implicit class implementing the DataFrame extension
    *
    */
  implicit class DFFormatter(df: DataFrame)(implicit spark:SparkSession) {

    import spark.implicits._

    /** Extension method for Lido Route dataframe
      * @return DataFrame Spark dataframe
      */
    def formatLidoRoute = df.select(trim($"FliNum").as("FliNum"), trim($"DepAirIat").as("DepAirIat"), to_date(trim($"DatOfOriUtc")).cast("String").as("DatOfOriUtc"),
      trim($"DesAirIat").as("DesAirIat"), trim($"RegOrAirFinNum").as("RegOrAirFinNum"), $"tibco_message_time".as("msg_received_datetime"),
      trim($"FliDupNo").as("lido_version_num"), $"flight_level_crztemp_drvd".as("cruize_temp"), $"flight_level_toctrop_drvd".as("TOCTROP"),
      $"flight_level_avisa_drvd".as("AVISA"), $"flight_level_avtrk_drvd".as("AVTRK"))
      .distinct()

    /** Extension method for Lido Flight dataframe
      * @return DataFrame Spark dataframe
      */
    def formatLidoFlight = df.select(trim($"FliNum").as("FliNum"), trim($"DepAirIat").as("DepAirIat"), to_date(trim($"DatOfOriUtc")).cast("String").as("DatOfOriUtc"),
        trim($"DesAirIat").as("DesAirIat"), trim($"RegOrAirFinNum").as("RegOrAirFinNum"), $"PlaBloTimInMin".as("pln_block_time"),
        $"EstTotFliTimTimOfPlnInMin".as("est_flight_time"), $"TriTimInMin".as("pln_trip_time"), $"ConTimTimForConFueInMin".as("pln_contingency_time"),
        $"PlaHolTimInMin".as("pln_frsv_time"), $"ExtTim1TimOfAddInMin".as("pln_additional_time"),$"TaxOutTimInMin".as("pln_taxi_out_time"),
        $"TaxInTimInMin".as("pln_taxi_in_time"), $"RampTime_drvd".as("pln_ramp_time"), $"EstTotFliTimTimOfPlnInMin".as("est_time_mins"), $"FirstAltFliTimInMin".as("altn_1_time"),
        $"SecondAltFliTimInMin".as("altn_2_time"), $"ThirdAltFliTimInMin".as("altn_3_time"), $"FourthAltFliTimInMin".as("altn_4_time"),
        $"ExtTim1TimOfAddInMin".as("extra_1_time"), $"ExtTim2TimOfAddInMin".as("extra_2_time"), $"ExtTim3TimOfAddInMin".as("extra_3_time"),
        $"ExtTim4TimOfAddInMin".as("extra_4_time"), trim($"FliDupNo").as("lido_version_num"), $"tibco_message_time".as("msg_received_datetime"),
        $"TriFue".as("trip_fuel"), $"ConFue".as("contingency_fuel"), $"PlaHolFue".as("frsv_fuel"), $"TaxFueOut".as("taxi_out_fuel"), $"TaxFueIn".as("taxi_in_fuel"),
        $"BloFue".as("block_fuel"), $"TakeOffFuel_drvd".as("t_o_fuel"), $"RampFuel_drvd".as("ramp_fuel"), $"FirstAltFue".as("altn_1_fuel"),
        $"SecondAltFue".as("altn_2_fuel"),$"ThirdAltFue".as("altn_3_fuel"), $"FourthAltFue".as("altn_4_fuel"), $"AddFue1".as("addl_1_fuel"), $"ReaForAdd1".as("rsn_addl_1_fuel"),
        $"AddFue2".as("addl_2_fuel"), $"ReaForAdd2".as("rsn_addl_2_fuel"), $"AddFue3".as("addl_3_fuel"), $"ReaForAdd3".as("rsn_addl_3_fuel"),
        $"AddFue4".as("addl_4_fuel"), $"ReaForAdd4".as("rsn_addl_4_fuel"), $"EcoFue".as("econ_fuel"), $"FueOveDes".as("fuel_over_destn"), $"TanCap".as("tank_capacity"),
        $"SavInUsd".as("saving_usd"), $"PerCor1000_drvd".as("perf_correction"), $"ConCovIn".as("contingency_coverage"), $"ConPer100".as("contingency_percent"),
        $"EnrFueAltEraIcaFor".as("enr_fuel_altn"), $"NoEnrAltPer100".as("enr_altn_prec"), $"TriFueOneFlBel".as("tripfuel_fl_blw"), $"TriFueCorZfwMin100Kg".as("zfw_1000_minus"),
        $"TriFueCorZfwPlu100Kg".as("zfw_1000_plus"), $"TriDisInNmAir".as("air_dist_nm"), $"AltAtTopOfCli100FeeOr10tMet".as("alt_toc"), $"AltAtTopOfdec100FeeOr10tMet".as("alt_tod"),
        $"GreCirDisInNm".as("great_circle_dist_nm"), $"TriDisInNmGro".as("grnd_dist"), $"SidDisInNm".as("sid_dist_nm"), $"StaDisInNm".as("star_dist_nm"),
        $"RouOptCri".as("route_optimiz"), $"PerCor1000_drvd".as("cruize_Deg"), $"AirSpePro".as("cruize"), $"AveWinComInKts_drvd".as("AVWC"), $"CosIndEcoCru".as("CID"),
        $"Gai".as("gain_loss"), $"Sid".as("SID"), $"SidExiPoi".as("SID_exitpoint"), $"Sta".as("STAR"), $"StaEntPoi".as("STAR_entrypoint"),
        $"NoOfAlt".as("no_altn_stations"),$"FirstAltIca".as("altn_1_icao_code"), $"FirstAltIat".as("altn_1_iata_code"), $"FirstAltFliDisInNm".as("altn_1_dist"),
        $"SecondAltIca".as("altn_2_icao_code"), $"SecondAltIat".as("altn_2_iata_code"), $"SecondAltFliDisInNm".as("altn_2_dist"), $"ThirdAltIca".as("altn_3_icao_code"),
        $"ThirdAltIat".as("altn_3_iata_code"), $"ThirdAltFliDisInNm".as("altn_3_dist"), $"FourthAltIca".as("altn_4_icao_code"), $"FourthAltIat".as("altn_4_iata_code"),
        $"FourthAltFliDisInNm".as("altn_4_dist"), $"NoEnrAltPer100".as("enr_altn_perc"), $"EnrFueAltEraIcaFor".as("enr_altn_fuel"),
        $"touch_down_fuel_drvd".as("touch_down_fuel"), $"additional_fuel_drvd".as("additional_fuel"))
        .withColumn("pln_altn_time", $"altn_1_time").withColumn("altn_fuel", $"altn_1_fuel")

    /** Extension method for Flight master dataframe
      * @return DataFrame Spark dataframe
      */
    def formatFlightmaster = df.select($"flight_identifier", $"flight_number", $"pln_dep_iata_station", $"flight_date", $"latest_pln_arr_iata_station", $"actual_reg_number", $"airline_code",
      $"act_dep_iata_station", $"act_arr_iata_station", $"act_dep_datetime_utc", $"act_arr_datetime_utc").distinct

    /** Extension method for lido weather dataframe
      * @return DataFrame Spark dataframe
      */
    def formatLidoWeather: DataFrame = df.where($"FirstAirportCodeIATA".isNotNull && $"messagetime".isNotNull)
  }
}
