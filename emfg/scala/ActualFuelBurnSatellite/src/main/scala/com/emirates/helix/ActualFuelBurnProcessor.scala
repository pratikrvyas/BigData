/*----------------------------------------------------------------------------
 * Created on  : 02/26/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : ActualFuelBurnProcessor.scala
 * Description : Secondary class file for processing ActualFuelBurn.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.actualfuelburnarguments.ActualFuelBurnArgs._
import com.emirates.helix.util.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.emirates.helix.util.ActualFuelBurnUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * Companion object for ActualFuelBurnProcessor class
  */
object ActualFuelBurnProcessor {

  /**
    * ActualFuelBurnProcessor instance creation
    * @param args user parameter instance
    * @return ActualFuelBurnProcessor instance
    */
  def apply(args: Args): ActualFuelBurnProcessor = {
    var processor = new ActualFuelBurnProcessor()
    processor.args = args
    processor
  }
}

/**
  * ActualFuelBurnProcessor class will read
  * AGS Route deduped data and populate Actual Fuel Burn satellite.
  */
class ActualFuelBurnProcessor extends SparkUtils with ActualFuelBurnUtils {
  private var args: Args = _

  /** Method to read the AGS Route deduped and flight master current data and join both.
    * This method also writes the rejected records to target location
    * @return DataFrame
    */
  def prepareActualFuelBurnBase(actualfuelburn_src_ags_route_dedupe_location : String,
                                flightmaster_current_location : String,
                                actualfuelburn_tgt_ags_route_rejected_location : String, coalesce_value : Int)
                               (implicit spark: SparkSession): DataFrame = {

    val ags_deduped_df = readValueFromHDFS(actualfuelburn_src_ags_route_dedupe_location, "parquet", spark)
    // ags_deduped_df.cache().repartition(coalesce_value * 100).createOrReplaceTempView("ags_deduped_df_tab")
    ags_deduped_df.createOrReplaceTempView("ags_deduped_df_tab")

    val flight_master_df_temp = readValueFromHDFS(flightmaster_current_location, "parquet", spark)
    flight_master_df_temp.createOrReplaceTempView("flight_master_df_temp_tab")
    val flight_master_df = spark.sql("select flight_identifier, flight_number, act_flight_off_datetime_utc, " +
      " actual_reg_number, act_dep_iata_station, act_arr_iata_station, airline_code from flight_master_df_temp_tab")
    flight_master_df.createOrReplaceTempView("flight_master_df_tab")

    val ags_joined_df = spark.sql(" select t.flight_identifier as flight_identifier, t.flight_number as flight_number," +
      " t.actual_reg_number as actual_reg_number, (substr(t.act_flight_off_datetime_utc,1,10)) as flight_date, s.PK_Origin as act_dep_iata_station," +
      " s.PK_Dest as act_arr_iata_station, 'EK' as airline_code, FileName as file_name, trim(s.RowNumber) as seq_no," +
      " s.Latitude as latitude, s.Longitude as longitude, s.LATPC as latitude_received, s.LONPC as longitude_received," +
      " s.BaroSetting as baro_setting, s.GrossWeight as gross_weight, s.FuelBurned as fuel_burned, s.FOB as fob," +
      " s.FOB_T1 as fob_t1, s.FOB_T2 as fob_t2, s.FOB_T3 as fob_t3, s.FOB_T4 as fob_t4, s.FOB_T5 as fob_t5," +
      " s.FOB_T6 as fob_t6, s.FOB_T7 as fob_t7, s.FOB_T8 as fob_t8, s.FOB_T9 as fob_t9, s.FOB_T10 as fob_t10," +
      " s.FOB_T11 as fob_t11, s.FOB_T12 as fob_t12, s.HeadingTrue as true_heading, s.AltStandard as alt_std," +
      " s.AltQNH as alt_qnh, s.CAS as CAS, s.TAS as TAS, s.MACH as mach, s.FlightPhase as phase," +
      " s.TrackDistanceAmended as track_dist, s.N11 as N11, s.N12 as N12, s.N13 as N13, s.N14 as N14, s.IVV as IVV," +
      " s.AirGroundDiscrete as air_grnd_discrete, s.TransitionAltitudeDiscrete as tran_alt_discrete, s.CG as CG," +
      " s.ISADEV as isa_deviation_actuals, s.WindDirectionAmmended as wind_direction_actuals," +
      " s.WindSpeed as wind_speed_actuals, s.TAT as TAT, s.SAT as SAT, s.CrossWindComponent as cr_wind_comp," +
      " s.CrossWindDriftVector as cr_wind_vector, s.WindComponentAGS as wind_comp_actuals," +
      " s.WindComponentNAV as wind_comp_navigation, s.Altitude as altitude_actuals, s.Height as height_actuals," +
      " s.GroundSpeed as gnd_speed_actuals, s.AirDist as air_distance_actuals," +
      " s.TrackDistanceAmended as acc_dist_nm_actuals, s.AccAirDist as acc_air_dist_nm_actuals," +
      " s.AccTime as acc_time_mins from  ags_deduped_df_tab s Left outer join flight_master_df_tab t" +
      " on PK_FlNum = flight_number and PK_Date = (substr(t.act_flight_off_datetime_utc,1,10))" +
      " and PK_Tail = actual_reg_number and PK_Origin = act_dep_iata_station" +
      " and PK_Dest = act_arr_iata_station and 'EK' = airline_code" +
      " where t.flight_identifier is not null ")

    val ags_not_joined_df = spark.sql(" select distinct s.PK_FlNum, s.PK_Date, s.PK_Tail, s.PK_Origin, s.PK_Dest," +
      " 'EK' as airline_code, s.FileName from ags_deduped_df_tab s Left outer join flight_master_df_tab t" +
      " on PK_FlNum = flight_number and PK_Date = (substr(t.act_flight_off_datetime_utc,1,10))" +
      " and PK_Tail = actual_reg_number and PK_Origin = act_dep_iata_station and PK_Dest = act_arr_iata_station" +
      " and 'EK' = airline_code where t.flight_identifier is null ")

    writeToHDFS(ags_not_joined_df, actualfuelburn_tgt_ags_route_rejected_location, "parquet",
      Some("overwrite"), Some(1))

    return ags_joined_df
  }

  /** Method to prepare the complex columns and join
    * 1. flight_act_waypnt_fuel_details
    * 2. flight_act_waypnt_distance_speed_details
    * @return DataFrame
    */
  def processActualFuelBurn(dataFrame: DataFrame, coalesce_value : Int)(implicit spark: SparkSession): DataFrame = {

    // import spark.sqlContext.implicits._
    dataFrame.createOrReplaceTempView("ags_joined_df_tab")
    // dataFrame.cache().repartition(coalesce_value * 100).createOrReplaceTempView("ags_joined_df_tab")
    // spark.cache().repartition(coalesce_value * 1000)
    // spark.catalog.cacheTable("ags_joined_df_tab")

    // Building flight_act_waypnt_fuel_details
    val flight_act_waypnt_fuel_details_df1 = spark.sql("select  flight_identifier, flight_number, " +
      "actual_reg_number, flight_date, act_dep_iata_station, act_arr_iata_station, airline_code, file_name, seq_no, " +
      "latitude, longitude, latitude_received, longitude_received, baro_setting, gross_weight, fuel_burned, " +
      "fob, fob_t1, fob_t2, fob_t3, fob_t4, fob_t5, fob_t6, fob_t7, fob_t8, fob_t9, fob_t10, fob_t11, " +
      "fob_t12 from ags_joined_df_tab")

    val flight_act_waypnt_fuel_details_df2 = toMap(flight_act_waypnt_fuel_details_df1,
      COLUMNLIST_flight_act_waypnt_fuel_details, "flight_act_waypnt_fuel_details")

    flight_act_waypnt_fuel_details_df2.createOrReplaceTempView("flight_act_waypnt_fuel_details_df2_tab")


    val flight_act_waypnt_fuel_details_df = spark.sql("select flight_identifier, flight_number, actual_reg_number, " +
      "flight_date, act_dep_iata_station, act_arr_iata_station, airline_code ," +
      " collect_list(flight_act_waypnt_fuel_details) as flight_act_waypnt_fuel_details  " +
      " from flight_act_waypnt_fuel_details_df2_tab group by flight_identifier, flight_number, actual_reg_number," +
      " flight_date, act_dep_iata_station, act_arr_iata_station, airline_code")

    flight_act_waypnt_fuel_details_df.createOrReplaceTempView("flight_act_waypnt_fuel_details_df_tab")

    // Building flight_act_waypnt_distance_speed_details
    val flight_act_waypnt_distance_speed_details_df1 = spark.sql(" select  flight_identifier, flight_number," +
      " actual_reg_number, flight_date, act_dep_iata_station, act_arr_iata_station, airline_code, file_name, seq_no, latitude," +
      " longitude, latitude_received, longitude_received, true_heading, baro_setting, alt_std, alt_qnh, CAS, TAS," +
      " mach, phase, track_dist, N11, N12, N13, N14, IVV, air_grnd_discrete, tran_alt_discrete, CG," +
      " isa_deviation_actuals, wind_direction_actuals, wind_speed_actuals, TAT, SAT, cr_wind_comp, cr_wind_vector," +
      " wind_comp_actuals,  wind_comp_navigation, altitude_actuals, height_actuals, gnd_speed_actuals," +
      " air_distance_actuals, acc_dist_nm_actuals, acc_air_dist_nm_actuals, acc_time_mins from ags_joined_df_tab ")

    val flight_act_waypnt_distance_speed_details_df2 = toMap(flight_act_waypnt_distance_speed_details_df1,
      COLUMNLIST_flight_act_waypnt_distance_speed_details, "flight_act_waypnt_distance_speed_details")

    flight_act_waypnt_distance_speed_details_df2.createOrReplaceTempView("flight_act_waypnt_distance_speed_details_df2_tab")

    val flight_act_waypnt_distance_speed_details_df = spark.sql("select flight_identifier, flight_number," +
      " actual_reg_number, flight_date, act_dep_iata_station, act_arr_iata_station, airline_code," +
      " collect_list(flight_act_waypnt_distance_speed_details) as flight_act_waypnt_distance_speed_details" +
      " from flight_act_waypnt_distance_speed_details_df2_tab group by flight_identifier, flight_number, actual_reg_number," +
      " flight_date, act_dep_iata_station, act_arr_iata_station, airline_code")

    flight_act_waypnt_distance_speed_details_df.createOrReplaceTempView("flight_act_waypnt_distance_speed_details_df_tab")

    // join all together
    val joined_df = spark.sql("select s1.flight_identifier, s1.flight_number, s1.actual_reg_number, s1.flight_date," +
      " s1.act_dep_iata_station, s1.act_arr_iata_station, s1.airline_code," +
      " s1.flight_act_waypnt_fuel_details as flight_act_waypnt_fuel_details," +
      " s2.flight_act_waypnt_distance_speed_details as flight_act_waypnt_distance_speed_details" +
      " from flight_act_waypnt_fuel_details_df_tab s1" +
      " left outer join flight_act_waypnt_distance_speed_details_df_tab s2" +
      " on s1.flight_identifier = s2.flight_identifier and s1.flight_number = s2.flight_number" +
      " and s1.actual_reg_number = s2.actual_reg_number and s1.flight_date = s2.flight_date" +
      " and s1.act_dep_iata_station = s2.act_dep_iata_station and s1.act_arr_iata_station = s2.act_arr_iata_station" +
      " and s1.airline_code = s2.airline_code")

    return joined_df
  }

  /** Method to write the  data to HDFS
    * @param out_df Dataframe to be written to HDFS
    */
  def writeActualFuelBurn(out_df: DataFrame,actualfuelburn_tgt_history_location : String,
                          actualfuelburn_tgt_current_location : String,
                          coalesce_value : Int, lookback_months: Int)
                         (implicit spark: SparkSession): Unit = {

    // out_df.cache().repartition(coalesce_value * 100).createOrReplaceTempView("actual_fuel_burn_tab")

    out_df.createOrReplaceTempView("actual_fuel_burn_tab")

    val max_flight_date = spark.sql("select max(flight_date) from actual_fuel_burn_tab limit 1").first().mkString

    val incr_sql = "select * from actual_fuel_burn_tab where to_date(flight_date) >= " +
      "add_months(to_date('" + max_flight_date + "')," + -1 * lookback_months + ")"
    val hist_sql = "select * from actual_fuel_burn_tab where to_date(flight_date) < " +
      "add_months(to_date('" + max_flight_date + "')," + -1 * lookback_months + ") "

    val output_df_last_incr = spark.sql(incr_sql).coalesce(coalesce_value)
    val output_df_last_hist = spark.sql(hist_sql).coalesce(coalesce_value)

    writeToHDFS(output_df_last_hist, actualfuelburn_tgt_history_location, "parquet",
      Some("error"), Some(coalesce_value))

    writeToHDFS(output_df_last_incr, actualfuelburn_tgt_current_location, "parquet",
      Some("error"), Some(coalesce_value))
  }
}