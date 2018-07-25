/*----------------------------------------------------------
 * Created on  : 29/04/2018
 * Author      : Krishnaraj Rajagopal
 * Email       : krishnaraj.rajagopal@dnata.com
 * Version     : 1.1
 * Project     : Helix-OpsEfficnecy
 * Filename    : SparkUtil.scala
 * Description : Class file for Flight details actual processor
 * ----------------------------------------------------------
 */

package com.emirates.helix.emfg

import com.emirates.helix.emfg.io.FlightDetailsActualArgs._
import com.emirates.helix.emfg.util.{SparkUtils, FightDetailsActualUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.struct

/**
  * Companion object for FlightDetailsActualCoreProcessor class
  */
object FlightDetailsActualProcessor {

  /**
    * Flight details actual Processor instance creation
    *
    * @param args user parameter instance
    * @return FlightDetailsActualCoreProcessor instance
    */
  def apply(args: Args): FlightDetailsActualProcessor = {
    var processor = new FlightDetailsActualProcessor()
    processor.args = args
    processor
  }
}

/**
  * FlightDetailsActualCoreProcessor
  *
  */
class FlightDetailsActualProcessor extends SparkUtils with FightDetailsActualUtils {
  private lazy val logger: Logger = Logger.getLogger(FlightDetailsActualProcessor.getClass)
  private var args: Args = _

  /**
    * Used to process Ags related fields for actual flight details
    *
    * @param ags_deduped_df   Ags deduped input path
    * @param flight_master_df Flight master input path
    * @param spark
    * @return
    */
  def processAgsAndCore(ags_deduped_df: DataFrame, flight_master_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    flight_master_df.createOrReplaceTempView("flight_master_df_tab")

    ags_deduped_df.createOrReplaceTempView("ags_deduped_df_tab")

    val ags_latest_df = spark.sql("select * from (select PK_Date,PK_FlNum,PK_Tail,PK_Origin,PK_Dest,TrackDistanceAmended as grnd_distance,AccAirDist as ESAD,FileName,cast(RowNumber as int) Row_Number,max(cast(RowNumber as int)) over(partition by PK_Date,PK_FlNum,PK_Tail,PK_Origin,PK_Dest) as max_RowNumber from ags_deduped_df_tab) A where Row_Number=max_RowNumber")

    ags_latest_df.createOrReplaceTempView("ags_latest_df_tab")

    val ags_flight_master_df = spark.sql("select fm.flight_identifier, fm.flight_number, fm.flight_date, fm.airline_code, fm.actual_reg_number,  fm.act_dep_iata_station, fm.act_arr_iata_station,fm.latest_pln_arr_iata_station, ags.grnd_distance,ags.ESAD,ags.FileName,fm.flight_blocktime_act,round(((cast(grnd_distance AS FLOAT) - cast(ESAD AS FLOAT))/(cast(flight_blocktime_act as long)/60)),2) as Windspeed, fm.taxi_in as taxi_in_mins, fm.taxi_out as taxi_out_mins,fm.flight_blocktime_act as block_time_mins,cast(cast(CASE WHEN fm.act_flight_on_datetime_utc IS NOT NULL AND fm.act_flight_off_datetime_utc IS NOT NULL THEN ((unix_timestamp(fm.act_flight_on_datetime_utc) - unix_timestamp(fm.act_flight_off_datetime_utc))/60) ELSE null end as int) as string) as trip_time_mins from flight_master_df_tab fm left outer join ags_latest_df_tab ags on PK_FlNum = flight_number and PK_Date = (substr(fm.act_flight_off_datetime_utc,1,10)) and PK_Tail = actual_reg_number and PK_Origin = act_dep_iata_station and PK_Dest = act_arr_iata_station and 'EK' = airline_code where fm.flight_identifier is not null")

    ags_flight_master_df.createOrReplaceTempView("ags_flight_master_latest_df_tab")

    val actual_flight_details_ags_core_df = spark.sql("select flight_identifier, flight_number, flight_date, airline_code, actual_reg_number,  act_dep_iata_station, act_arr_iata_station,latest_pln_arr_iata_station,struct(grnd_distance, ESAD, Windspeed, FileName) as flight_act_distance_details, struct(taxi_in_mins, taxi_out_mins, trip_time_mins, block_time_mins) as flight_act_time_details from ags_flight_master_latest_df_tab")

    return actual_flight_details_ags_core_df

  }

  /**
    *
    * Used to process Ultramain related fields for actual flight details
    *
    * @param um_deduped_df Ultramain deduped input path
    * @param spark
    * @return
    */
  def processUltramain(um_deduped_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    um_deduped_df.createOrReplaceTempView("um_deduped_tab")

    val um_pivot_df = spark.sql("SELECT U__FLIGHT_NO,U__SERIAL,SCH_DEP_DATE,U__DEPART_LOC,U__ARRIVE_LOC,U__CARRIER,U__FIELD_ID,REL_OBJ_ID,FIELD_ID,FIELD_VALUE,ADDED_BY,CHG_DATE_TIME,ADD_DATE_TIME,CASE WHEN trim(FIELD_ID) = 'fuel-sg' THEN FIELD_VALUE END AS fuel_specific_gravity,CASE WHEN trim(FIELD_ID) = 'fuel-uplift' THEN FIELD_VALUE END AS act_fuel_uplifted_litres, CASE WHEN trim(FIELD_ID) = 'fuel-uplift-uom' THEN FIELD_VALUE END AS `fuel-uplift-uom`, CASE WHEN trim(FIELD_ID) = 'waterpaft' THEN FIELD_VALUE END AS act_potable_water_aft_litres, CASE WHEN trim(FIELD_ID) = 'waterpdaft' THEN FIELD_VALUE END AS act_potable_water_pdaft_litres, CASE WHEN CHG_DATE_TIME IS NOT NULL THEN substr(CHG_DATE_TIME,1,19) ELSE substr(ADD_DATE_TIME,1,19) END as um_recorded_datetime FROM um_deduped_tab")

    um_pivot_df.createOrReplaceTempView("um_pivot_tab")

    val um_rel_df = spark.sql("select U__FLIGHT_NO,U__SERIAL,SCH_DEP_DATE,U__DEPART_LOC,U__ARRIVE_LOC,U__CARRIER,REL_OBJ_ID, max(fuel_specific_gravity) as fuel_specific_gravity,max(`fuel-uplift-uom`) as `fuel-uplift-uom`,max(act_fuel_uplifted_litres) as act_fuel_uplifted_litres, max(act_potable_water_pdaft_litres) as act_potable_water_pdaft_litres, max(act_potable_water_aft_litres) as act_potable_water_aft_litres, max(um_recorded_datetime) as um_recorded_datetime from um_pivot_tab group by U__FLIGHT_NO,U__SERIAL,SCH_DEP_DATE,U__DEPART_LOC,U__ARRIVE_LOC,U__CARRIER,REL_OBJ_ID")

    um_rel_df.createOrReplaceTempView("um_rel_tab")

    val actual_flt_details_um_df = spark.sql("select U__FLIGHT_NO,U__SERIAL,SCH_DEP_DATE,U__DEPART_LOC,U__ARRIVE_LOC,U__CARRIER,struct(fuel_specific_gravity,CASE WHEN trim(`fuel-uplift-uom`) = 'LTS' THEN round((act_fuel_uplifted_litres * 0.2641720524),2) END as act_fuel_uplifted_gal,act_fuel_uplifted_litres, act_potable_water_pdaft_litres, act_potable_water_aft_litres,um_recorded_datetime) as flight_act_uplift_details from um_rel_tab")

    return actual_flt_details_um_df
  }

  /**
    *
    * Used to process MFP related fields for actual flight details
    *
    * @param mfp_deduped_df MFP deduped input path
    * @param spark
    * @return
    */

  def processMFP(mfp_deduped_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    mfp_deduped_df.createOrReplaceTempView("mfp_deduped_tab")

    val flight_act_mfp_details_df = spark.sql("select FLIGHT_NUMBER,to_date(FLIGHT_DATE) as FLIGHT_DATE,FROM_SECTOR,TO_SECTOR, collect_list(struct(PDF_FILE_NAME as mfp_file_name ,FILE_PATH as mfp_file_path,NUMBER_OF_PAGES as mfp_page_count,SOURCE_FOLDER as mfp_source_folder,FOLDER_INDEX as mfp_folder_index,VALID_MFP as mfp_valid_ind,EXTRA_REASON_CODE as mfp_fuel_reason_code,EXTRA_REASON_COMMENT as mfp_fuel_reason_comment,CAP_STAFF_NUMBER as captain_staff_id,CAP_NAME as captain_staff_name,PUBLISHED_DATE as mfp_published_datetime,SEQID as mfp_source_sequence_id)) as flight_act_mfp_details from mfp_deduped_tab group by FLIGHT_NUMBER,to_date(FLIGHT_DATE),FROM_SECTOR,TO_SECTOR")

    flight_act_mfp_details_df.createOrReplaceTempView("flight_act_mfp_details_tab")

    val latest_flight_act_mfp_details_df = spark.sql("select FLIGHT_NUMBER,to_date(FLIGHT_DATE) as FLIGHT_DATE,FROM_SECTOR,TO_SECTOR, struct(PDF_FILE_NAME as mfp_file_name ,FILE_PATH as mfp_file_path,NUMBER_OF_PAGES as mfp_page_count,SOURCE_FOLDER as mfp_source_folder,FOLDER_INDEX as mfp_folder_index,VALID_MFP as mfp_valid_ind,EXTRA_REASON_CODE as mfp_fuel_reason_code,EXTRA_REASON_COMMENT as mfp_fuel_reason_comment,CAP_STAFF_NUMBER as captain_staff_id,CAP_NAME as captain_staff_name,substr(PUBLISHED_DATE,1,19) as mfp_published_datetime,SEQID as mfp_source_sequence_id) as latest_flight_act_mfp_details from mfp_deduped_tab where VALID_MFP='Y'")

    latest_flight_act_mfp_details_df.createOrReplaceTempView("latest_flight_act_mfp_details_tab")

    val flight_details_actual_mfp_df = spark.sql("select mfp.FLIGHT_NUMBER,mfp.FLIGHT_DATE,mfp.FROM_SECTOR,mfp.TO_SECTOR,mfp.flight_act_mfp_details,l_mfp.latest_flight_act_mfp_details from flight_act_mfp_details_tab mfp join latest_flight_act_mfp_details_tab l_mfp on mfp.FLIGHT_NUMBER=l_mfp.FLIGHT_NUMBER and mfp.FLIGHT_DATE=l_mfp.FLIGHT_DATE and mfp.FROM_SECTOR=l_mfp.FROM_SECTOR and mfp.TO_SECTOR=l_mfp.TO_SECTOR")

    return flight_details_actual_mfp_df
  }

  /**
    *
    * Used to process all source fields for actual flight details
    *
    * @param actual_flight_details_ags_core_df AGS and CORE processed data frame
    * @param epic_deduped_df                   EPIC deduped data frame
    * @param actual_flight_details_um_df       Ultamain processed data frame
    * @param actual_flight_details_mfp_df      MFP processed data frame
    * @param fuelmon_deduped_df                Fuelmon deduped data frame
    * @param spark
    * @return
    */

  def generateActualFlightDetails(actual_flight_details_ags_core_df: DataFrame, epic_deduped_df: DataFrame, actual_flight_details_um_df: DataFrame,
                                  actual_flight_details_mfp_df: DataFrame, fuelmon_deduped_df: DataFrame, egds_dedupe_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    actual_flight_details_ags_core_df.createOrReplaceTempView("afd_ags_core_tab")

    epic_deduped_df.createOrReplaceTempView("epic_deduped_tab")

    val epic_joined_df = spark.sql("select fm.*,struct(substr(HOLD_ENTRY,1,19) as hold_entry_time,substr(HOLD_EXIT,1,19) as hold_exit_time, HOLD_AREA as hold_area, EVENT_TYPE as event_type, NEAREST_STAR as nearest_STAR, HOLD_TIME as hold_time_mins, substr(EVENT_TIME,1,19) as epic_recorded_datetime) as flight_act_holding_time from afd_ags_core_tab fm left outer join epic_deduped_tab ED  on cast(fm.flight_number as int) = cast(ED.flight_no as int) and fm.actual_reg_number = ED.ac_regn and to_date(fm.flight_date) = to_date(ED.flight_date) and fm.act_dep_iata_station = ED.depstn_id and fm.act_arr_iata_station = ED.arrstn_id and fm.airline_code = CXCD_ID where fm.flight_number is not null and fm.actual_reg_number is not null and fm.flight_date is not null and fm.act_dep_iata_station is not null and fm.act_arr_iata_station is not null and fm.airline_code is not null")

    epic_joined_df.createOrReplaceTempView("afd_epic_joined_tab")

    actual_flight_details_um_df.createOrReplaceTempView("actual_flight_details_um_tab")

    val um_joined_df = spark.sql("select fm.*,  flight_act_uplift_details   from afd_epic_joined_tab fm   left outer join actual_flight_details_um_tab UM   on cast(fm.flight_number as int) = cast(UM.U__FLIGHT_NO as int)   and fm.actual_reg_number = UM.U__SERIAL   and to_date(fm.flight_date) = to_date(UM.SCH_DEP_DATE)   and fm.act_dep_iata_station = UM.U__DEPART_LOC   and fm.act_arr_iata_station = UM.U__ARRIVE_LOC   and fm.airline_code = UM.U__CARRIER   where fm.flight_number is not null   and fm.actual_reg_number is not null   and fm.flight_date is not null   and fm.act_dep_iata_station is not null   and fm.act_arr_iata_station is not null   and fm.airline_code is not null")

    um_joined_df.createOrReplaceTempView("afd_ultramain_joined_tab")

    actual_flight_details_mfp_df.createOrReplaceTempView("actual_flight_details_mfp_tab")

    val mfp_joined_df = spark.sql("select fm.*, mfp.flight_act_mfp_details, mfp.latest_flight_act_mfp_details from afd_ultramain_joined_tab fm Left outer join actual_flight_details_mfp_tab mfp  on fm.flight_number = mfp.FLIGHT_NUMBER  and fm.flight_date = mfp.FLIGHT_DATE  and fm.act_dep_iata_station = mfp.FROM_SECTOR  and fm.act_arr_iata_station = mfp.TO_SECTOR  where fm.flight_number is not null  and fm.flight_date is not null  and fm.act_dep_iata_station is not null  and fm.act_arr_iata_station is not null")

    mfp_joined_df.createOrReplaceTempView("afd_mfp_joined_tab")

    fuelmon_deduped_df.createOrReplaceTempView("fuelmon_deduped_tab")

    val fuelmon_joined_df = spark.sql("select fm.*,  struct(fl.ACT_CONTINGENCY_USED as contingency_fuel_used_kgs,  fl.CFP_EXTRA as captain_Extra_fuel_kgs,  fl.PCT_CONT_USED as pct_contingency_used_kgs,  fl.ZFW_CORRECTION as zfw_correction_kgs,  fl.WABI_TOF as wabi_takeoff_fuel_kgs,  fl.ADJS_EXTRA_FUEL as adj_extra_fuel,  fl.EXCESS_ARV_FUEL as excess_arrival_fuel,  fl.ADJS_EX_ARV_FUEL as adj_extra_arr_fuel,  fl.EXT_FUEL_PENALTY as extra_fuel_penalty,  fl.FUEL_BURN_SAVED as fuel_burn_saved, substr(fl.MODON,1,19) as fm_recorded_datetime) as flight_act_addnl_fuel_details  from afd_mfp_joined_tab fm  left outer join fuelmon_deduped_tab fl  on cast(fm.flight_number as int) = cast(fl.FLIGHTNO as int)  and fm.actual_reg_number = fl.ACREG  and to_date(fm.flight_date) = to_date(fl.FLIGHT_DATE)  and fm.act_dep_iata_station = fl.DEP_STN  and fm.act_arr_iata_station = fl.ARV_STN")

    egds_dedupe_df.createOrReplaceTempView("egds_dedupe_tab")

    fuelmon_joined_df.createOrReplaceTempView("fuelmon_joined_tab")

    val egds_joined_df = spark.sql("select fm.*, struct(SUBSTRING_INDEX(cast(egds.FUEL_OUT as string),'.',1) as fuel_out_kgs,SUBSTRING_INDEX(egds.FUEL_OFF,'.',1) as fuel_off_kgs,SUBSTRING_INDEX(cast(egds.FUEL_ON as string),'.',1) as fuel_on_kgs,SUBSTRING_INDEX(cast(egds.FUEL_IN as string),'.',1) as fuel_in_kgs,CASE WHEN FUEL_OUT IS NOT NULL AND egds.FUEL_IN IS NOT NULL THEN cast((egds.FUEL_OUT - egds.FUEL_IN) as string) ELSE null END as block_fuel_kgs,CASE WHEN egds.FUEL_OFF IS NOT NULL AND egds.FUEL_ON IS NOT NULL THEN cast((egds.FUEL_OFF - egds.FUEL_ON)as string) ELSE null END  as trip_fuel_kgs, substr(cast(processing_date as string),1,19) as fuel_populated_datetime) as flight_act_fuel_details,struct(cast(egds.zfw as string) as AZFW_kgs,cast(egds.OOOI_FUEL_OUT as string) as fuel_out_kgs,cast(egds.OOOI_FUEL_OFF as string) as fuel_off_kgs,cast(egds.OOOI_FUEL_ON as string) as fuel_on_kgs,cast(egds.OOOI_FUEL_IN as string) as fuel_in_kgs,substr(cast(processing_date as string),1,19) as egds_populated_datetime) as flight_oooi_fuel_details from fuelmon_joined_tab fm left outer join egds_dedupe_tab egds on cast(fm.flight_number as int) = cast(egds.flight_no as int) and fm.actual_reg_number = egds.acreg_no and to_date(fm.flight_date) = to_date(egds.flight_date) and fm.act_dep_iata_station = egds.dep_stn and fm.act_arr_iata_station = egds.arr_stn")

    return egds_joined_df
  }
}