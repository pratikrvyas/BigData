/*----------------------------------------------------------------------------
 * Created on  : 20/01/2018
 * Author      : Krishnaraj Rajagopal(s746216)
 * Email       : krishnaraj.rajagopal@emirates.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlightMasterUtils.scala
 * Description : Util class for Flight Master.
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix.emfg.common

/**
  * The common methods for operation efficiency project will be available in this Trait
  *
  */


import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions.{array, col, lit, udf}
import org.apache.spark.sql.hive.HiveContext

trait FlightMasterUtils {

  val COLUMNLIST_EST_FLIGHT_OUT_DATETIME = List("est_flight_out_datetime_local", "est_flight_out_datetime_utc", "msg_recorded_datetime")

  val COLUMNLIST_EST_FLIGHT_OFF_DATETIME = List("est_flight_off_datetime_local", "est_flight_off_datetime_utc", "msg_recorded_datetime")

  val COLUMNLIST_EST_FLIGHT_IN_DATETIME = List("est_flight_in_datetime_local", "est_flight_in_datetime_utc", "msg_recorded_datetime")

  val COLUMNLIST_EST_FLIGHT_ON_DATETIME = List("est_flight_on_datetime_local", "est_flight_on_datetime_utc", "msg_recorded_datetime")

  val COLUMNLIST_FLIGHT_STATUS_DETAILS = List("flight_status", "flight_status_desc", "msg_recorded_datetime")

  val COLUMNLIST_PLN_REG_DETAILS = List("pln_reg_number", "msg_recorded_datetime")

  val COLUMNLIST_FLIGHT_SI_DEP_REMARKS = List("flight_si_remarks_dep", "msg_recorded_datetime")

  val COLUMNLIST_FLIGHT_SI_ARR_REMARKS = List("flight_si_remarks_arr", "msg_recorded_datetime")

  val COLUMNLIST_FLIGHT_REMARKS = List("flight_remarks", "msg_recorded_datetime")

  val COLUMNLIST_FLIGHT_DOOR_REMARKS = List("door_remark", "msg_recorded_datetime")

  val COLUMNLIST_FLIGHT_DELAY_DETAILS = List("delay_code", "delay_reason", "delay_dept_code", "delay_duration_mins", "delay_iata_station", "delay_icao_station", "delay_type", "delay_posted_datetime", "msg_recorded_datetime")

  val COLUMNLIST_ACT_RUNWAY_DETAILS = List("act_dep_runway", "act_arr_runway")

  val COLUMNLIST_AIRCRAFT_DETAILS = List("aircraft_mfr", "aircraft_type", "aircraft_subtype", "aircraft_config", "aircraft_tot_capcity", "aircraft_F_capacity", "aircraft_J_capacity", "aircraft_Y_capacity", "aircraft_lease_ind", "ETOPS_Ind", "aircraft_version_code", "aircraft_owner", "potable_water_tank_count", "potable_water_tank_capacity_l", "no_engines")

  val FLIGHT_MASTER_OUTPUT_TEMP_TABLE: String = "flight_master_final_tab"

  val FLIGHT_DATE_COLUMN_NAME: String = "flight_date"

  val HISTORY: String = "History"


  /**
    * To convert a set of columns as Map <String,String>
    *
    */
  val asMap: UserDefinedFunction = udf((keys: Seq[String], values: Seq[String]) => keys.zip(values).toMap)

  /**
    * Method to convert struct to Map
    */

  def toMap(df: DataFrame, colList: List[String], newColName: String): DataFrame = {

    val keys = array(colList.map(lit): _*)
    val values = array(colList.map(col): _*)
    val mapDF = df.withColumn(newColName, asMap(keys, values))
    mapDF
  }


  /** Method to write processed data to HDFS
    *
    * @param out_df Dataframe to be written to HDFS
    */
  def writeOutput(out_df: DataFrame, look_back_months: Int, coalesce_value: Int,
                  output_history_location: String,
                  output_current_location: String, tempTableName: String, filterColumnName: String, compressionFormat: String)(implicit sqlContext: HiveContext): Unit = {
    out_df.registerTempTable(tempTableName)

    val max_flight_date = sqlContext.sql("select max(trim(" + filterColumnName + ")) " +
      s" from ${tempTableName}").first().mkString

    val incr_sql = "select * from " + tempTableName + " where to_date(trim(" + filterColumnName + ")) " +
      " >= add_months(to_date('" + max_flight_date + "')," + -1 * look_back_months + ")"

    val hist_sql = "select * from " + tempTableName + " where to_date(trim(" + filterColumnName + ")) " +
      " < add_months(to_date('" + max_flight_date + "')," + -1 * look_back_months + ")"

    val output_df_last_incr = sqlContext.sql(incr_sql).coalesce(coalesce_value)
    val output_df_last_hist = sqlContext.sql(hist_sql).coalesce(coalesce_value)

    output_df_last_hist.write.format("parquet").option("spark.sql.parquet.compression.codec", compressionFormat).save(output_history_location)
    output_df_last_incr.write.format("parquet").option("spark.sql.parquet.compression.codec", compressionFormat).save(output_current_location)

  }
}
