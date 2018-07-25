/*----------------------------------------------------------
 * Created on  : 21/03/2018
 * Author      : Krishnaraj Rajagopal
 * Email       : krishnaraj.rajagopal@dnata.com
 * Version     : 1.1
 * Project     : Helix-OpsEfficnecy
 * Filename    : SparkUtil.scala
 * Description : Class file for Flatten Fuel burn Planned Processor
 * ----------------------------------------------------------
 */

package com.emirates.helix.emfg

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import com.emirates.helix.emfg.io.FlattenFuelburnPlannedArgs._
import com.emirates.helix.util.SparkUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Companion object for FlattenFuelburnPlannedProcessor class
  */
object FlattenFuelburnPlannedProcessor {

  /**
    * FlattenFuelburnPlannedProcessor instance creation
    *
    * @param args user parameter instance
    * @return FlattenFuelburnPlannedProcessor instance
    */
  def apply(args: Args): FlattenFuelburnPlannedProcessor = {
    var processor = new FlattenFuelburnPlannedProcessor()
    processor.args = args
    processor
  }
}

/**
  * FlattenFuelburnPlannedProcessor class will flatten Fuel burn planned
  *
  */
class FlattenFuelburnPlannedProcessor extends SparkUtils {
  private var args: Args = _
  private lazy val logger: Logger = Logger.getLogger(FlattenFuelburnPlannedProcessor.getClass)

  /**
    * This method
    *
    * @return DataFrame
    */
  def table_diff(current_df: DataFrame, path: String, table_name: String,
                 table_names: List[String], prev_snap_dates: List[String])(implicit spark: SparkSession): DataFrame = {

    //PREV SNAP SHOT
    logger.info("[INFO] PERFORMING TABLE_DIFF WITH PARAMETERS - DF:" + current_df + " PATH:" + path + " table_name :" + table_name)
    logger.info("TABLE NAMES :" + table_names + " PREVIOUS_SNAP_DATES : " + prev_snap_dates)
    var dt = ""
    table_names.foreach { x =>
      if (x == table_name) {
        logger.info("[INFO] dt :" + dt)
        dt = prev_snap_dates(table_names.indexOf(x))
        logger.info("[INFO] dt :" + dt)
      }
    }
    logger.info("[INFO] dt :" + dt)
    if (dt.isEmpty || dt == "" || dt == null) {
      logger.info("[INFO] NO PREVIOUS FLATTEN SNAPSHOT FOUND HENCE CONSIDERING CURRENT SNAPSHOT ONLY. Count : " + current_df.count)
      return current_df.withColumn("hx_snapshot_date", current_date())
    }
    else {
      logger.info("[INFO] READING PREVIOUS FLATTEN SNAP DATA " + path + "/" + table_name + "/hx_snapshot_date <=" + dt + "...")
      val read_df = spark.read.parquet(path + "/" + table_name).createOrReplaceTempView("read_tbl")
      val prev_df = spark.sql("select * from read_tbl where hx_snapshot_date <= '" + dt + "'").drop("hx_snapshot_date")
      // val prev_df = spark.read.parquet(path + "/" + table_name + "/hx_snapshot_date=" + dt).drop("hx_snapshot_date")
      val diff_df = current_df.except(prev_df)
      logger.info("[INFO]  DELTA DIFF COUNT FORM PREVIOUS FLATTEN SNAPSHOT TO CURRENT SNAPSHOT IS " + diff_df.count)
      return diff_df.withColumn("hx_snapshot_date", current_date())
    }
  }

  /** Method to read the AGS Route deduped and flight master current data to join both.
    * This method also writes the rejected records to target location
    *
    * @return DataFrame
    */
  def processFlattenFuelburnPlannedBase(fuelburnplanned_src_location: String, fuelburnplanned_tgt_serving_location: String,
                                        control_file: String, coalesce_value: Int)
                                       (implicit spark: SparkSession) = {
    import spark.implicits._
    val ctrl_file_df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").load(control_file)
    val table_names = ctrl_file_df.select("table_name").map(r => r.getString(0).toString).collect.toList
    val prev_snap_dates = ctrl_file_df.select("prev_snap_date").map(r => if (r.getString(0) == null) "" else r.getString(0)).collect.toList
    //CURRENT SNAP SHOT
    logger.info("[INFO] GETTING CURRENT DATE FOR THE TODAYS LOADING ...")
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val currentDate = formatter.format(cal.getTime())
    logger.info("[INFO] CURRENT DATE FOR THE TODAYS LOADING IS : " + currentDate)

    //READING DATA
    logger.info("[INFO] READING INPUT DATA FROM THE PATH" + fuelburnplanned_src_location + "...")
    val input_df = readValueFromHDFS(fuelburnplanned_src_location, "parquet", spark)
    logger.info("[INFO] READ FROM THE PATH" + fuelburnplanned_src_location + "is successful")
    val input_df1 = input_df.coalesce(coalesce_value)
    input_df1.createOrReplaceTempView("input_df_tab")

    val flight_pln_waypnt_dist_speed_details_latest_df = spark.sql(
      "SELECT flight_identifier,  "+
            "cast(wpd.waypnt_seq_no as string) as waypnt_seq_no,  "+
            "wpd.waypnt_name,  "+
            "cast(wpd.altitude as string) as altitude,  "+
            "wpd.grnd_dist,  "+
            "wpd.acc_dist,  "+
            "wpd.wind,  "+
            "wpd.wind_comp,  "+
            "wpd.tas,  "+
            "wpd.cas,  "+
            "wpd.mach_no,  "+
            "wpd.isa_dev,  "+
            "wpd.sr_tdev,  "+
            "wpd.mora,  "+
            "wpd.seg_temp,  "+
            "wpd.cross_wind,  "+
            "wpd.air_dist_waypnt as air_dist,  "+
            "wpd.trop,  "+
            "wpd.flight_level,  "+
            "wpd.acc_air_dist_waypnt as acc_air_dist,  "+
            "wpd.tibco_message_time as msg_received_date,  "+
            "wpd.version as lido_version_num "+
      "FROM   (SELECT flight_identifier,  "+
                     "Explode(flight_pln_waypnt_dist_speed_details_latest) AS wpd  "+
              "FROM   input_df_tab) a").dropDuplicates()

    // FINDING DELTA for flight_pln_waypnt_dist_speed_details_latest
    val tbl_diff_flight_pln_waypnt_dist_speed_details_latest_df = table_diff(flight_pln_waypnt_dist_speed_details_latest_df,
      fuelburnplanned_tgt_serving_location, "flight_pln_waypnt_dist_speed_details_latest", table_names, prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    var tbl_diff_flight_pln_waypnt_dist_speed_details_latest_uri = new URI(fuelburnplanned_tgt_serving_location + "/ops_stg_edh_pln_flt_distwaypnt/hx_snapshot_date=" + currentDate).toString

    val flight_pln_waypnt_fuel_time_details_latest_df = spark.sql(
      "SELECT flight_identifier,  "+
              "wpf.waypnt_seq_no,  "+
              "wpf.waypnt_name,  "+
              "wpf.latitude,  "+
              "wpf.longitude,  "+
              "wpf.remaining_fuel,  "+
              "wpf.est_fob_waypnt as est_fob,  "+
              "wpf.est_fbu_waypnt as est_fbu,  "+
              "wpf.stm,  "+
              "wpf.atm,  "+
              "wpf.tibco_message_time as msg_received_date,  "+
              "wpf.version as lido_version_num,  "+
              "wpf.true_heading  "+
        "FROM   (SELECT flight_identifier,  "+
                     "Explode(flight_pln_waypnt_fuel_time_details_latest) AS wpf  "+
                "FROM input_df_tab) a").dropDuplicates()


    // FINDING DELTA for flight_pln_waypnt_fuel_time_details_latest
    val tbl_diff_flight_pln_waypnt_fuel_time_details_latest_df = table_diff(flight_pln_waypnt_fuel_time_details_latest_df,
      fuelburnplanned_tgt_serving_location, "flight_pln_waypnt_fuel_time_details_latest", table_names, prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    var tbl_diff_flight_pln_waypnt_fuel_time_details_latest_uri = new URI(fuelburnplanned_tgt_serving_location + "/ops_stg_edh_pln_flt_fuelwaypnt/hx_snapshot_date=" + currentDate).toString

    // WRITING CURRENT SNAPSHOT
    logger.info("[INFO] WRITING  FLIGHT_PLN_WAYPNT_DIST_SPEED_DETAILS_LATEST")
    writeToHDFS(tbl_diff_flight_pln_waypnt_dist_speed_details_latest_df, tbl_diff_flight_pln_waypnt_dist_speed_details_latest_uri, "parquet", Some("overwrite"), Some(coalesce_value))

    // WRITING CURRENT SNAPSHOT
    logger.info("[INFO] WRITING  FLIGHT_PLN_WAYPNT_FUEL_TIME_DETAILS_LATEST ")
    writeToHDFS(tbl_diff_flight_pln_waypnt_fuel_time_details_latest_df, tbl_diff_flight_pln_waypnt_fuel_time_details_latest_uri, "parquet", Some("overwrite"), Some(coalesce_value))

  }
}