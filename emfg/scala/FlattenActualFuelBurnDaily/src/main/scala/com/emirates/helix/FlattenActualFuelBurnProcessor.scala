/*----------------------------------------------------------------------------
 * Created on  : 03/05/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlattenActualFuelBurnProcessor.scala
 * Description : Secondary class file to process Flatten of Actual Fuel Burn satellite
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import com.emirates.helix.flattenactualfuelburnarguments.FlattenActualFuelBurnArgs._
import com.emirates.helix.util.SparkUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Companion object for ActualFuelBurnProcessor class
  */
object FlattenActualFuelBurnProcessor {

  /**
    * ActualFuelBurnProcessor instance creation
    * @param args user parameter instance
    * @return ActualFuelBurnProcessor instance
    */
  def apply(args: Args): FlattenActualFuelBurnProcessor = {
    var processor = new FlattenActualFuelBurnProcessor()
    processor.args = args
    processor
  }
}

/**
  * FlattenActualFuelBurnProcessor class will
  *
  */
class FlattenActualFuelBurnProcessor extends SparkUtils {
  private var args: Args = _
  private lazy val logger:Logger = Logger.getLogger(FlattenActualFuelBurnProcessor.getClass)

  /**
    * This method
    * @return DataFrame
    */
  def table_diff(current_df : DataFrame,path : String, table_name : String,
                 table_names : List[String],prev_snap_dates : List[String]) (implicit spark: SparkSession) : DataFrame = {

    //PREV SNAP SHOT
    logger.info("[INFO] PERFORMING TABLE_DIFF WITH PARAMETERS - DF:"+current_df+" PATH:"+path+" table_name :"+table_name)
    logger.info("TABLE NAMES :"+table_names+" PREVIOUS_SNAP_DATES : "+prev_snap_dates)
    var dt = ""
    table_names.foreach{x =>
      if(x == table_name){
        logger.info("[INFO] dt :"+dt)
        dt= prev_snap_dates(table_names.indexOf(x))
        logger.info("[INFO] dt :"+dt)
      }
    }
    logger.info("[INFO] dt :"+dt)
    if(dt.isEmpty || dt == "" || dt == null ){
      //logger.info("[INFO] NO PREVIOUS FLATTEN SNAPSHOT FOUND HENCE CONSIDERING CURRENT SNAPSHOT ONLY. Count : "+current_df.count)
      return current_df.withColumn("hx_snapshot_date",current_date())
    }
    else {
      logger.info("[INFO] READING PREVIOUS FLATTEN SNAP DATA "+path+"/"+table_name+"/hx_snapshot_date="+dt+"...")
      val read_df = spark.read.parquet(path + "/" + table_name)
      read_df.repartition(args.coalesce_value * 2).createOrReplaceTempView("read_tbl")
      // read_df.createOrReplaceTempView("read_tbl")
      val prev_df = spark.sql("select * from read_tbl where hx_snapshot_date <= '"+ dt +"'").drop("hx_snapshot_date")
      //val prev_df = spark.read.parquet(path + "/" + table_name + "/hx_snapshot_date=" + dt).drop("hx_snapshot_date")
      val diff_df = current_df.except(prev_df)
      // logger.info("[INFO]  DELTA DIFF COUNT FORM PREVIOUS FLATTEN SNAPSHOT TO CURRENT SNAPSHOT IS "+diff_df.count)
      return diff_df.withColumn("hx_snapshot_date",current_date())
    }
  }

  /** Method to read the AGS Route deduped and flight master current data to join both.
    * This method also writes the rejected records to target location
    * @return DataFrame
    */
  def processFlattenActualFuelBurnBase(actualfuelburn_src_location : String, actualfuelburn_tgt_serving_location: String,
                                       control_file : String, coalesce_value : Int)
                               (implicit spark: SparkSession) = {
    import spark.implicits._
    val ctrl_file_df = spark.read.format("com.databricks.spark.csv").option("header", "true") .option("inferSchema", "false").load(control_file)
    val table_names = ctrl_file_df.select("table_name").map(r => r.getString(0).toString).collect.toList
    val prev_snap_dates = ctrl_file_df.select("prev_snap_date").map(r => if(r.getString(0) == null) "" else r.getString(0)).collect.toList
    //CURRENT SNAP SHOT
    logger.info("[INFO] GETTING CURRENT DATE FOR THE TODAYS LOADING ...")
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val currentDate = formatter.format(cal.getTime())
    logger.info("[INFO] CURRENT DATE FOR THE TODAYS LOADING IS : "+currentDate)

    //READING DATA
    logger.info("[INFO] READING INPUT DATA FROM THE PATH" + actualfuelburn_src_location + "...")
    val input_df = readValueFromHDFS(actualfuelburn_src_location,"parquet",spark)
    logger.info("[INFO] READ FROM THE PATH" + actualfuelburn_src_location + "is successfull")
    // val input_df1 = input_df.repartition(coalesce_value * 10)
    // input_df.repartition(coalesce_value * 5).createOrReplaceTempView("input_df_tab")
    input_df.createOrReplaceTempView("input_df_tab")

    //spark.catalog.cacheTable("input_df_tab")

    // flight_act_waypnt_fuel_details_df complex column flatten
    val flight_act_waypnt_fuel_details_df_temp = spark.sql("select flight_identifier, flight_act_waypnt_fuel_details from input_df_tab").repartition(coalesce_value * 2)
     // logger.info("[INFO]  flight_act_waypnt_fuel_details_df_temp count "+flight_act_waypnt_fuel_details_df_temp.count)
    flight_act_waypnt_fuel_details_df_temp.createOrReplaceTempView("flight_act_waypnt_fuel_details_df_temp_tab")

    val flight_act_waypnt_fuel_details_df = spark.sql("select flight_identifier, fd.file_name, fd.seq_no, fd.latitude," +
      " fd.longitude, fd.latitude_received, fd.longitude_received, fd.baro_setting, fd.gross_weight," +
      " fd.fuel_burned, fd.fob, fd.fob_t1, fd.fob_t2, fd.fob_t3, fd.fob_t4, fd.fob_t5, fd.fob_t6, fd.fob_t7," +
      " fd.fob_t8, fd.fob_t9, fd.fob_t10, fd.fob_t11, fd.fob_t12" +
      "  from (select flight_identifier, explode(flight_act_waypnt_fuel_details)" +
      " as fd from flight_act_waypnt_fuel_details_df_temp_tab) a ")

    // FINDING DELTA for flight_act_waypnt_fuel_details
    val tbl_diff_fuel_details_df = table_diff(flight_act_waypnt_fuel_details_df,
      actualfuelburn_tgt_serving_location,"flight_act_waypnt_fuel_details",table_names,prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    var tbl_diff_fuel_details_uri = new URI(actualfuelburn_tgt_serving_location+"/flight_act_waypnt_fuel_details/hx_snapshot_date="+currentDate).toString

    // flight_act_waypnt_distance_speed_details complex column
    val flight_act_waypnt_distance_speed_details_df_temp = spark.sql("select flight_identifier," +
      " flight_act_waypnt_distance_speed_details from input_df_tab ").repartition(coalesce_value * 2)
    // logger.info("[INFO]  flight_act_waypnt_distance_speed_details_df_temp count "+flight_act_waypnt_distance_speed_details_df_temp.count)
    flight_act_waypnt_distance_speed_details_df_temp.createOrReplaceTempView("flight_act_waypnt_distance_speed_details_df_temp_tab")

    val flight_act_waypnt_distance_speed_details_df = spark.sql("select flight_identifier, dsd.file_name, dsd.seq_no, dsd.latitude," +
      " dsd.longitude, dsd.latitude_received, dsd.longitude_received, dsd.true_heading, " +
      " dsd.alt_std, dsd.alt_qnh, dsd.CAS, dsd.TAS, dsd.mach, dsd.phase, dsd.track_dist, dsd.N11, dsd.N12," +
      " dsd.N13, dsd.N14, dsd.IVV, dsd.air_grnd_discrete, dsd.tran_alt_discrete, dsd.CG, dsd.isa_deviation_actuals," +
      " dsd.wind_direction_actuals, dsd.wind_speed_actuals, dsd.TAT, dsd.SAT, dsd.cr_wind_comp, dsd.cr_wind_vector," +
      " dsd.wind_comp_actuals, dsd. wind_comp_navigation, dsd.altitude_actuals, dsd.height_actuals," +
      " dsd.gnd_speed_actuals, dsd.air_distance_actuals, dsd.acc_dist_nm_actuals, dsd.acc_air_dist_nm_actuals," +
      " dsd.acc_time_mins  from (select flight_identifier, explode(flight_act_waypnt_distance_speed_details)" +
      " as dsd from flight_act_waypnt_distance_speed_details_df_temp_tab) b ")

    // FINDING DELTA for flight_act_waypnt_distance_speed_details
    val tbl_diff_distance_speed_details_df = table_diff(flight_act_waypnt_distance_speed_details_df,
      actualfuelburn_tgt_serving_location,"flight_act_waypnt_distance_speed_details",table_names,prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    var flight_act_waypnt_distance_speed_details_uri = new URI(actualfuelburn_tgt_serving_location+"/flight_act_waypnt_distance_speed_details/hx_snapshot_date="+currentDate).toString

    // WRITING CURRENT SNAPSHOT
    logger.info("[INFO] WRITING  FLIGHT_ACT_WAYPNT_FUEL_DETAILS ")
    writeToHDFS(tbl_diff_fuel_details_df,tbl_diff_fuel_details_uri,"parquet",Some("overwrite"), Some(coalesce_value))

    logger.info("[INFO] WRITING  FLIGHT_ACT_WAYPNT_FUEL_DETAILS ")
    writeToHDFS(tbl_diff_distance_speed_details_df,flight_act_waypnt_distance_speed_details_uri,"parquet",Some("overwrite"), Some(coalesce_value))

  }
}