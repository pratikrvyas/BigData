/*----------------------------------------------------------------------------
 * Created on  : 03/05/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlattenPayloadProcessor.scala
 * Description : Secondary class file to process Flatten of Payload satellite
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import com.emirates.helix.flattenpayloadarguments.FlattenPayloadArgs._
import com.emirates.helix.util.SparkUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Companion object for FlattenPaylodProcessor class
  */
object FlattenPayloadProcessor {

  /**
    * FlattenPayloadProcessor instance creation
    * @param args user parameter instance
    * @return FlattenPayloadProcessor instance
    */
  def apply(args: Args): FlattenPayloadProcessor = {
    var processor = new FlattenPayloadProcessor()
    processor.args = args
    processor
  }
}

/**
  * FlattenPayloadProcessor class will
  *
  */
class FlattenPayloadProcessor extends SparkUtils {
  private var args: Args = _
  private lazy val logger:Logger = Logger.getLogger(FlattenPayloadProcessor.getClass)

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
      logger.info("[INFO] NO PREVIOUS FLATTEN SNAPSHOT FOUND HENCE CONSIDERING CURRENT SNAPSHOT ONLY. Count : "+current_df.count)
      return current_df.withColumn("hx_snapshot_date",current_date())
    }
    else {
      logger.info("[INFO] READING PREVIOUS FLATTEN SNAP DATA "+path+"/"+table_name+"/hx_snapshot_date <="+dt+"...")
      val read_df = spark.read.parquet(path + "/" + table_name).createOrReplaceTempView("read_tbl")
      val prev_df = spark.sql("select * from read_tbl where hx_snapshot_date <= '"+ dt +"'").drop("hx_snapshot_date")
      // val prev_df = spark.read.parquet(path + "/" + table_name + "/hx_snapshot_date=" + dt).drop("hx_snapshot_date")
      val diff_df = current_df.except(prev_df)
      // logger.info("[INFO]  DELTA DIFF COUNT FORM PREVIOUS FLATTEN SNAPSHOT TO CURRENT SNAPSHOT IS "+diff_df.count)
      return diff_df.withColumn("hx_snapshot_date",current_date())
    }
  }

  /** Method to read the AGS Route deduped and flight master current data to join both.
    * This method also writes the rejected records to target location
    * @return DataFrame
    */
  def processFlattenPayloadBase(payload_src_location : String, payload_tgt_serving_location: String,
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
    logger.info("[INFO] READING INPUT DATA FROM THE PATH" + payload_src_location + "...")
    val input_df = readValueFromHDFS(payload_src_location,"parquet",spark)
    logger.info("[INFO] READ FROM THE PATH" + payload_src_location + "is successfull")
    val input_df1 = input_df.repartition( coalesce_value * 2 )
    input_df1.createOrReplaceTempView("input_df_tab")
    spark.catalog.cacheTable("input_df_tab")

    // ==========================================================
    // flight_pln_payload_details = ops_stg_edh_flt_pln_payload
    val flight_pln_payload_details_df = spark.sql("select flight_identifier as FLIGHT_IDENTIFIER,  " +
      "flight_pln_payload_details.EZFW as EST_ZFW, flight_pln_payload_details.MZFW as MAX_ZFW,  " +
      "flight_pln_payload_details.ETOW as EST_TOW, flight_pln_payload_details.MTOW as MAX_TOW,  " +
      "flight_pln_payload_details.ELWT as EST_LWT, flight_pln_payload_details.MLWT as MAX_LWT,  " +
      "flight_pln_payload_details.MTAXIWT as MAX_TAXIWT, flight_pln_payload_details.lido_version_num as LIDO_VERSION_NUM,  " +
      "flight_pln_payload_details.msg_received_date as MSG_RECEIVED_DATE from input_df_tab " +
      "where flight_pln_payload_details.EZFW is not null or flight_pln_payload_details.MZFW is not null " +
      "or flight_pln_payload_details.ETOW is not null or flight_pln_payload_details.MTOW is not null " +
      "or flight_pln_payload_details.ELWT is not null or flight_pln_payload_details.MLWT is not null " +
      "or flight_pln_payload_details.MTAXIWT is not null " +
      "or flight_pln_payload_details.lido_version_num is not null").dropDuplicates()

    // FINDING DELTA
    logger.info("[INFO] DELTA  flight_pln_payload_details ")
    val diff_flight_pln_payload_details_df = table_diff(flight_pln_payload_details_df,
      payload_tgt_serving_location,"flight_pln_payload_details",table_names,prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    logger.info("[INFO] BUIDLING URI for flight_pln_payload_details ")
    var diff_flight_pln_payload_details_uri = new URI(payload_tgt_serving_location+"/flight_pln_payload_details/hx_snapshot_date="+currentDate).toString


    // ==========================================================
    // flight_act_payload_details = ops_stg_edh_flt_act_payload

    val flight_act_payload_details_df = spark.sql("select flight_identifier as FLIGHT_IDENTIFIER,   " +
      "flight_act_payload_details.ZFW as ZERO_FUEL_WT, flight_act_payload_details.MZFW as MAX_ZERO_FUEL_WT,  " +
      "flight_act_payload_details.TOW as TAKE_OFF_WT, flight_act_payload_details.RTOW as REGULATED_TAKE_OFF_WT,  " +
      "flight_act_payload_details.LWT as LANDING_WT, flight_act_payload_details.MLWT as MAX_LANDING_WT,   " +
      "flight_act_payload_details.DOW as DRY_OP_WT, flight_act_payload_details.DOI as DRY_OP_INDEX, " +
      " flight_act_payload_details.LIZFW as LIZFW, flight_act_payload_details.MACLIZFW as MACLIZFW,  " +
      "flight_act_payload_details.LITOW as LITOW, flight_act_payload_details.MACTOW as MACTOW,  " +
      "flight_act_payload_details.total_compartment_weight as TOTAL_COMPARTMENT_WEIGHT,  " +
      "flight_act_payload_details.underload as UNDERLOAD, " +
      " flight_act_payload_details.final_version_num as LOADSHEET_VERSION_NUM,  f" +
      "light_act_payload_details.msg_received_date as MSG_RECEIVED_DATE from input_df_tab " +
      "where flight_act_payload_details.ZFW is not null or flight_act_payload_details.MZFW is not null or " +
      "flight_act_payload_details.TOW is not null or flight_act_payload_details.RTOW is not null or " +
      "flight_act_payload_details.LWT is not null or flight_act_payload_details.MLWT  is not null or " +
      "flight_act_payload_details.DOW is not null or flight_act_payload_details.DOI is not null or " +
      "flight_act_payload_details.LIZFW is not null or flight_act_payload_details.MACLIZFW is not null or " +
      "flight_act_payload_details.LITOW is not null or flight_act_payload_details.MACTOW is not null or " +
      "flight_act_payload_details.total_compartment_weight is not null or " +
      "flight_act_payload_details.underload is not null or  " +
      "flight_act_payload_details.final_version_num is not null").dropDuplicates()

    // FINDING DELTA
    logger.info("[INFO] DELTA  flight_act_payload_details ")
    val diff_flight_act_payload_details_df = table_diff(flight_act_payload_details_df,
      payload_tgt_serving_location,"flight_act_payload_details",table_names,prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    logger.info("[INFO] BUIDLING URI for flight_act_payload_details ")
    var diff_flight_act_payload_details_uri = new URI(payload_tgt_serving_location+"/flight_act_payload_details/hx_snapshot_date="+currentDate).toString


    // ==========================================================
    // flight_deadload_details = ops_stg_edh_flt_act_deadload

    val flight_deadload_details_df = spark.sql("select flight_identifier as FLIGHT_IDENTIFIER,  " +
      " flight_deadload_details.baggage_weight as BAGGAGE_WEIGHT,  " +
      "flight_deadload_details.total_excess_bag_weight as TOTAL_EXCESS_BAG_WEIGHT,  " +
      "flight_deadload_details.cargo_weight as CARGO_WEIGHT,  flight_deadload_details.mail_weight as MAIL_WEIGHT, " +
      " flight_deadload_details.transit_weight as TRANSIT_BAG_WEIGHT, flight_deadload_details.miscl_weight as MISCL_WEIGHT, " +
      " flight_deadload_details.total_payload as TOTAL_PAYLOAD,  " +
      "flight_deadload_details.msg_received_date as MSG_RECEIVED_DATE  " +
      "from input_df_tab where flight_deadload_details.baggage_weight is not null " +
      "or  flight_deadload_details.total_excess_bag_weight is not null " +
      "or  flight_deadload_details.cargo_weight is not null " +
      "or   flight_deadload_details.mail_weight is not null " +
      "or  flight_deadload_details.transit_weight is not null " +
      "or flight_deadload_details.miscl_weight is not null " +
      "or   flight_deadload_details.total_payload is not null ").dropDuplicates()

    // FINDING DELTA
    logger.info("[INFO] DELTA  flight_deadload_details ")
    val diff_flight_deadload_details_df = table_diff(flight_deadload_details_df,
      payload_tgt_serving_location,"flight_deadload_details",table_names,prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    logger.info("[INFO] BUIDLING URI for flight_deadload_details ")
    var diff_flight_deadload_details_uri = new URI(payload_tgt_serving_location+"/flight_deadload_details/hx_snapshot_date="+currentDate).toString


    // ==========================================================
    // flight_pax_payload_details = ops_stg_edh_flt_act_pax

    val flight_pax_payload_details_df = spark.sql("select flight_identifier as FLIGHT_IDENTIFIER,   " +
      "flight_pax_payload_details.total_pax_loadsheet_count as TOTAL_PAX_COUNT,  " +
      "flight_pax_payload_details.f_pax_loadsheet_count as F_PAX_LOADSHEET_COUNT,  " +
      "flight_pax_payload_details.j_pax_loadsheet_count as J_PAX_LOADSHEET_COUNT,  " +
      "flight_pax_payload_details.y_pax_loadsheet_count as Y_PAX_LOADSHEET_COUNT,  " +
      "flight_pax_payload_details.infant_count as INFANTS_COUNT, flight_pax_payload_details.PWT as PAX_WEIGHT,  " +
      "flight_pax_payload_details.final_version_num as LOADSHEET_VERSION_NUM,  " +
      "flight_pax_payload_details.msg_received_date as MSG_RECEIVED_DATE from input_df_tab " +
      "where flight_pax_payload_details.total_pax_loadsheet_count is not null " +
      "or  flight_pax_payload_details.f_pax_loadsheet_count is not null " +
      "or   flight_pax_payload_details.j_pax_loadsheet_count is not null " +
      "or  flight_pax_payload_details.y_pax_loadsheet_count is not null " +
      "or  flight_pax_payload_details.infant_count is not null " +
      "or flight_pax_payload_details.PWT is not null " +
      "or flight_pax_payload_details.final_version_num is not null ").dropDuplicates()

    // FINDING DELTA
    logger.info("[INFO] DELTA  flight_pax_payload_details ")
    val diff_flight_pax_payload_details_df = table_diff(flight_pax_payload_details_df,
      payload_tgt_serving_location,"flight_pax_payload_details",table_names,prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    logger.info("[INFO] BUIDLING URI for flight_pax_payload_details ")
    var diff_flight_pax_payload_details_uri = new URI(payload_tgt_serving_location+"/flight_pax_payload_details/hx_snapshot_date="+currentDate).toString


    // ==========================================================
    // flight_act_compartment_details = ops_stg_edh_flt_act_comp_wt

    val flight_act_compartment_details_df = spark.sql("select FLIGHT_IDENTIFIER, " +
      "flight_act_compartment_details_expld.compartment_number as COMP_NO,  " +
      "flight_act_compartment_details_expld.compartment_weight as COMP_WEIGHT, " +
      "flight_act_compartment_details_expld.final_version_num as LOADSHEET_VERSION_NUM, " +
      "flight_act_compartment_details_expld.msg_received_date as MSG_RECEIVED_DATE from " +
      "(select flight_identifier as FLIGHT_IDENTIFIER,  flight_number as FLIGHT_NUMBER, " +
      "act_reg_number AS ACT_REG_NUMBER,  flight_date AS FLIGHT_DATE, dep_iata_station as " +
      "DEP_IATA_STATION,  arr_iata_station as ARR_IATA_STATION, airline_code as AIRLINE_CODE, " +
      "explode(flight_act_compartment_details) as flight_act_compartment_details_expld  from input_df_tab) t ").dropDuplicates()

    // FINDING DELTA
    logger.info("[INFO] DELTA  flight_act_compartment_details ")
    val diff_flight_act_compartment_details_df = table_diff(flight_act_compartment_details_df,
      payload_tgt_serving_location,"flight_act_compartment_details",table_names,prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    logger.info("[INFO] BUIDLING URI for flight_act_compartment_details ")
    var diff_flight_act_compartment_details_uri = new URI(payload_tgt_serving_location+"/flight_act_compartment_details/hx_snapshot_date="+currentDate).toString


    // ============================================================
    // flight_act_loadsheet_message = OPS_STG_EDH_FLT_LOADSHEET

    val flight_act_loadsheet_message_df = spark.sql("select flight_identifier as FLIGHT_IDENTIFIER, " +
      "flight_act_loadsheet_message as LOADSHEET_MESSAGE, flight_act_payload_details.msg_received_date as MSG_RECEIVED_DATE " +
      "from input_df_tab where flight_act_loadsheet_message is not null " +
      "and flight_act_loadsheet_message <> 'null' and flight_act_loadsheet_message <> '' ").dropDuplicates()

    // FINDING DELTA
    logger.info("[INFO] DELTA  flight_act_loadsheet_message ")
    val diff_flight_act_loadsheet_message_df = table_diff(flight_act_loadsheet_message_df,
      payload_tgt_serving_location,"flight_act_loadsheet_message",table_names,prev_snap_dates)

    // BUIDLING CURRENT SNAPSHOT LOCATION
    logger.info("[INFO] BUIDLING URI for flight_act_loadsheet_message ")
    var diff_flight_act_loadsheet_message_uri = new URI(payload_tgt_serving_location+"/flight_act_loadsheet_message/hx_snapshot_date="+currentDate).toString


    // WRITING THE TARGET
    logger.info("[INFO] WRITING  flight_act_loadsheet_message ")
    writeToHDFS(diff_flight_act_loadsheet_message_df,diff_flight_act_loadsheet_message_uri,"parquet",Some("overwrite"), Some(coalesce_value))

    // WRITING THE TARGET
    logger.info("[INFO] WRITING  flight_pln_payload_details ")
    writeToHDFS(diff_flight_pln_payload_details_df,diff_flight_pln_payload_details_uri,"parquet",Some("overwrite"), Some(coalesce_value))

    // WRITING THE TARGET
    logger.info("[INFO] WRITING  flight_deadload_details ")
    writeToHDFS(diff_flight_deadload_details_df,diff_flight_deadload_details_uri,"parquet",Some("overwrite"), Some(coalesce_value))

    // WRITING THE TARGET
    logger.info("[INFO] WRITING  flight_pax_payload_details ")
    writeToHDFS(diff_flight_pax_payload_details_df,diff_flight_pax_payload_details_uri,"parquet",Some("overwrite"), Some(coalesce_value))

    // WRITING THE TARGET
    logger.info("[INFO] WRITING  flight_act_compartment_details ")
    writeToHDFS(diff_flight_act_compartment_details_df,diff_flight_act_compartment_details_uri,"parquet",Some("overwrite"), Some(coalesce_value))

    // WRITING THE TARGET
    logger.info("[INFO] WRITING  flight_act_payload_details ")
    writeToHDFS(diff_flight_act_payload_details_df,diff_flight_act_payload_details_uri,"parquet",Some("overwrite"), Some(coalesce_value))

  }
}