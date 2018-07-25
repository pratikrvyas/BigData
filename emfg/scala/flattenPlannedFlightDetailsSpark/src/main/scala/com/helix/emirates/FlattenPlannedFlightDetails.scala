/*
*=====================================================================================================================================
* Created on     :   17/04/2018
* Author         :   Thangamani Kumaravelu
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   FlattenPlannedFlightDetails
* Description    :   This code flattens planned flight details data and stores in multiple tables
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue opseff --class "com.emirates.helix.FlattenPlannedFlightDetails" /home/ops_eff/ek_flightdetails/flattenPlannedFlightDetailsSpark-1.0-SNAPSHOT.jar -i /data/helix/modelled/emfg/public/flightplanneddetails/current -o /data/helix/modelled/emfg/serving -c snappy -p 2 -s 20

package com.helix.emirates

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlattenPlannedFlightDetails {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("Flatten_Planned_Flight_Details")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Flatten_Planned_Flight_Details")

  //DELTA LOGIC
  def table_diff(current_df : DataFrame,path : String, table_name : String,table_names : List[String],prev_snap_dates : List[String]) : DataFrame = {

    //PREV SNAP SHOT
    var dt = ""
    table_names.foreach{x =>
      if(x == table_name){
        dt= prev_snap_dates(table_names.indexOf(x))
      }
    }

    if(dt.isEmpty || dt == "" || dt == null ){
      log.info("[INFO] NO PREVIOUS FLATTEN SNAPSHOT FOUND HENCE CONSIDERING CURRENT SNAPSHOT ONLY "+current_df.count)
      return current_df.withColumn("hx_snapshot_date",current_date())
    }
    else {
      log.info("[INFO] READING PREVIOUS FLATTEN SNAP DATA "+path+"/"+table_name+"/hx_snapshot_date <="+dt+"...")
      val read_df = sqlContext.read.parquet(path + "/" + table_name).registerTempTable("read_tbl")
      val prev_df = sqlContext.sql("select * from read_tbl where hx_snapshot_date <= '"+ dt +"'").drop($"hx_snapshot_date")
      val diff_df = current_df.except(prev_df)
      log.info("[INFO]  DELTA DIFF COUNT FORM PREVIOUS FLATTEN SNAPSHOT TO CURRENT SNAPSHOT IS "+diff_df.count)
      return diff_df.withColumn("hx_snapshot_date",current_date())
    }

  }

  def flatten_plan_flight_details(input_path : String,output_path : String, shuffle : String, partitions :  Int, compression : String,ctrl_file_path : String)={

    //READING CONTROL FILE
    ///apps/helix/conf/oozie/workspaces/wf_flattenplannedflightdetailsemfg/ctrl_file.prop
    val ctrl_file_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true") .option("inferSchema", "true") .load(ctrl_file_path)
    val table_names = (ctrl_file_df.select("table_name").map(r => r.getString(0).toString).collect.toList)
    val prev_snap_dates = (ctrl_file_df.select("prev_snap_date").map(r => r.getString(0)).collect.toList)

    //CURRENT SNAP SHOT
    log.info("[INFO] GETTING CURRENT DATE FOR THE TODAYS LOADING ...")
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val currentDate = formatter.format(cal.getTime())
    log.info("[INFO] GETTING CURRENT DATE FOR THE TODAYS LOADING IS : "+currentDate)

    //SHUFFLE
    hiveContext.setConf("spark.sql.shuffle.partitions", shuffle)
    hiveContext.setConf("spark.sql.parquet.compression.codec", compression)

    //READING DATA
    log.info("[INFO] READING INPUT DATA FROM THE PATH" + input_path + "...")
    val ip_data = hiveContext.read.parquet(input_path)
    log.info("[INFO] READING INPUT DATA FROM THE PATH" + input_path + "is successfull")
    val ip_data1 = ip_data.repartition(partitions)
    ip_data1.registerTempTable("ip_tbl")
    hiveContext.cacheTable("ip_tbl")

    ip_data1.count

    //EXPLODING REQUIRED COLUMNS

    //##############LATEST_FLIGHT_PLN_TIME_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM LATEST_FLIGHT_PLN_TIME_DETAILS...")
    val pln_time_dtl = hiveContext.sql("select flight_identifier,latest_flight_pln_time_details.pln_block_time as pln_block_time,latest_flight_pln_time_details.est_flight_time as est_flight_time,latest_flight_pln_time_details.pln_trip_time as pln_trip_time,latest_flight_pln_time_details.pln_contingency_time as pln_contingency_time,latest_flight_pln_time_details.pln_altn_time as pln_altn_time,latest_flight_pln_time_details.pln_frsv_time as pln_frsv_time,latest_flight_pln_time_details.pln_additional_time as pln_additional_time,latest_flight_pln_time_details.pln_taxi_out_time as pln_taxi_out_time,latest_flight_pln_time_details.pln_taxi_in_time as pln_taxi_in_time,cast(latest_flight_pln_time_details.pln_ramp_time as string) as pln_ramp_time,latest_flight_pln_time_details.est_time_mins as est_time_mins,latest_flight_pln_time_details.extra_1_time as extra_1_time,latest_flight_pln_time_details.extra_2_time as extra_2_time,latest_flight_pln_time_details.extra_3_time as extra_3_time,latest_flight_pln_time_details.extra_4_time as extra_4_time from ip_tbl")

    //##############LATEST_FLIGHT_PLN_FUEL_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM LATEST_FLIGHT_PLN_FUEL_DETAILS...")
    val pln_fuel_dtl = hiveContext.sql("select flight_identifier,latest_flight_pln_fuel_details.touch_down_fuel as touch_down_fuel,latest_flight_pln_fuel_details.trip_fuel as trip_fuel,latest_flight_pln_fuel_details.altn_fuel as altn_fuel,latest_flight_pln_fuel_details.frsv_fuel as frsv_fuel,latest_flight_pln_fuel_details.additional_fuel as additional_fuel,latest_flight_pln_fuel_details.taxi_out_fuel as taxi_out_fuel,latest_flight_pln_fuel_details.taxi_in_fuel as taxi_in_fuel,latest_flight_pln_fuel_details.block_fuel as block_fuel,cast(latest_flight_pln_fuel_details.t_o_fuel as String) as t_o_fuel,cast(latest_flight_pln_fuel_details.ramp_fuel as string) as ramp_fuel,latest_flight_pln_fuel_details.addl_1_fuel as addl_1_fuel,latest_flight_pln_fuel_details.rsn_addl_1_fuel as rsn_addl_1_fuel,latest_flight_pln_fuel_details.addl_2_fuel as addl_2_fuel,latest_flight_pln_fuel_details.rsn_addl_2_fuel as rsn_addl_2_fuel,latest_flight_pln_fuel_details.addl_3_fuel as addl_3_fuel,latest_flight_pln_fuel_details.rsn_addl_3_fuel as rsn_addl_3_fuel,latest_flight_pln_fuel_details.addl_4_fuel as addl_4_fuel,latest_flight_pln_fuel_details.rsn_addl_4_fuel as rsn_addl_4_fuel,latest_flight_pln_fuel_details.contingency_fuel as contingency_fuel,latest_flight_pln_fuel_details.econ_fuel as econ_fuel,latest_flight_pln_fuel_details.fuel_over_destn as fuel_over_destn,latest_flight_pln_fuel_details.tank_capacity as tank_capacity,latest_flight_pln_fuel_details.saving_usd as saving_usd,latest_flight_pln_fuel_details.perf_correction as perf_correction,latest_flight_pln_fuel_details.contingency_coverage as contingency_coverage,latest_flight_pln_fuel_details.contingency_percent as contingency_percent,latest_flight_pln_alternate_details.enr_altn_fuel as enr_altn_fuel,latest_flight_pln_alternate_details.enr_altn_prec as enr_altn_prec,latest_flight_pln_fuel_details.tripfuel_fl_blw as tripfuel_fl_blw,latest_flight_pln_fuel_details.zfw_1000_minus as zfw_1000_minus,latest_flight_pln_fuel_details.zfw_1000_plus as zfw_1000_plus from ip_tbl")

    //##############LATEST_FLIGHT_PLN_DISTANCE_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM LATEST_FLIGHT_PLN_DISTANCE_DETAILS...")
    val pln_distance_dtl = hiveContext.sql("select flight_identifier,latest_flight_pln_distance_details.air_dist_nm as air_dist_nm,latest_flight_pln_distance_details.alt_toc as alt_toc,latest_flight_pln_distance_details.alt_tod as alt_tod,latest_flight_pln_distance_details.great_circle_dist_nm as great_circle_dist_nm,latest_flight_pln_distance_details.grnd_dist as grnd_dist,latest_flight_pln_distance_details.sid_dist_nm as sid_dist_nm,latest_flight_pln_distance_details.star_dist_nm as star_dist_nm,latest_flight_pln_distance_details.route_optimiz as route_optimiz,latest_flight_pln_distance_details.cruize_Deg as cruize_Deg,latest_flight_pln_distance_details.cruize as cruize,latest_flight_pln_distance_details.cruize_temp as cruize_temp,latest_flight_pln_distance_details.TOCTROP as toctrop,latest_flight_pln_distance_details.AVTRK as avtrk,latest_flight_pln_distance_details.AVWC as avwc,latest_flight_pln_distance_details.AVISA as avisa,latest_flight_pln_distance_details.CID as cid,latest_flight_pln_distance_details.gain_loss as gain_loss,latest_flight_pln_distance_details.SID as sid,latest_flight_pln_distance_details.SID_exitpoint as sid_exitpoint,latest_flight_pln_distance_details.STAR as star,latest_flight_pln_distance_details.STAR_entrypoint as star_entrypoint,latest_flight_pln_distance_details.no_altn_stations as no_altn_stations from ip_tbl")

    //##############LATEST_FLIGHT_PLN_ALTERNATE_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM LATEST_FLIGHT_PLN_ALTERNATE_DETAILS...")
    val pln_alternate_dtl = hiveContext.sql("select flight_identifier,latest_flight_pln_alternate_details.altn_1_icao_code as altn_1_icao_code,latest_flight_pln_alternate_details.altn_1_iata_code as altn_1_iata_code,latest_flight_pln_alternate_details.altn_1_dist as altn_1_dist,latest_flight_pln_alternate_details.altn_1_time as altn_1_time,latest_flight_pln_alternate_details.altn_1_fuel as altn_1_fuel,latest_flight_pln_alternate_details.altn_2_icao_code as altn_2_icao_code,latest_flight_pln_alternate_details.altn_2_iata_code as altn_2_iata_code,latest_flight_pln_alternate_details.altn_2_dist as altn_2_dist,latest_flight_pln_alternate_details.altn_2_time as altn_2_time,latest_flight_pln_alternate_details.altn_2_fuel as altn_2_fuel,latest_flight_pln_alternate_details.altn_3_icao_code as altn_3_icao_code,latest_flight_pln_alternate_details.altn_3_iata_code as altn_3_iata_code,latest_flight_pln_alternate_details.altn_3_dist as altn_3_dist,latest_flight_pln_alternate_details.altn_3_time as altn_3_time,latest_flight_pln_alternate_details.altn_3_fuel as altn_3_fuel,latest_flight_pln_alternate_details.altn_4_icao_code as altn_4_icao_code,latest_flight_pln_alternate_details.altn_4_iata_code as altn_4_iata_code,latest_flight_pln_alternate_details.altn_4_dist as altn_4_dist,latest_flight_pln_alternate_details.altn_4_time as altn_4_time,latest_flight_pln_alternate_details.altn_4_fuel as altn_4_fuel,latest_flight_pln_alternate_details.lido_version_num as lido_version_num,latest_flight_pln_alternate_details.msg_received_datetime as lido_msg_received_date from ip_tbl")

    //##############LATEST_FLIGHT_ARR_WEATHER_MESSAGE###################
    log.info("[INFO] SELECTING DATA FROM LATEST_FLIGHT_ARR_WEATHER_MESSAGE...")
    val arr_weather_message = hiveContext.sql("select flight_identifier,latest_flight_arr_weather_message.arr_metar_message as arr_metar_message,latest_flight_arr_weather_message.arr_metar_message_time as arr_metar_message_time,latest_flight_arr_weather_message.arr_metar_station_iata_code as arr_metar_station_iata_code,latest_flight_arr_weather_message.arr_metar_station_icao_code as arr_metar_station_icao_code,latest_flight_arr_weather_message.arr_metar_received_time as arr_metar_received_time from ip_tbl")

    //##############LATEST_FLIGHT_DEP_WEATHER_MESSAGE###################
    log.info("[INFO] SELECTING DATA FROM LATEST_FLIGHT_DEP_WEATHER_MESSAGE...")
    val dep_weather_message = hiveContext.sql("select flight_identifier,latest_flight_dep_weather_message.dep_metar_message as dep_metar_message,latest_flight_dep_weather_message.dep_metar_message_time as dep_metar_message_time,latest_flight_dep_weather_message.dep_metar_station_iata_code as dep_metar_station_iata_code,latest_flight_dep_weather_message.dep_metar_station_icao_code as dep_metar_station_icao_code,latest_flight_dep_weather_message.dep_metar_received_time as dep_metar_received_time from ip_tbl")

    //JOINING ALL THE DATA FRAMES
    val flight_plan_dtls = pln_time_dtl.join(pln_fuel_dtl,Seq("flight_identifier") ,"inner").join(pln_distance_dtl,Seq("flight_identifier") ,"inner").join(pln_alternate_dtl,Seq("flight_identifier") ,"inner").join(arr_weather_message,Seq("flight_identifier") ,"inner").join(dep_weather_message,Seq("flight_identifier") ,"inner").dropDuplicates

    ////FINDING DELTA
    val tbl_flt_pln_dtl = table_diff(flight_plan_dtls,output_path,"OPS_STG_EDH_FLT_PLAN_DTLS",table_names,prev_snap_dates)

    //DELETING CURRENT SNAP SHOT
    var uri : URI = new URI(output_path+"/OPS_STG_EDH_FLT_PLAN_DTLS/hx_snapshot_date="+currentDate)
    var hdfs : FileSystem = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_flt_pln_dtl.select("flight_identifier","pln_block_time","est_flight_time","pln_trip_time","pln_contingency_time","pln_altn_time","pln_frsv_time","pln_additional_time","pln_taxi_out_time","pln_taxi_in_time","pln_ramp_time","est_time_mins","extra_1_time","extra_2_time","extra_3_time","extra_4_time","touch_down_fuel","trip_fuel","altn_fuel","frsv_fuel","additional_fuel","taxi_out_fuel","taxi_in_fuel","block_fuel","t_o_fuel","ramp_fuel","addl_1_fuel","rsn_addl_1_fuel","addl_2_fuel","rsn_addl_2_fuel","addl_3_fuel","rsn_addl_3_fuel","addl_4_fuel","rsn_addl_4_fuel","contingency_fuel","econ_fuel","fuel_over_destn","tank_capacity","saving_usd","perf_correction","contingency_coverage","contingency_percent","enr_altn_fuel","enr_altn_prec","tripfuel_fl_blw","zfw_1000_minus","zfw_1000_plus","air_dist_nm","alt_toc","alt_tod","great_circle_dist_nm","grnd_dist","sid_dist_nm","star_dist_nm","route_optimiz","cruize_deg","cruize","cruize_temp","toctrop","avtrk","avwc","avisa","cid","gain_loss","sid","sid_exitpoint","star","star_entrypoint","no_altn_stations","altn_1_icao_code","altn_1_iata_code","altn_1_dist","altn_1_time","altn_1_fuel","altn_2_icao_code","altn_2_iata_code","altn_2_dist","altn_2_time","altn_2_fuel","altn_3_icao_code","altn_3_iata_code","altn_3_dist","altn_3_time","altn_3_fuel","altn_4_icao_code","altn_4_iata_code","altn_4_dist","altn_4_time","altn_4_fuel","lido_version_num","lido_msg_received_date","arr_metar_message","arr_metar_message_time","arr_metar_station_iata_code","arr_metar_station_icao_code","arr_metar_received_time","dep_metar_message","dep_metar_message_time","dep_metar_station_iata_code","dep_metar_station_icao_code","dep_metar_received_time","hx_snapshot_date").coalesce(partitions).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_PLAN_DTLS")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLAN_DTLS FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri1 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_PLAN_DTLS/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri1),true)}
    //############################################################

  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path : String = null, compression: String = null, spark_partitions : Int = 0, no_of_shuffles : String = null, ctrl_file_path : String = null)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("FlattenPlannedFlightDetails") {
      head("FlattenPlannedFlightDetails")
      opt[String]('i', "hdfs_input_path")
        .required()
        .action((x, config) => config.copy(hdfs_input_path = x))
        .text("Required parameter : Input file path for data")
      opt[String]('o', "hdfs_output_path")
        .required()
        .action((x, config) => config.copy(hdfs_output_path = x))
        .text("Required parameter : Output file path for data")
      opt[String]('s', "no_of_shuffles")
        .required()
        .action((x, config) => config.copy(no_of_shuffles = x))
        .text("Required parameter : no of shuffles")
      opt[String]('x', "ctrl_file_path")
        .required()
        .action((x, config) => config.copy(ctrl_file_path = x))
        .text("Required parameter : Control file path")
      opt[Int]('p', "spark_partitions")
        .required()
        .action((x, config) => config.copy(spark_partitions = x))
        .text("Required parameter : Output file path for data")
      opt[String]('c', "compression")
        .required()
        .valueName("snappy OR deflate")
        .validate(x => {
          if (! (x.matches(compress_rex.toString()))) Left("Invalid compression specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(compression = x))
        .text("Required parameter : Output data compression format, snappy or deflate")
    }

    parser.parse(args, Config()) map { config =>
      //CALLING PLAN FLIGHT DETAILS
      log.info("[INFO] PERFORMING FLATTENING PLANNED FLIGHT DETAILS ...")
      flatten_plan_flight_details(config.hdfs_input_path,config.hdfs_output_path,config.no_of_shuffles,config.spark_partitions,config.compression,config.ctrl_file_path)
      log.info("[INFO] PERFORMING FLATTENING PLANNED FLIGHT DETAILS IS COMPLETED SUCCESSFULLY")
    }
  }
}
