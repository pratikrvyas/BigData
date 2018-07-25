/*
*=====================================================================================================================================
* Created on     :   17/04/2018
* Author         :   Thangamani Kumaravelu
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   FlattenActualFlightDetails
* Description    :   This code flattens actual flight details data and stores in multiple tables
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue opseff --class "com.emirates.helix.FlattenActualFlightDetails" /home/ops_eff/ek_flightdetails/flattenActualFlightDetails-1.0-SNAPSHOT.jar -i /data/helix/modelled/emfg/public/flightactualdetails/current -o /data/helix/modelled/emfg/serving -c snappy -p 2 -s 20

package com.helix.emirates

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlattenActualFlightDetails {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("Flatten_Actual_Flight_Details")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Flatten_Actual_Flight_Details")

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

  def flatten_actual_flight_details(input_path : String,output_path : String, shuffle : String, partitions :  Int, compression : String,ctrl_file_path : String)={

    //READING CONTROL FILE
    ///apps/helix/conf/oozie/workspaces/wf_flattenactualflightdetailsemfg/ctrl_file.prop
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

    //##############FLIGHT_ACT_TIME_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM FLIGHT_ACT_TIME_DETAILS...")
    val act_time_dtl = hiveContext.sql("select flight_identifier,flight_act_time_details.block_time_mins as block_time_mins,cast(flight_act_time_details.trip_time_mins as String) as trip_time_mins,flight_act_time_details.taxi_out_mins as taxi_out_mins,flight_act_time_details.taxi_in_mins as taxi_in_mins from ip_tbl")

    //##############FLIGHT_ACT_FUEL_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM FLIGHT_ACT_FUEL_DETAILS...")
    val act_fuel_dtl = hiveContext.sql("select flight_identifier,flight_act_fuel_details.fuel_out_kgs as fuel_out_kgs,flight_act_fuel_details.fuel_off_kgs as fuel_off_kgs,flight_act_fuel_details.fuel_on_kgs as fuel_on_kgs,flight_act_fuel_details.fuel_in_kgs as fuel_in_kgs,flight_act_fuel_details.block_fuel_kgs as block_fuel_kgs,flight_act_fuel_details.trip_fuel_kgs as trip_fuel_kgs,flight_act_fuel_details.fuel_populated_datetime as fuel_populated_datetime from ip_tbl")

    //##############FLIGHT_OOOI_FUEL_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM FLIGHT_OOOI_FUEL_DETAILS...")
    val oooi_fuel_dtl = hiveContext.sql("select flight_identifier,flight_oooi_fuel_details.AZFW_kgs as oooi_azfw_kgs,flight_oooi_fuel_details.fuel_out_kgs as oooi_fuel_out_kgs,flight_oooi_fuel_details.fuel_off_kgs as oooi_fuel_off_kgs,flight_oooi_fuel_details.fuel_on_kgs as oooi_fuel_on_kgs,flight_oooi_fuel_details.fuel_in_kgs as oooi_fuel_in_kgs,flight_oooi_fuel_details.egds_populated_datetime as egds_populated_datetime from ip_tbl")

    //##############FLIGHT_ACT_ADDNL_FUEL_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM FLIGHT_ACT_ADDNL_FUEL_DETAILS...")
    val act_addnl_fuel_dtl = hiveContext.sql("select flight_identifier,flight_act_addnl_fuel_details.contingency_fuel_used_kgs as contingency_fuel_used_kgs,flight_act_addnl_fuel_details.captain_Extra_fuel_kgs as captain_Extra_fuel_kgs,flight_act_addnl_fuel_details.pct_contingency_used_kgs as pct_contingency_used_kgs,flight_act_addnl_fuel_details.zfw_correction_kgs as zfw_correction_kgs,flight_act_addnl_fuel_details.wabi_takeoff_fuel_kgs as wabi_takeoff_fuel_kgs,flight_act_addnl_fuel_details.adj_extra_fuel as adj_extra_fuel,flight_act_addnl_fuel_details.excess_arrival_fuel as excess_arrival_fuel,flight_act_addnl_fuel_details.adj_extra_arr_fuel as adj_extra_arr_fuel,flight_act_addnl_fuel_details.extra_fuel_penalty as extra_fuel_penalty,flight_act_addnl_fuel_details.fuel_burn_saved as fuel_burn_saved,flight_act_addnl_fuel_details.fm_recorded_datetime as fm_recorded_datetime from ip_tbl")

    //##############LATEST_FLIGHT_ACT_MFP_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM LATEST_FLIGHT_ACT_MFP_DETAILS...")
    val act_mfp_dtl = hiveContext.sql("select flight_identifier,latest_flight_act_mfp_details.mfp_file_name as mfp_file_name,latest_flight_act_mfp_details.mfp_file_path as mfp_file_path,latest_flight_act_mfp_details.mfp_page_count as mfp_page_count,latest_flight_act_mfp_details.mfp_source_folder as mfp_source_folder,latest_flight_act_mfp_details.mfp_folder_index as mfp_folder_index,latest_flight_act_mfp_details.mfp_valid_ind as mfp_valid_ind,latest_flight_act_mfp_details.mfp_fuel_reason_code as mfp_fuel_reason_code,latest_flight_act_mfp_details.mfp_fuel_reason_comment as mfp_fuel_reason_comment,latest_flight_act_mfp_details.captain_staff_id as captain_staff_id,latest_flight_act_mfp_details.captain_staff_name as captain_staff_name,latest_flight_act_mfp_details.mfp_published_datetime as mfp_published_datetime,latest_flight_act_mfp_details.mfp_source_sequence_id as mfp_source_sequence_id from ip_tbl")

    //##############FLIGHT_ACT_DISTANCE_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM FLIGHT_ACT_DISTANCE_DETAILS...")
    val act_distance_dtl = hiveContext.sql("select flight_identifier,flight_act_distance_details.grnd_distance as grnd_distance,flight_act_distance_details.ESAD as esad,cast(flight_act_distance_details.Windspeed as String) as windspeed,flight_act_distance_details.FileName as file_name from ip_tbl")

    //##############FLIGHT_ACT_HOLDING_TIME###################
    log.info("[INFO] SELECTING DATA FROM FLIGHT_ACT_HOLDING_TIME...")
    val act_holding_time = hiveContext.sql("select flight_identifier,flight_act_holding_time.hold_entry_time as hold_entry_time,flight_act_holding_time.hold_exit_time as hold_exit_time,flight_act_holding_time.hold_area as hold_area,flight_act_holding_time.event_type as event_type,flight_act_holding_time.nearest_STAR as nearest_star,flight_act_holding_time.hold_time_mins as hold_time_mins,flight_act_holding_time.epic_recorded_datetime as epic_recorded_datetime from ip_tbl")

    //##############FLIGHT_ACT_UPLIFT_DETAILS###################
    log.info("[INFO] SELECTING DATA FROM FLIGHT_ACT_UPLIFT_DETAILS...")
    val act_uplift_dtl = hiveContext.sql("select flight_identifier,cast(flight_act_uplift_details.act_fuel_uplifted_gal as String) as act_fuel_uplifted_gal,flight_act_uplift_details.act_fuel_uplifted_litres as act_fuel_uplifted_litres,flight_act_uplift_details.fuel_specific_gravity as fuel_specific_gravity,flight_act_uplift_details.act_potable_water_aft_litres as act_potable_water_aft_litres,flight_act_uplift_details.act_potable_water_pdaft_litres as act_potable_water_pdaft_litres,flight_act_uplift_details.um_recorded_datetime as um_recorded_datetime from ip_tbl")

    //JOINING ALL THE DATA FRAMES
    val flight_actual_dtls = act_time_dtl.join(act_fuel_dtl,Seq("flight_identifier") ,"inner").join(oooi_fuel_dtl,Seq("flight_identifier") ,"inner").join(act_addnl_fuel_dtl,Seq("flight_identifier") ,"inner").join(act_mfp_dtl,Seq("flight_identifier") ,"inner").join(act_distance_dtl,Seq("flight_identifier") ,"inner").join(act_holding_time,Seq("flight_identifier") ,"inner").join(act_uplift_dtl,Seq("flight_identifier") ,"inner").dropDuplicates

    ////FINDING DELTA
    val tbl_flt_act_dtl = table_diff(flight_actual_dtls,output_path,"OPS_STG_EDH_FLT_ACT_DTLS",table_names,prev_snap_dates)

    //DELETING CURRENT SNAP SHOT
    var uri : URI = new URI(output_path+"/OPS_STG_EDH_FLT_ACT_DTLS/hx_snapshot_date="+currentDate)
    var hdfs : FileSystem = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_flt_act_dtl.select("flight_identifier","block_time_mins","trip_time_mins","taxi_out_mins","taxi_in_mins","fuel_out_kgs","fuel_off_kgs","fuel_on_kgs","fuel_in_kgs","block_fuel_kgs","trip_fuel_kgs","fuel_populated_datetime","oooi_azfw_kgs","oooi_fuel_out_kgs","oooi_fuel_off_kgs","oooi_fuel_on_kgs","oooi_fuel_in_kgs","egds_populated_datetime","contingency_fuel_used_kgs","captain_extra_fuel_kgs","pct_contingency_used_kgs","zfw_correction_kgs","wabi_takeoff_fuel_kgs","adj_extra_fuel","excess_arrival_fuel","adj_extra_arr_fuel","extra_fuel_penalty","fuel_burn_saved","fm_recorded_datetime","mfp_file_name","mfp_file_path","mfp_page_count","mfp_source_folder","mfp_folder_index","mfp_valid_ind","mfp_fuel_reason_code","mfp_fuel_reason_comment","captain_staff_id","captain_staff_name","mfp_published_datetime","mfp_source_sequence_id","grnd_distance","esad","windspeed","file_name","hold_entry_time","hold_exit_time","hold_area","event_type","nearest_star","hold_time_mins","epic_recorded_datetime","act_fuel_uplifted_gal","act_fuel_uplifted_litres","fuel_specific_gravity","act_potable_water_aft_litres","act_potable_water_pdaft_litres","um_recorded_datetime","hx_snapshot_date").coalesce(partitions).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_ACT_DTLS")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_ACT_DTLS FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri1 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_ACT_DTLS/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri1),true)}
    //############################################################

  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path : String = null, compression: String = null, spark_partitions : Int = 0, no_of_shuffles : String = null, ctrl_file_path : String = null)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("FlattenActualFlightDetails") {
      head("FlattenActualFlightDetails")
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
          if (!(x.matches(compress_rex.toString()))) Left("Invalid compression specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(compression = x))
        .text("Required parameter : Output data compression format, snappy or deflate")
    }

    parser.parse(args, Config()) map { config =>
      //CALLING FLIGHT_MASTER_LIDO
      log.info("[INFO] PERFORMING FLATTENING ACTUAL FLIGHT DETAILS ...")
      flatten_actual_flight_details(config.hdfs_input_path,config.hdfs_output_path,config.no_of_shuffles,config.spark_partitions,config.compression,config.ctrl_file_path)
      log.info("[INFO] PERFORMING FLATTENING ACTUAL FLIGHT DETAILS IS COMPLETED SUCCESSFULLY")
    }
  }
}

