/*
*=====================================================================================================================================
* Created on     :   18/02/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   FlattenFlightHubMaster
* Description    :   This code flattens flightMaster data and stores in multiple tables
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue opseff --class "com.emirates.helix.FlattenFlightMaster" /home/ops_eff/ek_flightMaster/exportFlightHubMaster-1.0-SNAPSHOT.jar -i /data/helix/modelled/emfg/public/flightmaster/current -o /data/helix/modelled/emfg/serving -c snappy -p 2 -s 20 -d 2018-05-14

package com.emirates.helix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlattenFlightMaster {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("Genome_Lido_Fsum_Flight_Master")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Genome_Lido_Fsum_Flight_Master")

  //DELTA LOGIC
  def table_diff(current_df : DataFrame,path : String, table_name : String,table_names : List[String],prev_snap_dates : List[String],snap_date : String) : DataFrame = {

    //PREV SNAP SHOT
    var dt = ""
    table_names.foreach{x =>
      if(x == table_name){
        dt= prev_snap_dates(table_names.indexOf(x))
      }
    }

    if(dt.isEmpty || dt == "" || dt == null ){
      log.info("[INFO] NO PREVIOUS FLATTEN SNAPSHOT FOUND HENCE CONSIDERING CURRENT SNAPSHOT ONLY "+current_df.count)
      return current_df.withColumn("hx_snapshot_date",lit(snap_date))
    }
    else {
      log.info("[INFO] READING PREVIOUS FLATTEN SNAP DATA "+path+"/"+table_name+"/hx_snapshot_date <="+dt+"...")
      val read_df = sqlContext.read.parquet(path + "/" + table_name).registerTempTable("read_tbl")
      val prev_df = sqlContext.sql("select * from read_tbl where hx_snapshot_date <= '"+ dt +"'").drop($"hx_snapshot_date")
      val diff_df = current_df.except(prev_df)
      log.info("[INFO]  DELTA DIFF COUNT FORM PREVIOUS FLATTEN SNAPSHOT TO CURRENT SNAPSHOT IS "+diff_df.count)
      return diff_df.withColumn("hx_snapshot_date",lit(snap_date))
    }

  }


  def flatten_flight_master(input_path : String,output_path : String, shuffle : String, partitions :  Int, compression : String,ctrl_file_path : String,snapdate : String)={

    //READING CONTROL FILE
    ///apps/helix/conf/oozie/workspaces/wf_ServingFlightMasterExaData/ctrl_file.prop
    val ctrl_file_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true") .option("inferSchema", "true") .load(ctrl_file_path)
    val table_names = (ctrl_file_df.select("table_name").map(r => r.getString(0).toString).collect.toList)
    val prev_snap_dates = (ctrl_file_df.select("prev_snap_date").map(r => r.getString(0)).collect.toList)

    //CURRENT SNAP SHOT
    log.info("[INFO] GETTING CURRENT DATE FOR THE TODAYS LOADING ...")
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    var currentDate = ""
    if(snapdate.isEmpty){
      currentDate = formatter.format(cal.getTime())
    }
    else{
      currentDate = snapdate
    }

    log.info("[INFO] GETTING CURRENT DATE FOR THE TODAYS LOADING IS : "+currentDate)

    //SHUFFLE
    hiveContext.setConf("spark.sql.shuffle.partitions", shuffle)
    hiveContext.setConf("spark.sql.parquet.compression.codec", compression)

    //READING DATA
    log.info("[INFO] READING INPUT DATA FROM THE PATH" + input_path + "...")
    val ip_data = hiveContext.read.parquet(input_path)
    log.info("[INFO] READING INPUT DATA FROM THE PATH" + input_path + "is successfull")
    val ip_data1 = ip_data.repartition(partitions).where($"AIRLINE_CODE" === "EK")
    ip_data1.registerTempTable("ip_tbl")
    hiveContext.cacheTable("ip_tbl")

    ip_data1.count

    //EXPLODING REQUIRED COLUMNS

    //##############OPS_STG_EDH_FLIGHT_PLN_REG###################
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_PLN_REG ...")
    val explode_df1 = hiveContext.sql("select flight_identifier,pln_reg_details_expld['pln_reg_number'] as pln_reg_number,pln_reg_details_expld['msg_recorded_datetime'] as pln_posted_datetime from (select flight_identifier,explode(pln_reg_details) as pln_reg_details_expld  from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df1 = table_diff(explode_df1,output_path,"OPS_STG_EDH_FLIGHT_PLN_REG",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    var uri : URI = new URI(output_path+"/OPS_STG_EDH_FLIGHT_PLN_REG/hx_snapshot_date="+currentDate)
    var hdfs : FileSystem = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df1.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLIGHT_PLN_REG")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_PLN_REG FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri1 : URI = new URI(output_path+"/OPS_STG_EDH_FLIGHT_PLN_REG/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri1),true)}
    //############################################################

    //#############OPS_STG_EDH_FLIGHT_DELAY_TRANS#################
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_DELAY_TRANS ...")
    val explode_df2 = hiveContext.sql("select flight_identifier,flight_delay_details_expld['delay_code'] as delay_code,flight_delay_details_expld['delay_reason'] as delay_reason,flight_delay_details_expld['delay_dept_code'] as delay_dept_code,flight_delay_details_expld['delay_duration_mins'] as delay_duration_mins,flight_delay_details_expld['delay_iata_station'] as delay_iata_station,flight_delay_details_expld['delay_icao_station'] as delay_icao_station,flight_delay_details_expld['delay_type'] as delay_type,flight_delay_details_expld['delay_posted_datetime'] as delay_posted_datetime from (select flight_identifier,explode(flight_delay_details) as flight_delay_details_expld from ip_tbl) src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df2 = table_diff(explode_df2,output_path,"OPS_STG_EDH_FLIGHT_DELAY_TRANS",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLIGHT_DELAY_TRANS/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df2.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLIGHT_DELAY_TRANS")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_DELAY_TRANS FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri2 : URI = new URI(output_path+"/OPS_STG_EDH_FLIGHT_DELAY_TRANS/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri2),true)}
    //###########################################################

    //###############OPS_STG_EDH_FLT_SI_REMARKS_DEP############## -MISSING TABLE -- NOT REQUIRED
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_DEP ...")
    val explode_df3 = hiveContext.sql("select flight_identifier,flight_si_dep_remarks_expld['flight_si_remarks_dep'] as flight_si_remarks_dep,flight_si_dep_remarks_expld['msg_recorded_datetime'] as remarks_posted_datetime from (select flight_identifier,explode(flight_si_dep_remarks) as flight_si_dep_remarks_expld from ip_tbl)src ").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df3 = table_diff(explode_df3,output_path,"OPS_STG_EDH_FLT_SI_REMARKS_DEP",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_DEP/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df3.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_DEP")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_DEP FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri3 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_DEP/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri3),true)}
    //###########################################################

    //###############OPS_STG_EDH_FLT_SI_REMARKS_ARR############## -MISSING TABLE -- NOT REQUIRED
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_ARR ...")
    val explode_df4 = hiveContext.sql("select flight_identifier,flight_si_arr_remarks_expld['flight_si_remarks_arr'] as flight_si_remarks_arr,flight_si_arr_remarks_expld['msg_recorded_datetime'] as remarks_posted_datetime from (select flight_identifier,explode(flight_si_arr_remarks) as flight_si_arr_remarks_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df4 = table_diff(explode_df4,output_path,"OPS_STG_EDH_FLT_SI_REMARKS_ARR",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_ARR/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df4.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_ARR")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_ARR FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri4 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_SI_REMARKS_ARR/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri4),true)}
    //##########################################################

    //###############OPS_STG_EDH_FLIGHT_STAT_TRANS##############
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_STAT_TRANS ...")
    val explode_df5 = hiveContext.sql("select flight_identifier,flight_status_details_expld['flight_status'] as flight_status,flight_status_details_expld['flight_status_desc'] as flight_status_desc,flight_status_details_expld['msg_recorded_datetime'] as flt_status_recorded_datetime from (select flight_identifier,explode(flight_status_details) as flight_status_details_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df5 = table_diff(explode_df5,output_path,"OPS_STG_EDH_FLIGHT_STAT_TRANS",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLIGHT_STAT_TRANS/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df5.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLIGHT_STAT_TRANS")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_STAT_TRANS FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri5 : URI = new URI(output_path+"/OPS_STG_EDH_FLIGHT_STAT_TRANS/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri5),true)}
    //##########################################################

    //##############OPS_STG_EDH_FLIGHT_REMARKS############## -MISSING TABLE --NOT REQUIRED
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_REMARKS ...")
    val explode_df6 = hiveContext.sql("select flight_identifier,flight_remarks_expld['flight_remarks'] as flight_remarks,flight_remarks_expld['msg_recorded_datetime'] as remarks_posted_datetime from (select flight_identifier,explode(flight_remarks) as flight_remarks_expld from ip_tbl)src")

    ////FINDING DELTA
    val tbl_diff_df6 = table_diff(explode_df6,output_path,"OPS_STG_EDH_FLIGHT_REMARKS",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLIGHT_REMARKS/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df6.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLIGHT_REMARKS")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_REMARKS FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri6 : URI = new URI(output_path+"/OPS_STG_EDH_FLIGHT_REMARKS/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri6),true)}
    //#######################################################

    //#############OPS_STG_EDH_FLT_DOOR_REMARKS############# -MISSING TABLE --NOT REQUIRED
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_DOOR_REMARKS ...")
    val explode_df7 = hiveContext.sql("select flight_identifier,flight_door_remarks_expld['door_remark'] as door_remark,flight_door_remarks_expld['msg_recorded_datetime'] as remarks_posted_datetime from (select flight_identifier,explode(flight_door_remarks) as flight_door_remarks_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df7 = table_diff(explode_df7,output_path,"OPS_STG_EDH_FLT_DOOR_REMARKS",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_DOOR_REMARKS/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df7.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_DOOR_REMARKS")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_DOOR_REMARKS FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri7 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_DOOR_REMARKS/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri7),true)}
    //#####################################################

    //########OPS_STG_EDH_FLT_OUT_TIME_EST####### -MISSING TABLE
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_OUT_TIME_EST ...")
    val explode_df8 = hiveContext.sql("select flight_identifier,est_flight_out_datetime_expld['est_flight_out_datetime_local'] as est_flight_out_datetime_local,est_flight_out_datetime_expld['est_flight_out_datetime_utc'] as est_flight_out_datetime_utc,est_flight_out_datetime_expld['msg_recorded_datetime'] as est_out_posted_datetime from (select flight_identifier,explode(est_flight_out_datetime) as est_flight_out_datetime_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df8 = table_diff(explode_df8,output_path,"OPS_STG_EDH_FLT_OUT_TIME_EST",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_OUT_TIME_EST/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df8.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_OUT_TIME_EST")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_OUT_TIME_EST FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri8 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_OUT_TIME_EST/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri8),true)}
    //####################################################

    //########OPS_STG_EDH_FLT_IN_TIME_EST####### -MISSING TABLE
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_IN_TIME_EST ...")
    val explode_df9 = hiveContext.sql("select flight_identifier,est_flight_in_datetime_expld['est_flight_in_datetime_local'] as est_flight_in_datetime_local,est_flight_in_datetime_expld['est_flight_in_datetime_utc'] as est_flight_in_datetime_utc,est_flight_in_datetime_expld['msg_recorded_datetime'] as est_in_posted_datetime from (select flight_identifier,explode(est_flight_in_datetime) as est_flight_in_datetime_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df9 = table_diff(explode_df9,output_path,"OPS_STG_EDH_FLT_IN_TIME_EST",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_IN_TIME_EST/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df9.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_IN_TIME_EST")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_IN_TIME_EST FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri9 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_IN_TIME_EST/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri9),true)}
    //####################################################

    //########OPS_STG_EDH_FLT_OFF_TIME_EST####### -MISSING TABLE
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_OFF_TIME_EST ...")
    val explode_df13 = hiveContext.sql("select flight_identifier,est_flight_off_datetime_expld['est_flight_off_datetime_local'] as est_flight_off_datetime_local,est_flight_off_datetime_expld['est_flight_off_datetime_utc'] as est_flight_off_datetime_utc,est_flight_off_datetime_expld['msg_recorded_datetime'] as est_off_posted_datetime from (select flight_identifier,explode(est_flight_off_datetime) as est_flight_off_datetime_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df13 = table_diff(explode_df13,output_path,"OPS_STG_EDH_FLT_OFF_TIME_EST",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_OFF_TIME_EST/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df13.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_OFF_TIME_EST")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_OFF_TIME_EST FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri10 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_OFF_TIME_EST/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri10),true)}
    //####################################################

    //########OPS_STG_EDH_FLT_ON_TIME_EST ####### -MISSING TABLE
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_ON_TIME_EST ...")
    val explode_df10 = hiveContext.sql("select flight_identifier,est_flight_on_datetime_expld['est_flight_on_datetime_local'] as est_flight_on_datetime_local,est_flight_on_datetime_expld['est_flight_on_datetime_utc'] as est_flight_on_datetime_utc,est_flight_on_datetime_expld['msg_recorded_datetime'] as est_on_posted_datetime from (select flight_identifier,explode(est_flight_on_datetime) as est_flight_on_datetime_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df10 = table_diff(explode_df10,output_path,"OPS_STG_EDH_FLT_ON_TIME_EST",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_ON_TIME_EST/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df10.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_ON_TIME_EST")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_ON_TIME_EST FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri11 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_ON_TIME_EST/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri11),true)}
    //####################################################

    //##########OPS_STG_EDH_FLT_PLN_RUNWAY###############
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_RUNWAY ...")
    val explode_df11 = hiveContext.sql("select flight_identifier,pln_runway_details_expld.pln_dep_runway,pln_runway_details_expld.pln_arr_runway,pln_runway_details_expld.version as lido_version_num,pln_runway_details_expld.datetime as runway_msg_created_datetime from (select flight_identifier,explode(pln_runway_details) as pln_runway_details_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df11 = table_diff(explode_df11,output_path,"OPS_STG_EDH_FLT_PLN_RUNWAY",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_RUNWAY/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df11.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_PLN_RUNWAY")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_RUNWAY FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri12 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_RUNWAY/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri12),true)}
    //####################################################

    //##########OPS_STG_EDH_FLT_PLN_ALT_APT###############
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_ALT_APT ...")
    val explode_df12 = hiveContext.sql("select flight_identifier,flight_pln_altn_airport_details_expld.pln_alt_apt1_iata,flight_pln_altn_airport_details_expld.pln_alt_apt1_icao,flight_pln_altn_airport_details_expld.pln_alt_apt2_iata,flight_pln_altn_airport_details_expld.pln_alt_apt2_icao,flight_pln_altn_airport_details_expld.pln_alt_apt3_iata,flight_pln_altn_airport_details_expld.pln_alt_apt3_icao,flight_pln_altn_airport_details_expld.pln_alt_apt4_iata,flight_pln_altn_airport_details_expld.pln_alt_apt4_icao,flight_pln_altn_airport_details_expld.version as lido_version_num,flight_pln_altn_airport_details_expld.datetime as alt_msg_received_datetime from (select flight_identifier,explode(flight_pln_altn_airport_details) as flight_pln_altn_airport_details_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df12 = table_diff(explode_df12,output_path,"OPS_STG_EDH_FLT_PLN_ALT_APT",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_ALT_APT/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df12.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_PLN_ALT_APT")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_ALT_APT FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri13 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_ALT_APT/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri13),true)}
    //####################################################

    //##########OPS_STG_EDH_FLT_COST_INDEX###############
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_COST_INDEX ...")
    val explode_df14 = hiveContext.sql("select flight_identifier,flight_cost_index_details_expld.cost_index as cost_index,flight_cost_index_details_expld.version as lido_version_num,flight_cost_index_details_expld.datetime as ci_posted_datetime from (select flight_identifier,explode(flight_cost_index_details) as flight_cost_index_details_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df14 = table_diff(explode_df14,output_path,"OPS_STG_EDH_FLT_COST_INDEX",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_COST_INDEX/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df14.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_COST_INDEX")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_COST_INDEX FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri14 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_COST_INDEX/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri14),true)}
    //####################################################

    //##########OPS_STG_EDH_FLT_PLN_ARR_BAY###############
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_ARR_BAY ...")
    val explode_df15 = hiveContext.sql("select flight_identifier,pln_arr_bay_details_expld.pln_arr_old_bay_number as pln_arr_old_bay_number,pln_arr_bay_details_expld.pln_arr_new_bay_number as pln_arr_new_bay_number,pln_arr_bay_details_expld.pln_arr_bay_change_reason_code as pln_arr_bay_change_reason_code,pln_arr_bay_details_expld.pln_arr_bay_change_reason_desc as pln_arr_bay_change_reason_desc,pln_arr_bay_details_expld.arr_bay_recorded_datetime as arr_bay_recorded_datetime from (select flight_identifier,explode(pln_arr_bay_details) as pln_arr_bay_details_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df15 = table_diff(explode_df15,output_path,"OPS_STG_EDH_FLT_PLN_ARR_BAY",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_ARR_BAY/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df15.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_PLN_ARR_BAY")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_ARR_BAY FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri15 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_ARR_BAY/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri15),true)}
    //####################################################

    //##########OPS_STG_EDH_FLT_PLN_DEP_BAY###############
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_DEP_BAY ...")
    val explode_df16 = hiveContext.sql("select flight_identifier,pln_dep_bay_details_expld.pln_dep_old_bay_number as pln_dep_old_bay_number,pln_dep_bay_details_expld.pln_dep_new_bay_number as pln_dep_new_bay_number,pln_dep_bay_details_expld.pln_dep_bay_change_reason_code as pln_dep_bay_change_reason_code,pln_dep_bay_details_expld.pln_dep_bay_change_reason_desc as pln_dep_bay_change_reason_desc,pln_dep_bay_details_expld.dep_bay_recorded_datetime as dep_bay_recorded_datetime from (select flight_identifier,explode(pln_dep_bay_details) as pln_dep_bay_details_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df16 = table_diff(explode_df16,output_path,"OPS_STG_EDH_FLT_PLN_DEP_BAY",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_DEP_BAY/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df16.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_PLN_DEP_BAY")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_DEP_BAY FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri16 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_DEP_BAY/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri16),true)}
    //####################################################

    //##########OPS_STG_EDH_FLT_PLN_ARR_GATE###############
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_ARR_GATE ...")
    val explode_df17 = hiveContext.sql("select flight_identifier,pln_arr_gate_details_expld.pln_arr_gate_number,pln_arr_gate_details_expld.pln_arr_concourse_number,pln_arr_gate_details_expld.pln_arr_terminal_number,pln_arr_gate_details_expld.pln_arr_datetime_utc,pln_arr_gate_details_expld.pln_arr_datetime_local,pln_arr_gate_details_expld.arr_gate_recorded_datetime from (select flight_identifier,explode(pln_arr_gate_details) as pln_arr_gate_details_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df17 = table_diff(explode_df17,output_path,"OPS_STG_EDH_FLT_PLN_ARR_GATE",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_ARR_GATE/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df17.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_PLN_ARR_GATE")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_ARR_GATE FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri17 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_ARR_GATE/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri17),true)}
    //####################################################

    //##########OPS_STG_EDH_FLT_PLN_DEP_GATE###############
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_DEP_GATE ...")
    val explode_df18 = hiveContext.sql("select flight_identifier,pln_dep_gate_details_expld.pln_dep_gate_number,pln_dep_gate_details_expld.pln_dep_concourse_number,pln_dep_gate_details_expld.pln_dep_terminal_number,pln_dep_gate_details_expld.pln_dep_datetime_utc,pln_dep_gate_details_expld.pln_dep_datetime_local,pln_dep_gate_details_expld.dep_gate_recorded_datetime from (select flight_identifier,explode(pln_dep_gate_details) as pln_dep_gate_details_expld from ip_tbl)src").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df18 = table_diff(explode_df18,output_path,"OPS_STG_EDH_FLT_PLN_DEP_GATE",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_DEP_GATE/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df18.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLT_PLN_DEP_GATE")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLT_PLN_DEP_GATE FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri18 : URI = new URI(output_path+"/OPS_STG_EDH_FLT_PLN_DEP_GATE/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri18),true)}
    //####################################################


    //############OPS_STG_EDH_FLIGHT_HUB_MASTER########### - need to join with simple cols
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_HUB_MASTER ...")
    val nested_df1 = hiveContext.sql("select flight_identifier,pln_runway_details_latest.pln_dep_runway,pln_runway_details_latest.pln_arr_runway,act_runway_details['act_dep_runway'] as act_dep_runway,act_runway_details['act_arr_runway'] as act_arr_runway,act_bay_details.act_dep_bay_number,act_bay_details.act_arr_bay_number,act_dep_gate_details.act_dep_gate_number,act_dep_gate_details.act_dep_concourse_number,act_dep_gate_details.act_dep_terminal_number,act_arr_gate_details.act_arr_gate_number,act_arr_gate_details.act_arr_concourse_number,act_arr_gate_details.act_arr_terminal_number,flight_cost_index_details_latest.cost_index,flight_pln_altn_airport_details_latest.pln_alt_apt1_iata,flight_pln_altn_airport_details_latest.pln_alt_apt1_icao,flight_pln_altn_airport_details_latest.pln_alt_apt2_iata,flight_pln_altn_airport_details_latest.pln_alt_apt2_icao,flight_pln_altn_airport_details_latest.pln_alt_apt3_iata,flight_pln_altn_airport_details_latest.pln_alt_apt3_icao,flight_pln_altn_airport_details_latest.pln_alt_apt4_iata,flight_pln_altn_airport_details_latest.pln_alt_apt4_icao from ip_tbl").dropDuplicates

    nested_df1.registerTempTable("nested_tbl1")

    //####################################################

    //########OPS_STG_EDH_FLIGHT_HUB_MASTER############### -JOIN WITH SIMPLE COLS
    val nested_df2 = hiveContext.sql("select flight_identifier,aircraft_details['aircraft_mfr'] as aircraft_mfr,aircraft_details['aircraft_type'] as aircraft_type,aircraft_details['aircraft_subtype'] as aircraft_subtype,aircraft_details['aircraft_config'] as aircraft_config,aircraft_details['aircraft_tot_capcity'] as aircraft_tot_capcity,aircraft_details['aircraft_F_capacity'] as aircraft_F_capacity,aircraft_details['aircraft_J_capacity'] as aircraft_J_capacity,aircraft_details['aircraft_Y_capacity'] as aircraft_Y_capacity,aircraft_details['aircraft_lease_ind'] as aircraft_lease_ind,aircraft_details['ETOPS_Ind'] as etops_ind,aircraft_details['aircraft_version_code'] as aircraft_version_code,aircraft_details['aircraft_owner'] as aircraft_owner,aircraft_details['potable_water_tank_count'] as potable_water_tank_count,aircraft_details['potable_water_tank_capacity_l'] as potable_water_tank_capacity_l,aircraft_details['no_engines'] as no_engines from ip_tbl").dropDuplicates

    nested_df2.registerTempTable("nested_tbl2")

    //###################################################


    //#######OPS_STG_EDH_FLIGHT_HUB_MASTER############### -JOIN WITH NESTED COLS
    val simple_cols = hiveContext.sql("select flight_identifier,flight_number,actual_reg_number,flight_date,pln_dep_iata_station,pln_dep_icao_station,pln_arr_iata_station,pln_arr_icao_station,latest_pln_arr_iata_station,act_dep_iata_station,act_dep_icao_station,act_arr_iata_station,act_arr_icao_station,sch_dep_datetime_utc,sch_dep_datetime_local,sch_arr_datetime_utc,sch_arr_datetime_local,act_dep_datetime_utc,act_dep_datetime_local,act_arr_datetime_utc,act_arr_datetime_local,flight_service_type,flight_service_desc,airline_code,airline_name,airline_country_name,flight_call_sign,arr_bag_carousel_number,flight_suffix,flight_leg_number,flight_dep_number,flight_via_route,flight_cabin_door_closure_date_local,flight_cargo_door_closure_date_local,flight_blocktime_orig,flight_blocktime_act,act_flight_out_datetime_utc,act_flight_out_datetime_local,act_flight_off_datetime_utc,act_flight_off_datetime_local,act_flight_in_datetime_utc,act_flight_in_datetime_local,act_flight_on_datetime_utc,act_flight_on_datetime_local,aircraft_subtype as flight_subtype from ip_tbl").dropDuplicates

    simple_cols.registerTempTable("simple_tbl")

    val simple_join1 = hiveContext.sql("select t1.*,t2.flight_number,t2.actual_reg_number,t2.flight_date,t2.pln_dep_iata_station,t2.pln_dep_icao_station,t2.pln_arr_iata_station,t2.pln_arr_icao_station,t2.latest_pln_arr_iata_station,t2.act_dep_iata_station,t2.act_dep_icao_station,t2.act_arr_iata_station,t2.act_arr_icao_station,t2.sch_dep_datetime_utc,t2.sch_dep_datetime_local,t2.sch_arr_datetime_utc,t2.sch_arr_datetime_local,t2.act_dep_datetime_utc,t2.act_dep_datetime_local,t2.act_arr_datetime_utc,t2.act_arr_datetime_local,t2.flight_service_type,t2.flight_service_desc,t2.airline_code,t2.airline_name,t2.airline_country_name,t2.flight_call_sign,t2.arr_bag_carousel_number,t2.flight_suffix,t2.flight_leg_number,t2.flight_dep_number,t2.flight_via_route,t2.flight_cabin_door_closure_date_local as flight_cabin_door_closure_date,t2.flight_cargo_door_closure_date_local as flight_cargo_door_closure_date,t2.flight_blocktime_orig,t2.flight_blocktime_act,t2.act_flight_out_datetime_utc,t2.act_flight_out_datetime_local,t2.act_flight_off_datetime_utc,t2.act_flight_off_datetime_local,t2.act_flight_in_datetime_utc,t2.act_flight_in_datetime_local,t2.act_flight_on_datetime_utc,t2.act_flight_on_datetime_local,t3.aircraft_mfr,t3.aircraft_type,t3.aircraft_subtype,t3.aircraft_config,t3.aircraft_tot_capcity,t3.aircraft_f_capacity,t3.aircraft_j_capacity,t3.aircraft_y_capacity,t3.aircraft_lease_ind,t3.etops_ind,t3.aircraft_version_code,t3.aircraft_owner,t3.potable_water_tank_count,t3.potable_water_tank_capacity_l,t3.no_engines,t2.flight_subtype from simple_tbl t2 inner join nested_tbl1 t1 on t1.flight_identifier = t2.flight_identifier inner join nested_tbl2 t3 on t1.flight_identifier = t3.flight_identifier").dropDuplicates

    ////FINDING DELTA
    val tbl_diff_df19 = table_diff(simple_join1,output_path,"OPS_STG_EDH_FLIGHT_HUB_MASTER",table_names,prev_snap_dates,currentDate)

    //DELETING CURRENT SNAP SHOT
    uri = new URI(output_path+"/OPS_STG_EDH_FLIGHT_HUB_MASTER/hx_snapshot_date="+currentDate)
    hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    tbl_diff_df19.coalesce(2).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path+"/OPS_STG_EDH_FLIGHT_HUB_MASTER")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+"/OPS_STG_EDH_FLIGHT_HUB_MASTER FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri19 : URI = new URI(output_path+"/OPS_STG_EDH_FLIGHT_HUB_MASTER/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri19),true)}

    //####################################################

  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path : String = null, compression: String = null, spark_partitions : Int = 0, no_of_shuffles : String = null, ctrl_file_path : String = null, snap_date : String = null)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("FlattenFlightMaster") {
      head("FlattenFlightMaster")
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
      opt[String]('d', "snap_date")
        .optional()
        .action((x, config) => config.copy(snap_date = x))
        .text("Required parameter : snapdate")
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
      log.info("[INFO] PERFORMING FLATTENING FLIGHT MASTER ...")
      val output_df = flatten_flight_master(config.hdfs_input_path,config.hdfs_output_path,config.no_of_shuffles,config.spark_partitions,config.compression,config.ctrl_file_path,config.snap_date)
      log.info("[INFO] PERFORMING FLATTENING FLIGHT MASTER IS COMPLETED SUCCESSFULLY")
    }
  }
}
