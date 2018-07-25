/*
*=====================================================================================================================================
* Created on     :   14/03/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Description    :   This spark code builds Satellite planned fuel burn
* ======================================================================================================================================
*/

//spark-submit --master yarn --driver-cores 4 --driver-memory 4g --num-executors 10 --total-executor-cores 4 --executor-cores 4 --executor-memory 4g --queue opseff --num-executors 5 --conf "spark.sql.shuffle.partitions=50" --queue opseff --class "com.emirates.helix.SatellitePlannedFuelBurn" /home/ops_eff/ek_lido/Genome_Lido_Planned_Fule_Burn-1.0-SNAPSHOT.jar -i /data/helix/modelled/emfg/dedup/lido/lido_fsum_route_level/* -f /data/helix/modelled/emfg/public/flightmaster/current/snapdate=2018-03-20/* -r /data/helix/modelled/emfg/rejected/lido/lido_route_level/snapdate=2018-03-22/sanptime=00-00/ -o /data/helix/modelled/emfg/public/fuelburnplanned/current/snapdate=2018-03-22/snaptime=00-00/ -c snappy -p 2 -b true -h /data/helix/modelled/emfg/public/fuelburnplanned/current/snapdate=2018-03-22/snaptime=00-00/ -m 5

package com.emirates.helix

import com.databricks.spark.avro._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat

import org.apache.spark.sql.hive._


object SatellitePlannedFuelBurn {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("SatellitePlannedFuelBurn")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("SatellitePlannedFuelBurn")

  def satellitePlannedFuelBurn(input_df : DataFrame,fltmstr_df : DataFrame,spark_partitions : Int) : (DataFrame,DataFrame) = {

    //import com.databricks.spark.avro._
    //val input_df = hiveContext.read.parquet("/data/helix/uat/modelled/emfg/dedupe/lido/lido_fsum_route_level/*")
    input_df.registerTempTable("ip_tbl")

    //SATELLITE PLANNED FUEL BURN
    val lido_satellite_cols_df = hiveContext.sql("select FliNum,DepAirIat as dep,DatOfOriUtc as flt_dt,DesAirIat as Des,RouOptCri,RegOrAirFinNum as Tail_no,AirDes as airline_code,NoOfWayOfMaiRouDepDes as total_waypoints,FliDupNo as version,altitude as flight_level,cast(waypoint_seq_no as string) as waypnt_seq_no,tibco_message_time,TrackWindSegment_drvd as WIND_COMP,cast(mora_or_mea_drvd as string) as MORA,waypointName as waypnt_name,cast(latitude_drvd as string) as LATITUDE,cast(longitude_drvd as string) as LONGITUDE,cast(dstToNxtWaypnt_drvd as string) as GRND_DIST,cast(estFltTimeToNxtWaypnt_drvd as string) as STM,cast(accum_distance_drvd as string) as ACC_DIST,cast(remaining_fuel_drvd as string) as REMAINING_FUEL,cast(altitude_drvd_upd as string) as ALTITUDE,cast(isa_dev_drvd as string) as ISA_DEV,cast(sr_tdev_drvd as string) as SR_TDEV,cast(segWndCmptCrs_drvd as string) as CROSS_WIND,cast(wind_drvd as string) as WIND,cast(machno_drvd as string) as MACHNO,cast(tas_drvd as string) as TAS,cast(trop_drvd as string) as TROP,cast(cas_drvd as string) as CAS,estFuelOnBrdOvrWaypnt as est_fob_waypnt,estTotFuelBurnOvrWaypnt as est_fbu_waypnt,outbndTrueTrk as true_heading,estElpTimeOvrWaypnt as ATM,SegTemp as seg_temp, airdist_drvd,acc_airdist_drvd,in_flight_status from ip_tbl")

    //FLIGHT_PLN_WAYPNT_FUEL_TIME_DETAILS
    val flight_pln_waypnt_fuel_time_details_df = lido_satellite_cols_df.withColumn("flight_pln_waypnt_fuel_time_details",struct($"waypnt_seq_no",$"waypnt_name",$"latitude",$"longitude",$"remaining_fuel",$"est_fob_waypnt",$"est_fbu_waypnt",$"stm",$"atm",$"tibco_message_time",$"version",$"in_flight_status",$"true_heading"))

    //FLIGHT_PLN_WAYPNT_DIST_SPEED_DETAILS
    val flight_pln_waypnt_dist_speed_details_df = flight_pln_waypnt_fuel_time_details_df.withColumn("flight_pln_waypnt_dist_speed_details",struct($"waypnt_seq_no",$"waypnt_name",$"altitude",$"in_flight_status",$"grnd_dist",$"acc_dist",$"wind",$"wind_comp",$"tas",$"cas",$"machno".as("mach_no"),$"isa_dev",$"sr_tdev",$"mora",$"seg_temp",$"cross_wind",$"airdist_drvd".as("air_dist_waypnt"),$"trop",$"flight_level",$"acc_airdist_drvd".as("acc_air_dist_waypnt"),$"tibco_message_time",$"version"))


    flight_pln_waypnt_dist_speed_details_df.registerTempTable("nested_tbl")
    hiveContext.cacheTable("nested_tbl")

    //FIND LATEST OF NESTED COLS
    val flight_pln_waypnt_fuel_time_details_latest = hiveContext.sql("select FliNum,dep,flt_dt,des,tail_no,airline_code,total_waypoints,flight_pln_waypnt_fuel_time_details as flight_pln_waypnt_fuel_time_details_latest,flight_pln_waypnt_dist_speed_details as flight_pln_waypnt_dist_speed_details_latest,DENSE_RANK() OVER (PARTITION BY FliNum,flt_dt,Dep,Des ORDER BY tibco_message_time DESC) as rank from nested_tbl where coalesce(trim(RouOptCri),'') != 'Y'").repartition(spark_partitions*100)

    val latest_df = flight_pln_waypnt_fuel_time_details_latest.where($"rank" === 1)

    latest_df.registerTempTable("latest_tbl")

    val groupby_latest = hiveContext.sql("select FliNum,dep,flt_dt,des,tail_no,concat_ws('#',collect_set(airline_code)) as airline_code,concat_ws('#',collect_set(total_waypoints)) as total_waypoints,collect_list(flight_pln_waypnt_fuel_time_details_latest) as flight_pln_waypnt_fuel_time_details_latest,collect_list(flight_pln_waypnt_dist_speed_details_latest) as flight_pln_waypnt_dist_speed_details_latest from latest_tbl group by FliNum,dep,flt_dt,des,tail_no")

    groupby_latest.registerTempTable(("groupby_latest_tbl"))

    //GROUP BY FLIGHT IDENTIFIERS
    val groupby_df = hiveContext.sql("select FliNum,dep,flt_dt,des,tail_no,collect_list(flight_pln_waypnt_fuel_time_details) as flight_pln_waypnt_fuel_time_details,collect_list(flight_pln_waypnt_dist_speed_details) as flight_pln_waypnt_dist_speed_details from nested_tbl group by FliNum,dep,flt_dt,des,tail_no")

    groupby_df.registerTempTable("groupby_tbl")

    //JOINING WITH SIMPLE COLS
    val join_df = hiveContext.sql("select t1.FliNum,t1.dep,t1.flt_dt,t1.des,t2.tail_no,t2.airline_code,t2.total_waypoints,t1.flight_pln_waypnt_fuel_time_details,t1.flight_pln_waypnt_dist_speed_details,t2.flight_pln_waypnt_fuel_time_details_latest,t2.flight_pln_waypnt_dist_speed_details_latest from groupby_tbl t1 left outer join groupby_latest_tbl t2 on t1.FliNum = t2.FliNum and t1.dep = t2.dep and t1.flt_dt = t2.flt_dt and t1.des = t2.des and t1.tail_no = t2.tail_no")

    //JOINING WITH FLIGHT MASTER
    //val fltmstr_df = hiveContext.read.parquet("/data/helix/uat/modelled/emfg/public/flightmaster/{history/snapdate=2018-04-19/snaptime=00-00,current/snapdate=2018-04-19/snaptime=00-00}")
    join_df.registerTempTable("satellite_tbl")
    fltmstr_df.registerTempTable("fltmstr_tbl")

    val final_df_not_joined_df = hiveContext.sql("select t2.flight_identifier,t1.* from satellite_tbl t1 left outer join fltmstr_tbl t2 on trim(t1.FliNum) = t2.flight_number and trim(t1.Dep) = t2.pln_dep_iata_station and to_date(trim(t1.flt_dt)) = t2.flight_date and trim(t1.Des) = t2.latest_pln_arr_iata_station and t2.actual_reg_number = trim(t1.tail_no) where flight_identifier is null")

    val final_df_joined_df = hiveContext.sql("select t2.flight_identifier,t1.* from satellite_tbl t1 left outer join fltmstr_tbl t2 on trim(t1.FliNum) = t2.flight_number and trim(t1.Dep) = t2.pln_dep_iata_station and to_date(trim(t1.flt_dt)) = t2.flight_date and trim(t1.Des) = t2.latest_pln_arr_iata_station and t2.actual_reg_number = trim(t1.tail_no) where flight_identifier is not null")

    val renamed_final_df_not_joined_df = final_df_not_joined_df.withColumnRenamed("FliNum","flight_number").withColumnRenamed("flt_dt","flight_date").withColumnRenamed("dep","pln_dep_iata_station").withColumnRenamed("des","latest_pln_arr_iata_station").withColumnRenamed("tail_no","pln_reg_number").withColumn("flight_date",to_date($"flight_date"))

    val renamed_final_df_joined_df = final_df_joined_df.withColumnRenamed("FliNum","flight_number").withColumnRenamed("flt_dt","flight_date").withColumnRenamed("dep","pln_dep_iata_station").withColumnRenamed("des","pln_arr_iata_station").withColumnRenamed("tail_no","pln_reg_number").withColumn("flight_date",to_date($"flight_date"))


    //final_df_joined_df.write.parquet("/data/helix/modelled/emfg/public/fuelburnplanned/current/snapdate=2018-03-21/sanptime=00-00/")

    return (renamed_final_df_joined_df,renamed_final_df_not_joined_df)
  }

  def writetohdfs(output_df : DataFrame, spark_partitions : Int, output_hdfs_path : String,rejected_df : DataFrame,rejected_hdfs_path : String,output_hdfs_path_hist : String = null,months : Int,incr_flag : Boolean) = {


    val rejected_df_last_incr_coalesece = rejected_df.coalesce(spark_partitions)
    log.info("[INFO] WRITING DATA TO REJECTED PATH " + rejected_hdfs_path + "...")
    hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    rejected_df_last_incr_coalesece.write.mode("overwrite").parquet(rejected_hdfs_path)
    log.info("[INFO] WRITING DATA TO REJECTED PATH " + rejected_hdfs_path + " successfully")

    if (incr_flag) {
      val output_df_last_incr_coalesece = output_df.coalesce(spark_partitions)
      log.info("[INFO] WRITING DATA TO OUTPUT PATH " + output_hdfs_path + "...")
      hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")
      output_df_last_incr_coalesece.write.option("mode", "overwrite").parquet(output_hdfs_path)
      log.info("[INFO] WRITING DATA TO OUTPUT PATH " + output_hdfs_path + " successfully")
    }
    else {
      output_df.registerTempTable("plnfuelburn_tbl")
      hiveContext.cacheTable("plnfuelburn_tbl")

      val max_flight_date = hiveContext.sql("select max(flight_date) from plnfuelburn_tbl").first().mkString
      log.info("[INFO] MAX FLIGHT DATE IS " + max_flight_date + "...")
      log.info("[INFO] LOOK BACK MONTHS IS " + months + "...")
      val incr_sql = "select * from plnfuelburn_tbl where to_date(flight_date) >= add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"
      val hist_sql = "select * from plnfuelburn_tbl where to_date(flight_date) < add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"

      log.info("[INFO] HISTORY SQL QUERY " + hist_sql + "...")
      log.info("[INFO] INCREMENTAL SQL QUERY " + incr_sql + "...")

      val output_df_last_incr = hiveContext.sql(incr_sql).repartition(spark_partitions*100)
      val output_df_last_hist = hiveContext.sql(hist_sql).repartition(spark_partitions*100)

      val output_df_last_incr_coalesece = output_df_last_incr.coalesce(spark_partitions)
      val output_df_last_hist_coalesece = output_df_last_hist.coalesce(spark_partitions)

      log.info("[INFO] WRITING DATA TO INCREMENTAL OUTPUT PATH " + output_hdfs_path + "...")
      output_df_last_incr_coalesece.write.mode("overwrite").parquet(output_hdfs_path)
      log.info("[INFO] WRITING DATA TO INCREMENTAL OUTPUT PATH " + output_hdfs_path + " successfully")

      log.info("[INFO] WRITING DATA TO OUTPUT PATH " + output_hdfs_path_hist + "...")
      output_df_last_hist_coalesece.write.mode("overwrite").parquet(output_hdfs_path_hist)
      log.info("[INFO] WRITING DATA TO OUTPUT PATH " + output_hdfs_path_hist + " successfully")

    }
  }
  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path : String = null, compression: String = null, spark_partitions : Int = 0, rejected_hdfs_path : String = null,emfg_hdfs_path : String = null,hdfs_output_path_hist : String = null,months : Int = 0,incr_flag : Boolean = true)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("SatellitePlannedFuleBurn") {
      head("SatellitePlannedFuleBurn")
      opt[String]('i', "hdfs_input_path")
        .required()
        .action((x, config) => config.copy(hdfs_input_path = x))
        .text("Required parameter : Input file path for data")
      opt[String]('f', "emfg_hdfs_path")
        .required()
        .action((x, config) => config.copy(emfg_hdfs_path = x))
        .text("Required parameter : emfg file path for data")
      opt[String]('o', "hdfs_output_path")
        .required()
        .action((x, config) => config.copy(hdfs_output_path = x))
        .text("Required parameter : Output file path for data")
      opt[String]('h', "hdfs_output_path_hist")
        .optional()
        .action((x, config) => config.copy(hdfs_output_path_hist = x))
        .text("Required parameter : Hist Output file path for data")
      opt[Int]('m', "months")
        .optional()
        .action((x, config) => config.copy(months = x))
        .text("Required parameter : look back months")
      opt[Boolean]('b', "incr_flag")
        .optional()
        .action((x, config) => config.copy(incr_flag = x))
        .text("Required parameter : Incremental flag not set")
      opt[String]('r', "rejected_hdfs_path")
        .required()
        .action((x, config) => config.copy(rejected_hdfs_path = x))
        .text("Required parameter : Rejected file path for data")
      opt[Int]('p', "spark_partitions")
        .required()
        .action((x, config) => config.copy(spark_partitions = x))
        .text("Required parameter : Spark partitions ")
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

      //READING DATA FORM PREREQUISITE
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_input_path + "...")
      val input_df = hiveContext.read.parquet(config.hdfs_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_input_path + "is successfull")

      //READING DATA FORM EMFG
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.emfg_hdfs_path + "...")
      val emfg_df = hiveContext.read.parquet(config.emfg_hdfs_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.emfg_hdfs_path + "is successfull")

      //CALLING FLIGHT_MASTER_LIDO
      log.info("[INFO] PERFORMING SATELLITE MAPPING (NESTING AND LATEST)) ...")
      val (output_df,rejected_df) = satellitePlannedFuelBurn(input_df,emfg_df,config.spark_partitions)
      log.info("[INFO] PERFORMING SATELLITE MAPPING (NESTING AND LATEST)) IS COMPLETED SUCCESSFULLY")

      //WRITING DATA TO PREREQUISITE IN AVRO FORMAT WITH SNAPPY COMPRESSION
      log.info("[INFO] LOOK BACK MONTHS IS " + config.months + "...")
      writetohdfs(output_df, config.spark_partitions, config.hdfs_output_path,rejected_df,config.rejected_hdfs_path,config.hdfs_output_path_hist,config.months,config.incr_flag)
    }
  }

}
