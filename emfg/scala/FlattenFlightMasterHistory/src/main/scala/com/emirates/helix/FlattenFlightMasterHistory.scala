/*
*=====================================================================================================================================
* Created on     :   28/03/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   FlattenFlightMasterHistory
* Description    :   This code flattens flightMaster data and stores in multiple tables
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue opseff --num-executors 10 --executor-cores 2 --driver-memory 4g --executor-memory 6g --conf spark.sql.shuffle.partitions=150 --class "com.emirates.helix.FlattenFlightMasterHistory" /home/ops_eff/ek_flightMaster/FlattenFlightMasterHistory-1.0-SNAPSHOT.jar -i /data/helix/modelled/emfg/public/flightmaster/history/snapdate=2018-03-29/snaptime=01-00 -o /data/helix/modelled/emfg/serving/flightMasterHistory -c snappy -p 1000 -s 150 -l 3

package com.emirates.helix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlattenFlightMasterHistory {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("Genome_Flight_Master_History")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Genome_Flight_Master_History")

  def flatten_flight_master_history(input_df : DataFrame,output_path : String, shuffle : String, partitions :  Int, compression : String,look_back_years : Int = 3)={

    //val input_df = hiveContext.read.parquet("/data/helix/modelled/emfg/public/flightmaster/history1.0/snapdate=2018-03-14/snaptime=00-00")
    //val look_back_years = 3
    //val partitions = 1000
    //val output_path = "/data/helix/modelled/emfg/serving/OPS_STG_EDH_HUB_MASTER_HIST"

    //CURRENT SNAP SHOT
    log.info("[INFO] GETTING CURRENT DATE FOR THE TODAYS LOADING ...")
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val currentDate = formatter.format(cal.getTime())
    log.info("[INFO] GETTING CURRENT DATE FOR THE TODAYS LOADING IS : "+currentDate)

    //SHUFFLE
    hiveContext.setConf("spark.sql.shuffle.partitions", shuffle)
    hiveContext.setConf("spark.sql.parquet.compression.codec", compression)

    //FILTERING INPUT DATA BASED LOOK BACK YEARS
    input_df.registerTempTable("ip_tbl")
    val max_flight_date = hiveContext.sql("select max(flight_date) from ip_tbl").first().mkString
    log.info("[INFO] MAX FLIGHT DATE IS " + max_flight_date + "...")
    log.info("[INFO] LOOK BACK YEARS IS " + look_back_years + "...")
    val hist_sql = "select * from ip_tbl where to_date(flight_date) >= add_months(to_date('" + max_flight_date + "')," + -1 * look_back_years * 12 + ")"
    log.info("[INFO] HISTORY SQL QUERY " + hist_sql + "...")
    val filtered_hist_df = hiveContext.sql(hist_sql)

    filtered_hist_df.registerTempTable("filtered_hist_tbl")
    hiveContext.cacheTable("filtered_hist_tbl")

    //SIMPLE COLUMNS DATAFRAME - add latest_pln_arr_iata_station - removed flight_delete_ind col
    val simple_cols_df = filtered_hist_df.select("act_arr_datetime_local","act_arr_datetime_utc","act_arr_iata_station","act_arr_icao_station","act_dep_datetime_local","act_dep_datetime_utc","act_dep_iata_station","act_dep_icao_station","act_flight_in_datetime_local","act_flight_in_datetime_utc","act_flight_off_datetime_local","act_flight_off_datetime_utc","act_flight_on_datetime_local","act_flight_on_datetime_utc","act_flight_out_datetime_local","act_flight_out_datetime_utc","actual_reg_number","airline_code","airline_name","arr_bag_carousel_number","flight_blocktime_act","flight_blocktime_orig","flight_cabin_door_closure_date_local","flight_cargo_door_closure_date_local","flight_date","flight_dep_number","flight_identifier","flight_leg_number","flight_number","flight_service_desc","flight_service_type","flight_suffix","pln_arr_iata_station","pln_arr_icao_station","latest_pln_arr_iata_station","pln_dep_iata_station","pln_dep_icao_station","sch_arr_datetime_local","sch_arr_datetime_utc","sch_dep_datetime_local","sch_dep_datetime_utc","flight_via_route","aircraft_subtype").withColumnRenamed("aircraft_subtype","flight_subtype").withColumnRenamed("flight_cabin_door_closure_date_local","flight_cabin_door_closure_date").withColumnRenamed("flight_cargo_door_closure_date_local","flight_cargo_door_closure_date").dropDuplicates()

    //NESTED COLUMNS DATAFRAME -- remove quotes to aircraft_details[potal_water_tank_count,aircraft_details[potal_water_tank_capacity_l] after flt_mstr changes
    val nested_cols_df = hiveContext.sql("select flight_identifier,concat(coalesce(pln_runway_details_latest.pln_dep_runway,''),'||',coalesce(pln_runway_details_latest.pln_arr_runway,'')) as pln_runway_details,concat(coalesce(act_runway_details['act_dep_runway'],''),'||',coalesce(act_runway_details['act_arr_runway'],'')) as act_runway_details,concat(coalesce(act_bay_details.act_dep_bay_number,''),'||',coalesce(act_bay_details.act_arr_bay_number,'')) as act_bay_details ,concat(coalesce(act_dep_gate_details.act_dep_gate_number,''),'||',coalesce(act_dep_gate_details.act_dep_concourse_number,''),'||',coalesce(act_dep_gate_details.act_dep_terminal_number,'')) as act_dep_gate_details ,concat(coalesce(act_arr_gate_details.act_arr_gate_number,''),'||',coalesce(act_arr_gate_details.act_arr_concourse_number,''),'||',coalesce(act_arr_gate_details.act_arr_terminal_number,'')) as act_arr_gate_details ,flight_cost_index_details_latest.cost_index,concat(coalesce(flight_pln_altn_airport_details_latest.pln_alt_apt1_iata,''),'||',coalesce(flight_pln_altn_airport_details_latest.pln_alt_apt1_icao,''),'||',coalesce(flight_pln_altn_airport_details_latest.pln_alt_apt2_iata,''),'||',coalesce(flight_pln_altn_airport_details_latest.pln_alt_apt2_icao,''),'||',coalesce(flight_pln_altn_airport_details_latest.pln_alt_apt3_iata,''),'||',coalesce(flight_pln_altn_airport_details_latest.pln_alt_apt3_icao,''),'||',coalesce(flight_pln_altn_airport_details_latest.pln_alt_apt4_iata,''),'||',coalesce(flight_pln_altn_airport_details_latest.pln_alt_apt4_icao,'')) as  flight_pln_altn_apt_details,concat(coalesce(aircraft_details['aircraft_mfr'],''),'||',coalesce(aircraft_details['aircraft_type'],''),'||',coalesce(aircraft_details['aircraft_subtype'],''),'||',coalesce(aircraft_details['aircraft_config'],''),'||',coalesce(aircraft_details['aircraft_tot_capcity'],''),'||',coalesce(aircraft_details['aircraft_F_capacity'],''),'||',coalesce(aircraft_details['aircraft_J_capacity'],''),'||',coalesce(aircraft_details['aircraft_Y_capacity'],''),'||',coalesce(aircraft_details['aircraft_lease_ind'],''),'||',coalesce(aircraft_details['ETOPS_Ind'],''),'||',coalesce(aircraft_details['aircraft_version_code'],''),'||',coalesce(aircraft_details['aircraft_owner'],''),'||',coalesce(aircraft_details['potable_water_tank_count'],''),'||',coalesce(aircraft_details['potable_water_tank_capacity_l'],''),'||',coalesce(aircraft_details['no_engines'],'')) as aircraft_details from filtered_hist_tbl").dropDuplicates()

    //ARRAY NESTED COLUMNS DATAFRAME - pln_reg_details - latest and before latest
    val pln_reg_details_df = hiveContext.sql("select flight_identifier,pln_reg_details_expld['pln_reg_number'] as pln_reg_number,pln_reg_details_expld['msg_recorded_datetime'] as pln_posted_datetime from (select flight_identifier,explode(pln_reg_details) as pln_reg_details_expld  from filtered_hist_tbl)src").dropDuplicates()

    pln_reg_details_df.registerTempTable("pln_reg_details_tbl")

    val pln_reg_details_rank_df = hiveContext.sql("select flight_identifier,pln_reg_number,pln_posted_datetime,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY pln_posted_datetime desc) as rank from pln_reg_details_tbl where cast(trim(pln_reg_number) as int) is null and trim(pln_reg_number) is not null")

    val pln_reg_details_rank_1_2_df = pln_reg_details_rank_df.where($"rank".isin(1,2))

    pln_reg_details_rank_1_2_df.registerTempTable("pln_reg_details_rank_1_2_tbl")

    val pln_reg_details_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(pln_reg_number,''),'||',coalesce(pln_posted_datetime,'')) as pln_reg_details from pln_reg_details_rank_1_2_tbl order by flight_identifier,pln_posted_datetime")

    pln_reg_details_concat_df.registerTempTable("pln_reg_details_concat_tbl")

    val pln_reg_details_groupby_df = hiveContext.sql("select flight_identifier,concat_ws('#',collect_list(pln_reg_details)) as  pln_reg_details from pln_reg_details_concat_tbl group by flight_identifier")

    //ARRAY NESTED COLUMNS DATAFRAME - flight_delay_details - all delay needed
    val flight_delay_details_df = hiveContext.sql("select flight_identifier,flight_delay_details_expld['delay_code'] as delay_code,flight_delay_details_expld['delay_reason'] as delay_reason,flight_delay_details_expld['delay_dept_code'] as delay_dept_code,flight_delay_details_expld['delay_duration_mins'] as delay_duration_mins,flight_delay_details_expld['delay_iata_station'] as delay_iata_station,flight_delay_details_expld['delay_icao_station'] as delay_icao_station,flight_delay_details_expld['delay_type'] as delay_type,flight_delay_details_expld['delay_posted_datetime'] as delay_posted_datetime from (select flight_identifier,explode(flight_delay_details) as flight_delay_details_expld from filtered_hist_tbl) src").dropDuplicates

    flight_delay_details_df.registerTempTable("flight_delay_details_tbl")

    val flight_delay_details_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(delay_code,''),'||',coalesce(delay_reason,''),'||',coalesce(delay_dept_code,''),'||',coalesce(delay_duration_mins,''),'||',coalesce(delay_iata_station,''),'||',coalesce(delay_icao_station,''),'||',coalesce(delay_type,''),'||',coalesce(delay_posted_datetime,'')) as flight_delay_details from flight_delay_details_tbl order by flight_identifier,delay_posted_datetime")

    flight_delay_details_concat_df.registerTempTable("flight_delay_details_concat_tbl")

    val flight_delay_details_groupby_df = hiveContext.sql("select flight_identifier,concat_ws('#',collect_list(flight_delay_details)) as  flight_delay_details from flight_delay_details_concat_tbl group by flight_identifier")

    //ARRAY NESTED COLUMNS DATAFRAME - flight_status_details - only latest is needed (??) -fix for status dup with respect to flt_status_recorded_datetime
    val flight_status_details_df = hiveContext.sql("select flight_identifier,flight_status_details_expld['flight_status'] as flight_status,flight_status_details_expld['flight_status_desc'] as flight_status_desc,flight_status_details_expld['msg_recorded_datetime'] as flt_status_recorded_datetime from (select flight_identifier,explode(flight_status_details) as flight_status_details_expld from filtered_hist_tbl)src").dropDuplicates

    flight_status_details_df.registerTempTable("flight_status_details_tbl")

    val flight_status_details_rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY flt_status_recorded_datetime desc) as rank from flight_status_details_tbl")

    //FIX DUP
    //flight_status_details_rank_df.where($"rank" === 1).where(!upper($"flight_status").isin("PDEP")).registerTempTable("flight_status_details_rank_tbl")

    flight_status_details_rank_df.where($"rank" === 1).registerTempTable("flight_status_details_rank_tbl")

    //val cnld_delete_df = hiveContext.sql("select *, DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY from flight_status)")

    val flight_status_details_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(flight_status,''),'||',coalesce(flight_status_desc,''),'||',coalesce(flt_status_recorded_datetime,'')) as flight_status_details from flight_status_details_rank_tbl order by flight_identifier,flt_status_recorded_datetime")

    flight_status_details_concat_df.registerTempTable("flight_status_details_concat_tbl")

    val flight_status_details_groupby_df = hiveContext.sql("select flight_identifier,concat_ws('#',collect_list(flight_status_details)) as flight_status_details from flight_status_details_concat_tbl group by flight_identifier")


    //ARRAY NESTED COLUMNS DATAFRAME - pln_dep_bay_details - only latest is needed - fix dup issue for pln bay details
    val pln_dep_bay_details_df = hiveContext.sql("select flight_identifier,act_dep_bay_number,pln_dep_bay_details_expld.pln_dep_old_bay_number as pln_dep_old_bay_number,pln_dep_bay_details_expld.pln_dep_new_bay_number as pln_dep_new_bay_number,pln_dep_bay_details_expld.pln_dep_bay_change_reason_code as pln_dep_bay_change_reason_code,pln_dep_bay_details_expld.pln_dep_bay_change_reason_desc as pln_dep_bay_change_reason_desc,pln_dep_bay_details_expld.dep_bay_recorded_datetime as dep_bay_recorded_datetime from (select flight_identifier,act_bay_details.act_dep_bay_number,explode(pln_dep_bay_details) as pln_dep_bay_details_expld from filtered_hist_tbl)src").dropDuplicates

    pln_dep_bay_details_df.registerTempTable("pln_dep_bay_details_tbl")

    val pln_dep_bay_details_rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY dep_bay_recorded_datetime desc) as rank from pln_dep_bay_details_tbl")

    pln_dep_bay_details_rank_df.where($"rank" === 1).where($"act_dep_bay_number" === $"pln_dep_new_bay_number").registerTempTable("pln_dep_bay_details_rank_tbl")

    val pln_dep_bay_details_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(pln_dep_old_bay_number,''),'||',coalesce(pln_dep_new_bay_number,''),'||',coalesce(pln_dep_bay_change_reason_code,''),'||',coalesce(pln_dep_bay_change_reason_desc,''),'||',coalesce(dep_bay_recorded_datetime,'')) as pln_dep_bay_details from pln_dep_bay_details_rank_tbl")

    //ARRAY NESTED COLUMNS DATAFRAME - pln_arr_bay_details - only the latest is needed - fix dup issue for pln bay details
    val pln_arr_bay_details_df = hiveContext.sql("select flight_identifier,act_arr_bay_number,pln_arr_bay_details_expld.pln_arr_old_bay_number as pln_arr_old_bay_number,pln_arr_bay_details_expld.pln_arr_new_bay_number as pln_arr_new_bay_number,pln_arr_bay_details_expld.pln_arr_bay_change_reason_code as pln_arr_bay_change_reason_code,pln_arr_bay_details_expld.pln_arr_bay_change_reason_desc as pln_arr_bay_change_reason_desc,pln_arr_bay_details_expld.arr_bay_recorded_datetime as arr_bay_recorded_datetime from (select flight_identifier,act_bay_details.act_arr_bay_number,explode(pln_arr_bay_details) as pln_arr_bay_details_expld from filtered_hist_tbl)src").dropDuplicates

    pln_arr_bay_details_df.registerTempTable("pln_arr_bay_details_tbl")

    val pln_arr_bay_details_rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY arr_bay_recorded_datetime desc) as rank from pln_arr_bay_details_tbl")

    pln_arr_bay_details_rank_df.where($"rank" === 1).where($"act_arr_bay_number" === $"pln_arr_new_bay_number").registerTempTable("pln_arr_bay_details_rank_tbl")

    val pln_arr_bay_details_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(pln_arr_old_bay_number,''),'||',coalesce(pln_arr_new_bay_number,''),'||',coalesce(pln_arr_bay_change_reason_code,''),'||',coalesce(pln_arr_bay_change_reason_desc,''),'||',coalesce(arr_bay_recorded_datetime,'')) as pln_arr_bay_details from pln_arr_bay_details_rank_tbl")


    //ARRAY NESTED COLUMNS DATAFRAME - pln_arr_gate_details - only the latest is needed
    val pln_arr_gate_details_df = hiveContext.sql("select flight_identifier,pln_arr_gate_details_expld.pln_arr_gate_number,pln_arr_gate_details_expld.pln_arr_concourse_number,pln_arr_gate_details_expld.pln_arr_terminal_number,pln_arr_gate_details_expld.pln_arr_datetime_utc,pln_arr_gate_details_expld.pln_arr_datetime_local,pln_arr_gate_details_expld.arr_gate_recorded_datetime from (select flight_identifier,explode(pln_arr_gate_details) as pln_arr_gate_details_expld from filtered_hist_tbl)src").dropDuplicates

    pln_arr_gate_details_df.registerTempTable("pln_arr_gate_details_tbl")

    val pln_arr_gate_details_rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY arr_gate_recorded_datetime desc) as rank from pln_arr_gate_details_tbl")

    pln_arr_gate_details_rank_df.where($"rank" === 1).registerTempTable("pln_arr_gate_details_rank_tbl")

    val pln_arr_gate_details_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(pln_arr_gate_number,''),'||',coalesce(pln_arr_concourse_number,''),'||',coalesce(pln_arr_terminal_number,''),'||',coalesce(pln_arr_datetime_utc,''),'||',coalesce(pln_arr_datetime_local,''),'||',coalesce(arr_gate_recorded_datetime,'')) as pln_arr_gate_details from pln_arr_gate_details_rank_tbl")


    //ARRAY NESTED COLUMNS DATAFRAME - pln_dep_gate_details - only the latest is needed
    val pln_dep_gate_details_df = hiveContext.sql("select flight_identifier,pln_dep_gate_details_expld.pln_dep_gate_number,pln_dep_gate_details_expld.pln_dep_concourse_number,pln_dep_gate_details_expld.pln_dep_terminal_number,pln_dep_gate_details_expld.pln_dep_datetime_utc,pln_dep_gate_details_expld.pln_dep_datetime_local,pln_dep_gate_details_expld.dep_gate_recorded_datetime from (select flight_identifier,explode(pln_dep_gate_details) as pln_dep_gate_details_expld from filtered_hist_tbl)src").dropDuplicates

    pln_dep_gate_details_df.registerTempTable("pln_dep_gate_details_tbl")

    val pln_dep_gate_details_rank_df = hiveContext.sql("select *, DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY dep_gate_recorded_datetime desc) as rank from pln_dep_gate_details_tbl")

    pln_dep_gate_details_rank_df.where($"rank" === 1).registerTempTable("pln_dep_gate_details_rank_tbl")

    val pln_dep_gate_details_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(pln_dep_gate_number,''),'||',coalesce(pln_dep_concourse_number,''),'||',coalesce(pln_dep_terminal_number,''),'||',coalesce(pln_dep_datetime_utc,''),'||',coalesce(pln_dep_datetime_local,''),'||',coalesce(dep_gate_recorded_datetime,'')) as pln_dep_gate_details from pln_dep_gate_details_rank_tbl")

    //ARRAY NESTED COLUMNS DATAFRAME - est_flight_out_datetime - olny the latest is needed
    val est_flight_out_datetime_df = hiveContext.sql("select flight_identifier,est_flight_out_datetime_expld['est_flight_out_datetime_local'] as est_flight_out_datetime_local,est_flight_out_datetime_expld['est_flight_out_datetime_utc'] as est_flight_out_datetime_utc,est_flight_out_datetime_expld['msg_recorded_datetime'] as est_out_posted_datetime from (select flight_identifier,explode(est_flight_out_datetime) as est_flight_out_datetime_expld from filtered_hist_tbl)src").dropDuplicates

    est_flight_out_datetime_df.registerTempTable("est_flight_out_datetime_tbl")

    val est_flight_out_datetime_rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY est_out_posted_datetime desc) as rank from est_flight_out_datetime_tbl")

    est_flight_out_datetime_rank_df.where($"rank" ===  1).registerTempTable("est_flight_out_datetime_rank_tbl")

    val est_flight_out_datetime_dup_fix_df = hiveContext.sql("select * from (select *, ROW_NUMBER() OVER (PARTITION BY flight_identifier ORDER BY est_flight_out_datetime_local desc) as row_no from est_flight_out_datetime_rank_tbl) A where row_no = 1 ")

    est_flight_out_datetime_dup_fix_df.registerTempTable("est_flight_out_datetime_dup_fix_tbl")

    val est_flight_out_datetime_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(est_flight_out_datetime_local,''),'||',coalesce(est_flight_out_datetime_utc,''),'||',coalesce(est_out_posted_datetime,'')) as est_flight_out_datetime from est_flight_out_datetime_dup_fix_tbl")

    //ARRAY NESTED COLUMNS DATAFRAME - est_flight_in_datetime - only the latest is needed
    val est_flight_in_datetime_df = hiveContext.sql("select flight_identifier,est_flight_in_datetime_expld['est_flight_in_datetime_local'] as est_flight_in_datetime_local,est_flight_in_datetime_expld['est_flight_in_datetime_utc'] as est_flight_in_datetime_utc,est_flight_in_datetime_expld['msg_recorded_datetime'] as est_in_posted_datetime from (select flight_identifier,explode(est_flight_in_datetime) as est_flight_in_datetime_expld from filtered_hist_tbl)src").dropDuplicates

    est_flight_in_datetime_df.registerTempTable("est_flight_in_datetime_tbl")

    val est_flight_in_datetime_rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY est_in_posted_datetime desc) as rank from est_flight_in_datetime_tbl")

    est_flight_in_datetime_rank_df.where($"rank" === 1).registerTempTable("est_flight_in_datetime_rank_tbl")

    val est_flight_in_datetime_dup_fix_df = hiveContext.sql("select * from (select *, ROW_NUMBER() OVER (PARTITION BY flight_identifier ORDER BY est_flight_in_datetime_local desc) as row_no from est_flight_in_datetime_rank_tbl) A where row_no = 1")

    est_flight_in_datetime_dup_fix_df.registerTempTable("est_flight_in_datetime_dup_fix_tbl")

    val est_flight_in_datetime_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(est_flight_in_datetime_local,''),'||',coalesce(est_flight_in_datetime_utc,''),'||',coalesce(est_in_posted_datetime,'')) as est_flight_in_datetime from est_flight_in_datetime_dup_fix_tbl")


    //ARRAY NESTED COLUMNS DATAFRAME - est_flight_off_datetime
    val est_flight_off_datetime_df = hiveContext.sql("select flight_identifier,est_flight_off_datetime_expld['est_flight_off_datetime_local'] as est_flight_off_datetime_local,est_flight_off_datetime_expld['est_flight_off_datetime_utc'] as est_flight_off_datetime_utc,est_flight_off_datetime_expld['msg_recorded_datetime'] as est_off_posted_datetime from (select flight_identifier,explode(est_flight_off_datetime) as est_flight_off_datetime_expld from filtered_hist_tbl)src").dropDuplicates

    est_flight_off_datetime_df.registerTempTable("est_flight_off_datetime_tbl")

    val est_flight_off_datetime_rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY est_off_posted_datetime desc) as rank from est_flight_off_datetime_tbl")

    est_flight_off_datetime_rank_df.where($"rank" === 1).registerTempTable("est_flight_off_datetime_rank_tbl")

    val est_flight_off_datetime_dup_fix_df = hiveContext.sql("select * from (select *, ROW_NUMBER() OVER (PARTITION BY flight_identifier ORDER BY est_flight_off_datetime_local desc) as row_no from est_flight_off_datetime_rank_tbl) A where row_no = 1")

    est_flight_off_datetime_dup_fix_df.registerTempTable("est_flight_off_datetime_dup_fix_tbl")

    val est_flight_off_datetime_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(est_flight_off_datetime_local,''),'||',coalesce(est_flight_off_datetime_utc,''),'||',coalesce(est_off_posted_datetime,'')) as est_flight_off_datetime from est_flight_off_datetime_dup_fix_tbl")

    //ARRAY NESTED COLUMNS DATAFRAME - est_flight_on_datetime - needed only the latest
    val est_flight_on_datetime_df = hiveContext.sql("select flight_identifier,est_flight_on_datetime_expld['est_flight_on_datetime_local'] as est_flight_on_datetime_local,est_flight_on_datetime_expld['est_flight_on_datetime_utc'] as est_flight_on_datetime_utc,est_flight_on_datetime_expld['msg_recorded_datetime'] as est_on_posted_datetime from (select flight_identifier,explode(est_flight_on_datetime) as est_flight_on_datetime_expld from filtered_hist_tbl)src").dropDuplicates

    est_flight_on_datetime_df.registerTempTable("est_flight_on_datetime_tbl")

    val est_flight_on_datetime_rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY flight_identifier ORDER BY est_on_posted_datetime desc) as rank from est_flight_on_datetime_tbl")

    est_flight_on_datetime_rank_df.where($"rank" === 1).registerTempTable("est_flight_on_datetime_rank_tbl")

    val est_flight_on_datetime_dup_fix_df = hiveContext.sql("select * from (select *,ROW_NUMBER() OVER (PARTITION BY flight_identifier ORDER BY est_flight_on_datetime_local desc) as row_no from est_flight_on_datetime_tbl) A where row_no = 1")


    est_flight_on_datetime_dup_fix_df.registerTempTable("est_flight_on_datetime_dup_fix_tbl")

    val est_flight_on_datetime_concat_df = hiveContext.sql("select flight_identifier,concat(coalesce(est_flight_on_datetime_local,''),'||',coalesce(est_flight_on_datetime_utc,''),'||',coalesce(est_on_posted_datetime,'')) as est_flight_on_datetime from est_flight_on_datetime_dup_fix_tbl")

    //SAMPLE DATA PRINT FOR DEBUGGING
    //simple_cols_df.show(5,false)
    //nested_cols_df.show(5,false)
    // pln_reg_details_groupby_df.show(5,false)
    //flight_delay_details_groupby_df.show(5,false)
    //flight_status_details_concat_df.show(5,false)
    //pln_dep_bay_details_concat_df.show(5,false)
    //pln_arr_bay_details_concat_df.show(5,false)
    //pln_arr_gate_details_concat_df.show(5,false)
    //pln_dep_gate_details_concat_df.show(5,false)
    //est_flight_out_datetime_concat_df.show(5,false)
    //est_flight_in_datetime_concat_df.show(5,false)
    //est_flight_off_datetime_concat_df.show(5,false)
    //est_flight_on_datetime_concat_df.show(5,false)

    //JOINING ALL COLS
    val join_all_df = simple_cols_df.join(nested_cols_df,Seq("flight_identifier"),"left_outer").cache().repartition(partitions).join(pln_reg_details_groupby_df,Seq("flight_identifier"),"left_outer").join(flight_delay_details_groupby_df,Seq("flight_identifier"),"left_outer").join(flight_status_details_concat_df,Seq("flight_identifier"),"left_outer").join(pln_dep_bay_details_concat_df,Seq("flight_identifier"),"left_outer").join(pln_arr_bay_details_concat_df,Seq("flight_identifier"),"left_outer").join(pln_arr_gate_details_concat_df,Seq("flight_identifier"),"left_outer").join(pln_dep_gate_details_concat_df,Seq("flight_identifier"),"left_outer").join(est_flight_out_datetime_concat_df,Seq("flight_identifier"),"left_outer").join(est_flight_in_datetime_concat_df,Seq("flight_identifier"),"left_outer").join(est_flight_off_datetime_concat_df,Seq("flight_identifier"),"left_outer").join(est_flight_on_datetime_concat_df,Seq("flight_identifier"),"left_outer").withColumn("hx_snapshot_date",current_date())

    //DELETING CURRENT SNAP SHOT
    val uri = new URI(output_path+"/hx_snapshot_date="+currentDate)
    val hdfs = FileSystem.get(uri,sc.hadoopConfiguration)
    try{hdfs.delete(new Path(uri),true)}catch{case e:Exception => println("")}

    //WRITING TO HDFS
    import org.apache.spark.sql.types.IntegerType
    join_all_df.withColumn("ACTUAL_REG_NUMBER",when(trim($"ACTUAL_REG_NUMBER").cast(IntegerType).isNull,$"ACTUAL_REG_NUMBER")).select("FLIGHT_IDENTIFIER","FLIGHT_NUMBER","ACTUAL_REG_NUMBER","PLN_REG_DETAILS","FLIGHT_DATE","PLN_DEP_IATA_STATION","PLN_DEP_ICAO_STATION","PLN_ARR_IATA_STATION","PLN_ARR_ICAO_STATION","LATEST_PLN_ARR_IATA_STATION","ACT_DEP_IATA_STATION","ACT_DEP_ICAO_STATION","ACT_ARR_IATA_STATION","ACT_ARR_ICAO_STATION","SCH_DEP_DATETIME_UTC","SCH_DEP_DATETIME_LOCAL","SCH_ARR_DATETIME_UTC","SCH_ARR_DATETIME_LOCAL","ACT_DEP_DATETIME_UTC","ACT_DEP_DATETIME_LOCAL","ACT_ARR_DATETIME_UTC","ACT_ARR_DATETIME_LOCAL","FLIGHT_DELAY_DETAILS","FLIGHT_SUBTYPE","FLIGHT_SERVICE_TYPE","FLIGHT_SERVICE_DESC","FLIGHT_STATUS_DETAILS","AIRLINE_CODE","AIRLINE_NAME","PLN_RUNWAY_DETAILS","ACT_RUNWAY_DETAILS","PLN_DEP_BAY_DETAILS","PLN_ARR_BAY_DETAILS","ACT_BAY_DETAILS","PLN_DEP_GATE_DETAILS","PLN_ARR_GATE_DETAILS","ACT_DEP_GATE_DETAILS","ACT_ARR_GATE_DETAILS","ARR_BAG_CAROUSEL_NUMBER","COST_INDEX","AIRCRAFT_DETAILS","FLIGHT_SUFFIX","FLIGHT_LEG_NUMBER","FLIGHT_DEP_NUMBER","FLIGHT_VIA_ROUTE","FLIGHT_PLN_ALTN_APT_DETAILS","FLIGHT_CABIN_DOOR_CLOSURE_DATE","FLIGHT_CARGO_DOOR_CLOSURE_DATE","FLIGHT_BLOCKTIME_ORIG","FLIGHT_BLOCKTIME_ACT","ACT_FLIGHT_OUT_DATETIME_UTC","ACT_FLIGHT_OUT_DATETIME_LOCAL","ACT_FLIGHT_OFF_DATETIME_UTC","ACT_FLIGHT_OFF_DATETIME_LOCAL","ACT_FLIGHT_ON_DATETIME_UTC","ACT_FLIGHT_ON_DATETIME_LOCAL","ACT_FLIGHT_IN_DATETIME_UTC","ACT_FLIGHT_IN_DATETIME_LOCAL","EST_FLIGHT_OUT_DATETIME","EST_FLIGHT_OFF_DATETIME","EST_FLIGHT_ON_DATETIME","EST_FLIGHT_IN_DATETIME","hx_snapshot_date").where($"AIRLINE_CODE" === "EK").coalesce((partitions/100).toInt + 1).write.partitionBy("hx_snapshot_date").mode("append").parquet(output_path)
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+output_path+" FINISHED SUCCESSFULLY")

    //WRITING SUCCESS FLAG
    var uri19 : URI = new URI(output_path+"/hx_snapshot_date="+currentDate+"/_SUCCESS")
    try{hdfs.create(new Path(uri19),true)}
  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path : String = null, compression: String = null, spark_partitions : Int = 0, no_of_shuffles : String = null,look_back_years : Int = 3)

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
      opt[Int]('p', "spark_partitions")
        .required()
        .action((x, config) => config.copy(spark_partitions = x))
        .text("Required parameter : spark no of partitions")
      opt[Int]('l', "look_back_years")
        .required()
        .action((x, config) => config.copy(look_back_years = x))
        .text("Required parameter : look back years")
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
      val hist_input_df = hiveContext.read.parquet(config.hdfs_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_input_path + "is successfull")


      //CALLING FLIGHT_MASTER_LIDO
      log.info("[INFO] PERFORMING FLATTENING FLIGHT MASTER HISTORY...")
      flatten_flight_master_history(hist_input_df,config.hdfs_output_path,config.no_of_shuffles,config.spark_partitions,config.compression,config.look_back_years)
      log.info("[INFO] PERFORMING FLATTENING FLIGHT MASTER HISTROY IS COMPLETED SUCCESSFULLY")
    }
  }


}
