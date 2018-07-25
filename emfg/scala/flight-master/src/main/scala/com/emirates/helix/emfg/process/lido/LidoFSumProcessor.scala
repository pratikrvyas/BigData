/*
*=======================================================================================================
* Created on     :   17/01/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   Genome_Lido_Fsum_Flight_Master
* Description    :   This is spark application is used to dedup lido fsum flight level (HIST+INCR)
* ======================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.helix.emirates.Genome_Lido_Fsum_Flight_Master" /home/ops_eff/ek_lido/Genome_Lido_Fsum_Flight_Master-1.0-SNAPSHOT.jar -i /data/helix/modelled/flight_genome/dedup/lido/lido_fsum_flight_level/current/1.0/22-01-2018/00-00/ -w true -o /data/helix/modelled/flight_genome/processing/lido_fsum_flight_master/1.0/ -c snappy -p 2

package com.emirates.helix.emfg.process.lido

import java.io.Serializable

import com.emirates.helix.emfg.FlightMasterArguments.Args
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}

/**
  * Companion object for DivertedFlightProcessor class
  */
object LidoFSumProcessor {

  def apply(args: Args): LidoFSumProcessor = {
    var processor = new LidoFSumProcessor()
    processor.args = args
    processor
  }
}

/**
  * LIDO data processing for Flight Master
  */

class LidoFSumProcessor extends Serializable {
  private var args: Args = _

  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("Genome_Lido_Fsum_Flight_Master")


  //PLN_RUNWAY_DETAILS
  case class  pln_runway_details_case(pln_dep_runway : String, pln_arr_runway : String, version : String,in_flight_status : String, datetime : String, helix_uuid : String, helix_time : String)

  //FLIGHT_COST_INDEX
  case class  flight_cost_index_case(cost_index : String, version : String,in_flight_status : String, datetime : String, helix_uuid : String, helix_time : String)

  //FLIGHT_PLN_ALTN_AIRPORT_DETAILS
  case class flight_pln_altn_airport_details_case(pln_alt_apt1_iata: String, pln_alt_apt1_icao: String, pln_alt_apt2_iata: String, pln_alt_apt2_icao: String, pln_alt_apt3_iata: String, pln_alt_apt3_icao: String, pln_alt_apt4_iata: String, pln_alt_apt4_icao: String, version: String, datetime: String, in_flight_status: String, helix_uuid: String, helix_time: String)

  //PLN_REG_DETAILS
  case class pln_reg_details_case(pln_reg_number: String, version: String, datetime: String, helix_uuid: String, helix_time: String)

  /**
    *
    * @param input_df LIDO deduped input data frame
    * @param sqlContext
    * @return LIDO processed data for Flight Master
    *         This is used to generate LIDO data for Flight Master
    */

  def generateLIDODataForFM(input_df: DataFrame)(implicit sqlContext: HiveContext): DataFrame = {

    import sqlContext.implicits._

    input_df.registerTempTable("ip_tbl")

    //LIDO_FLIGHT_MASTER COLS
    val lido_flight_master_cols_df = sqlContext.sql("select FliNum,DepAirIat as dep,to_date(DatOfOriUtc) as flt_dt,DesAirIat as Des,RegOrAirFinNum as Tail_no,AtcCal,FliDupNo as ver,CosIndEcoCru as cost_ind,DepRun,ArrRun,FirstAltIat as AltApt1,SecondAltIat as AltApt2,ThirdAltIat as AltApt3,FourthAltIat as AltApt4,FirstAltIca,SecondAltIca,ThirdAltIca,FourthAltIca,RouOptCri,tibco_message_time as msg_time,helix_timestamp as helix_time,helix_uuid from  ip_tbl")

    //PLN_RUNWAY_DETAILS
    val pln_runway_details_udf = udf((DepRun : String ,ArrRun : String,FliDupNo : String,RouOptCri : String,tibco_message_time  : String,helix_uuid : String, helix_time : String) => pln_runway_details_case(DepRun,ArrRun,FliDupNo,RouOptCri,tibco_message_time,helix_uuid,helix_time))

    val pln_runway_details_col_df = lido_flight_master_cols_df.withColumn("pln_runway_details",pln_runway_details_udf($"DepRun",$"ArrRun",$"ver",coalesce($"RouOptCri",lit("")),$"msg_time",$"helix_uuid",$"helix_time"))

    //FLIGHT_COST_INDEX
    val flight_cost_index_udf = udf((cost_ind : String,FliDupNo : String,RouOptCri : String, tibco_message_time  : String,helix_uuid : String, helix_time : String) => flight_cost_index_case(cost_ind,FliDupNo,RouOptCri,tibco_message_time,helix_uuid,helix_time))

    val flight_cost_index_col_df = pln_runway_details_col_df.withColumn("flight_cost_index_details",flight_cost_index_udf($"cost_ind",$"ver",coalesce($"RouOptCri",lit("")),$"msg_time",$"helix_uuid",$"helix_time"))

    //FLIGHT_PLN_ALTN_AIRPORT_DETAILS
    val flight_pln_altn_airport_details_udf = udf((pln_alt_apt1_iata: String, pln_alt_apt1_icao: String, pln_alt_apt2_iata: String, pln_alt_apt2_icao: String, pln_alt_apt3_iata: String, pln_alt_apt3_icao: String, pln_alt_apt4_iata: String, pln_alt_apt4_icao: String, FliDupNo: String, tibco_message_time_helix_uuid_time: String) => flight_pln_altn_airport_details_case(pln_alt_apt1_iata, pln_alt_apt1_icao, pln_alt_apt2_iata, pln_alt_apt2_icao, pln_alt_apt3_iata, pln_alt_apt3_icao, pln_alt_apt4_iata, pln_alt_apt4_icao, FliDupNo, tibco_message_time_helix_uuid_time.split("//")(1), tibco_message_time_helix_uuid_time.split("//")(0), tibco_message_time_helix_uuid_time.split("//")(2), tibco_message_time_helix_uuid_time.split("//")(3)))

    val flight_pln_altn_airport_details_col_df = flight_cost_index_col_df.withColumn("flight_pln_altn_airport_details", flight_pln_altn_airport_details_udf($"AltApt1", $"FirstAltIca", $"AltApt2", $"SecondAltIca", $"AltApt3", $"ThirdAltIca", $"AltApt4", $"FourthAltIca", $"ver", concat(coalesce($"RouOptCri", lit("")), lit("//"), $"msg_time", lit("//"), $"helix_uuid", lit("//"), $"helix_time")))

    //PLN_REG_DETAILS
    val pln_reg_details_udf = udf((pln_reg_number: String, version: String, datetime: String, helix_uuid: String, helix_time: String) => pln_reg_details_case(pln_reg_number, version, datetime, helix_uuid, helix_time))

    val pln_reg_details_col_df = flight_pln_altn_airport_details_col_df.withColumn("pln_reg_details", pln_reg_details_udf($"Tail_no", $"ver", $"msg_time", $"helix_uuid", $"helix_time"))

    pln_reg_details_col_df.registerTempTable("nested_tbl")
    sqlContext.cacheTable("nested_tbl")

    //SELECTING FLIGHT MASTER NESTED COLS
    val lido_flight_master_nested_df = sqlContext.sql("select FliNum as flight_number,dep as dep_iata_station,des as des_iata_station,flt_dt as flight_date,tail_no,AtcCal as flight_call_sign,pln_reg_details,pln_runway_details,flight_cost_index_details,flight_pln_altn_airport_details from nested_tbl")

    //GROUP BY FLIGHT IDENTIFIERS
    lido_flight_master_nested_df.registerTempTable("lido_flight_master_tbl")
    //change
    //val lido_flight_master_groupby_df = sqlContext.sql("select flight_number,flight_date,dep_iata_station,des_iata_station,coalesce(flight_call_sign,'') as flight_call_sign,collect_set(pln_reg_details) as pln_reg_details,collect_set(pln_runway_details) as pln_runway_details,collect_set(flight_cost_index_details) as flight_cost_index_details,collect_set(flight_pln_altn_airport_details) as flight_pln_altn_airport_details from lido_flight_master_tbl group by flight_number,flight_date,dep_iata_station,des_iata_station,coalesce(flight_call_sign,'')")
    //val lido_flight_master_groupby_df = sqlContext.sql("select flight_number,flight_date,dep_iata_station,des_iata_station,tail_no,concat_ws('#',collect_set(coalesce(flight_call_sign,''))) as flight_call_sign,collect_set(pln_reg_details) as pln_reg_details,collect_set(pln_runway_details) as pln_runway_details,collect_set(flight_cost_index_details) as flight_cost_index_details,collect_set(flight_pln_altn_airport_details) as flight_pln_altn_airport_details from lido_flight_master_tbl group by flight_number,flight_date,dep_iata_station,des_iata_station,tail_no")
    val lido_flight_master_groupby_df = sqlContext.sql("select flight_number,flight_date,dep_iata_station,des_iata_station,tail_no,collect_set(pln_reg_details) as pln_reg_details,collect_set(pln_runway_details) as pln_runway_details,collect_set(flight_cost_index_details) as flight_cost_index_details,collect_set(flight_pln_altn_airport_details) as flight_pln_altn_airport_details from lido_flight_master_tbl group by flight_number,flight_date,dep_iata_station,des_iata_station,tail_no")
    //FIND LATEST OF NESTED COLS
    val pln_reg_number_latest_df = sqlContext.sql("select *,RANK() OVER (PARTITION BY FliNum,flt_dt,Dep,Des,tail_no ORDER BY msg_time,ver,helix_uuid,helix_time DESC) as rank from nested_tbl where coalesce(trim(RouOptCri),'') != 'Y'")

    pln_reg_number_latest_df.registerTempTable("tmp_tbl")

    val rank_df = sqlContext.sql("select *,RANK() OVER (PARTITION BY FliNum,flt_dt,Dep,Des,tail_no ORDER BY rank DESC) as rank1 from tmp_tbl")

    rank_df.registerTempTable("tmp_tbl1")

    val latest_df = sqlContext.sql("select FliNum,dep,flt_dt,des,Tail_no,pln_runway_details,AtcCal as flight_call_sign,flight_cost_index_details,flight_pln_altn_airport_details from tmp_tbl1 where rank1 = 1 and coalesce(trim(RouOptCri),'') != 'Y'")

    //JOINING LATEST AND NESTED DATA TO FORM FLIGHT MASTER
    lido_flight_master_groupby_df.registerTempTable("tbl1")
    latest_df.registerTempTable("tbl2")

    val nested_latest_df = sqlContext.sql("select t1.*,t2.flight_call_sign,t2.Tail_no as pln_reg_number_latest,t2.pln_runway_details as pln_runway_details_latest,t2.flight_cost_index_details as flight_cost_index_details_latest,t2.flight_pln_altn_airport_details as flight_pln_altn_airport_details_latest from tbl1 t1 left outer join tbl2 t2 on t1.flight_number = t2.FliNum and to_date(t1.flight_date) = to_date(t2.flt_dt) and t1.dep_iata_station = t2.dep and t1.des_iata_station = t2.des and t1.tail_no = t2.tail_no")

    nested_latest_df
  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path: String = null, hdfs_output_path: String = null, compression: String = null, spark_partitions: Int = 0, write_flag: Boolean = false)

}