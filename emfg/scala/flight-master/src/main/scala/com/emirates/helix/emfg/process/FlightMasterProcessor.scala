/*----------------------------------------------------------------------------
 * Created on  : 20/01/2018
 * Author      : Krishnaraj Rajagopal(s746216)
 * Email       : krishnaraj.rajagopal@emirates.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlightMasterProcessor.scala
 * Description : Class file for processing Flight Master data from CORE, AGS, DMIS, LIDO sources.
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix.emfg.process

import com.emirates.helix.emfg.FlightMasterArguments.Args
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.TimestampType


/**
  * Companion object for FlightMasterProcessor class
  */
object FlightMasterProcessor {

  def apply(args: Args): FlightMasterProcessor = {
    var processor = new FlightMasterProcessor()
    processor.args = args
    processor
  }
}

class FlightMasterProcessor {
  private var args: Args = _

  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("FlightMasterProcessor")

  def generateFlightMaster(core_dmis_fm_df: DataFrame, ags_fm_df: DataFrame, lido_fm_df: DataFrame, numberOfPartition: Integer)(implicit sqlContext: HiveContext): DataFrame = {
    import sqlContext.implicits._

    log.info("Generating Flight Master - Invoked FlightMasterProcessor.generateFlightMaster method")

    core_dmis_fm_df.registerTempTable("core_master_tab")

    ags_fm_df.registerTempTable("ags_master_tab")

    val core_ags_dmis_combined = sqlContext.sql("select * from (select c.*, a.act_runway_details as act_runway_details from core_master_tab c left outer join ags_master_tab a on (substr(c.act_flight_off_datetime_utc,1,10)) = (substr(a.FltDate,1,10))  and c.flight_number = a.FlightNumber and c.actual_reg_number = a.Tail and c.act_dep_iata_station = a.Origin and c.act_arr_iata_station = a.Destination ) A")

    core_ags_dmis_combined.registerTempTable("core_ags_dmis_combined_tab")

    lido_fm_df.registerTempTable("lido_master_tab")

    val flight_master_df = sqlContext.sql("select t1.*,t2.flight_call_sign,t2.pln_runway_details,t2.flight_cost_index_details,t2.flight_pln_altn_airport_details,t2.pln_runway_details_latest,t2.flight_cost_index_details_latest,t2.flight_pln_altn_airport_details_latest from core_ags_dmis_combined_tab t1 left outer join lido_master_tab t2 on t1.flight_number = t2.flight_number and t1.flight_date = t2.flight_date and t1.pln_dep_iata_station = t2.dep_iata_station  and t1.latest_pln_arr_iata_station = t2.des_iata_station and t1.actual_reg_number = t2.pln_reg_number_latest").repartition(numberOfPartition)

    return flight_master_df
  }
}
