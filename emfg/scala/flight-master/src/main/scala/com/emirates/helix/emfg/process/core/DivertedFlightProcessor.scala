/*----------------------------------------------------------------------------
 * Created on  : 01/04/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DivertedFlightProcessor.scala
 * Description : Class file for processing Diverted Flight data.
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix.emfg.process.core

import com.emirates.helix.emfg.FlightMasterArguments._
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Companion object for DivertedFlightProcessor class
  */
object DivertedFlightProcessor {

  def apply(args: Args): DivertedFlightProcessor = {
    var processor = new DivertedFlightProcessor()
    processor.args = args
    processor
  }
}


class DivertedFlightProcessor {

  private var args: Args = _

  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("DivertedFlightProcess")

  /**
    *
    * @param core_input_dataFrame
    * @param sqlContext
    * @return Diverted Flight Data frame
    */
  def processDivertedFlight(core_input_dataFrame: DataFrame)(implicit sqlContext: HiveContext): DataFrame = {

    log.info("Generating Diverted flight data for Flight Master - Invoked DivertedFlightProcessor.processDivertedFlight method")

    core_input_dataFrame.registerTempTable("core_input_dfs_tab")

    val flight_first_rec_df = sqlContext.sql("select flight_number, flight_suffix, flight_date, pln_dep_iata_station,pln_arr_iata_station, airline_code,flight_dep_number from (select flight_number, flight_suffix, flight_date, pln_dep_iata_station,pln_arr_iata_station, airline_code,flight_dep_number, rank() over(partition by  flight_number,flight_suffix, flight_date, pln_dep_iata_station, airline_code,flight_dep_number order by tibco_messageTime asc) as tibco_ranked_rec from (  select tibco_messageTime, flight_number, flight_suffix, flight_date, pln_dep_iata_station,pln_arr_iata_station, airline_code,flight_dep_number,rank() over(partition by  flight_number,flight_suffix, flight_date, pln_dep_iata_station, airline_code,flight_dep_number order by TransDateTime asc) as ranked_rec  from ( select trim(tibco_messageTime) as tibco_messageTime, trim(Audit.TransDateTime) as TransDateTime  , LPAD(trim(FlightId.FltNum),4,'0') as flight_number  , case when trim(FlightId.FltSuffix) is null  or trim(FlightId.FltSuffix) = 'null' then '' else trim(FlightId.FltSuffix) end as flight_suffix, trim(FlightId.FltDate.`_ATTRIBUTE_VALUE`) as flight_date  , trim(FlightId.DepStn) as pln_dep_iata_station  , trim(FlightId.ArrStn) as pln_arr_iata_station , trim(FlightId.CxCd) as airline_code, trim(FlightId.DepNum) as flight_dep_number from core_input_dfs_tab ) a ) b where ranked_rec = 1 ) c where  tibco_ranked_rec = 1 ")
    val flight_first_rec_uniq_df = flight_first_rec_df.dropDuplicates
    flight_first_rec_uniq_df.registerTempTable("flight_first_rec_df_tab")

    val flight_start_rec_df = sqlContext.sql("select flight_number,flight_suffix, flight_date, pln_dep_iata_station,pln_arr_iata_station as latest_pln_arr_iata_station, airline_code,flight_dep_number from (select flight_number, flight_suffix, flight_date, pln_dep_iata_station,pln_arr_iata_station, airline_code,flight_dep_number,rank() over(partition by  flight_number, flight_suffix, flight_date, pln_dep_iata_station, airline_code,flight_dep_number order by TransDateTime desc) as ranked_rec  from (select trim(Audit.TransDateTime) as TransDateTime  , LPAD(trim(FlightId.FltNum),4,'0') as flight_number  , trim(FlightId.FltDate.`_ATTRIBUTE_VALUE`) as flight_date  , case when trim(FlightId.FltSuffix) is null  or trim(FlightId.FltSuffix) = 'null' then '' else trim(FlightId.FltSuffix) end as flight_suffix, trim(FlightId.DepStn) as pln_dep_iata_station  , trim(FlightId.ArrStn) as pln_arr_iata_station , trim(FlightId.CxCd) as airline_code, trim(FlightId.DepNum) as flight_dep_number from core_input_dfs_tab where trim(Events[0].FltStatus) = 'PDEP') A ) B where ranked_rec = 1")
    flight_start_rec_df.dropDuplicates.registerTempTable("flight_start_rec_df_tab")

    val core_final_input_df = sqlContext.sql("select s1.*, case when trim(t1.latest_pln_arr_iata_station) is not null then trim(t1.latest_pln_arr_iata_station) else initial_pln_arr_iata_station end as latest_pln_arr_iata_station from (select t.pln_arr_iata_station  as initial_pln_arr_iata_station, s.*  from core_input_dfs_tab s left outer join flight_first_rec_df_tab t on LPAD(trim(FlightId.FltNum),4,'0') =  flight_number and trim(FlightId.FltDate.`_ATTRIBUTE_VALUE`) = flight_date and trim(FlightId.DepStn) = pln_dep_iata_station  and trim(FlightId.CxCd) = airline_code and trim(FlightId.DepNum) = flight_dep_number and t.flight_suffix = case when trim(FlightId.FltSuffix) is null  or trim(FlightId.FltSuffix) = 'null' then '' else trim(FlightId.FltSuffix) end ) s1 left outer join flight_start_rec_df_tab t1 on LPAD(trim(s1.FlightId.FltNum),4,'0') =  t1.flight_number and trim(s1.FlightId.FltDate.`_ATTRIBUTE_VALUE`) = t1.flight_date and trim(s1.FlightId.DepStn) = t1.pln_dep_iata_station  and trim(s1.FlightId.CxCd) = t1.airline_code and trim(s1.FlightId.DepNum) = t1.flight_dep_number and flight_suffix = case when trim(FlightId.FltSuffix) is null  or trim(FlightId.FltSuffix) = 'null' then '' else trim(FlightId.FltSuffix) end")
    core_final_input_df
  }
}
