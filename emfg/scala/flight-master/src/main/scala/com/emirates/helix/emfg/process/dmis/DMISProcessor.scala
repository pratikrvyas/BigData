/*----------------------------------------------------------
 * Created on  : 12/11/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UDF.scala
 * Description : Scala file to keep all UDF functions
 * ----------------------------------------------------------
 */
package com.emirates.helix.emfg.process.dmis

import com.emirates.helix.emfg.FlightMasterArguments.Args
import com.emirates.helix.udf._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.databricks.spark.avro._
import com.emirates.helix.emfg.common.{FlightMasterUtils, SparkUtils}


/**
  * Companion object for DMISProcessor class
  */
object DMISProcessor {

  def apply(args: Args): DMISProcessor = {
    val processor = new DMISProcessor()
    processor.args = args
    processor
  }
}

class DMISProcessor extends SparkUtils with FlightMasterUtils {
  private var args: Args = _

  /**
    *
    * @param config Ags input data frame
    * @return Processed DMIS data for Flight Master
    */
  def generateDMISDataForFM(config: Args, coreDataFrame: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    val dmis_flight_info_df = sqlContext.read.parquet(config.dmis_input)

    val dmis_baydetails_df = sqlContext.read.parquet(config.dmis_baydetail_input)

    val baychange_lookup = sqlContext.read.avro(config.baychange_lookup)
    val concourse_lookup = sqlContext.read.avro(config.concourse_lookup)
      .select($"TERMINAL_ID", $"CONCOURSE_CODE", $"GATE_NAME").distinct()
    /*============================ JOIN BAY DETAILS FEED ==============================================================*/

    val dmis_baydetails = dmis_baydetails_df.alias("dmis_baydetails_df")
      .join(broadcast(baychange_lookup.alias("baychange_lookup")), dmis_baydetails_df("bchtReasonId") === baychange_lookup("Bay_Change_Reason_Code"), "left_outer")
      .select($"dmis_baydetails_df.airlineCode", $"dmis_baydetails_df.bchtFlightNumber", $"dmis_baydetails_df.bchtStaStd", $"dmis_baydetails_df.aircraftRegistrationNumber",
        $"dmis_baydetails_df.departureStation", $"dmis_baydetails_df.arrivalStation", $"dmis_baydetails_df.bchtOldBayNo", $"dmis_baydetails_df.bchtNewBayNo", $"dmis_baydetails_df.bchtReasonId",
        $"dmis_baydetails_df.arrivalDepartureFlag", $"baychange_lookup.Bay_Change_Reason_Description", $"dmis_baydetails_df.tibco_messageTime").withColumn("flightdate", date_format($"bchtStaStd", "yyyy-MM-dd"))


    val dmis_baydetails_arr = dmis_baydetails.where(lower($"arrivalDepartureFlag") === "a")
      .withColumn("pln_arr_bay_details", DMISUDF.getPlnArrBayDetails($"bchtOldBayNo", $"bchtNewBayNo", $"bchtReasonId", $"Bay_Change_Reason_Description",
        date_format($"tibco_messageTime", "yyyy-MM-dd HH:mm:ss.SSS"))).withColumn("pln_arr_gate_details", DMISUDF.getPlnArrGateDetails())
      .groupBy($"bchtFlightNumber", $"flightdate", $"aircraftRegistrationNumber", $"departureStation", $"arrivalStation")
      .agg(collect_set($"pln_arr_bay_details").as("pln_arr_bay_details"), collect_set($"pln_arr_gate_details").as("pln_arr_gate_details"))

    val dmis_baydetails_dep = dmis_baydetails.where(lower($"arrivalDepartureFlag") === "d")
      .withColumn("pln_dep_bay_details", DMISUDF.getPlnDepBayDetails($"bchtOldBayNo", $"bchtNewBayNo", $"bchtReasonId", $"Bay_Change_Reason_Description",
        date_format($"tibco_messageTime", "yyyy-MM-dd HH:mm:ss.SSS"))).withColumn("pln_dep_gate_details", DMISUDF.getPlnDepGateDetails())
      .groupBy($"bchtFlightNumber", $"flightdate", $"aircraftRegistrationNumber", $"departureStation", $"arrivalStation")
      .agg(collect_set($"pln_dep_bay_details").as("pln_dep_bay_details"), collect_set($"pln_dep_gate_details").as("pln_dep_gate_details"))

    val dmis_baydetails_join1 = coreDataFrame.alias("coreDataFrame").join(dmis_baydetails_arr.alias("dmis_baydetails_arr"), concat(coreDataFrame("airline_code"), coreDataFrame("flight_number")) === dmis_baydetails_arr("bchtFlightNumber")
      && coreDataFrame("actual_reg_number") === dmis_baydetails_arr("aircraftRegistrationNumber") && coreDataFrame("pln_arr_iata_station") === dmis_baydetails_arr("arrivalStation")
      && coreDataFrame("pln_dep_iata_station") === dmis_baydetails_arr("departureStation")
      && date_format(coreDataFrame("initial_sch_arr_datetime_local"), "yyyy-MM-dd") === dmis_baydetails_arr("flightdate"), "left_outer")
      .select($"coreDataFrame.*", $"dmis_baydetails_arr.pln_arr_bay_details", $"dmis_baydetails_arr.pln_arr_gate_details")

    val dmis_baydetails_final = dmis_baydetails_join1.alias("dmis_baydetails_join1").join(dmis_baydetails_dep.alias("dmis_baydetails_dep"),
      concat(dmis_baydetails_join1("airline_code"), dmis_baydetails_join1("flight_number")) === dmis_baydetails_dep("bchtFlightNumber")
        && dmis_baydetails_join1("actual_reg_number") === dmis_baydetails_dep("aircraftRegistrationNumber")
        && ((dmis_baydetails_join1("act_arr_iata_station") === dmis_baydetails_dep("arrivalStation"))
        || (dmis_baydetails_join1("pln_arr_iata_station") === dmis_baydetails_dep("arrivalStation")))
        && dmis_baydetails_join1("pln_dep_iata_station") === dmis_baydetails_dep("departureStation")
        && date_format(dmis_baydetails_join1("initial_sch_dep_datetime_local"), "yyyy-MM-dd") === dmis_baydetails_dep("flightdate"), "left_outer")
      .select($"dmis_baydetails_join1.*", $"dmis_baydetails_dep.pln_dep_bay_details", $"dmis_baydetails_dep.pln_dep_gate_details")



    /*============================ JOIN BAY DETAILS FEED END==============================================================*/

    /*============================ JOIN FLIGHT INFO FEED ==============================================================*/
    val dmis_flightinfo = dmis_flight_info_df.alias("dmis_flight_info_df")
      .join(broadcast(concourse_lookup.alias("concourse_lookup")), dmis_flight_info_df("gateNumber") === concourse_lookup("GATE_NAME"), "left_outer")
      .select($"dmis_flight_info_df.airlineCode", $"dmis_flight_info_df.actualDate", $"dmis_flight_info_df.flightNumber",
        $"dmis_flight_info_df.registrationNumber", $"dmis_flight_info_df.departureStation", $"dmis_flight_info_df.arrivalStation",
        $"dmis_flight_info_df.baggageBeltNumber", $"dmis_flight_info_df.arrivalDepartureFlag", $"dmis_flight_info_df.bayNumber", $"dmis_flight_info_df.gateNumber",
        $"dmis_flight_info_df.handlingTerminal", $"concourse_lookup.CONCOURSE_CODE", $"dmis_flight_info_df.viaRoutes".as("flight_via_route"))

    val dmis_flightinfo_arr = dmis_flightinfo.where(lower($"arrivalDepartureFlag") === "a")
      .withColumn("act_bay_details", DMISUDF.getActBayDetails($"arrivalDepartureFlag", $"bayNumber"))
      .withColumn("act_arr_gate_details", DMISUDF.getActArrGateDetails($"gateNumber", $"CONCOURSE_CODE", $"handlingTerminal"))

    val dmis_flightinfo_dep = dmis_flightinfo.where(lower($"arrivalDepartureFlag") === "d")
      .withColumn("act_bay_details", DMISUDF.getActBayDetails($"arrivalDepartureFlag", $"bayNumber"))
      .withColumn("act_dep_gate_details", DMISUDF.getActDepGateDetails($"gateNumber", $"CONCOURSE_CODE", $"handlingTerminal"))

    val dmis_flightinfo_join1 = dmis_baydetails_final.alias("dmis_baydetails_final").join(dmis_flightinfo_arr.alias("dmis_flightinfo_arr"),
      concat(dmis_baydetails_final("airline_code"), dmis_baydetails_final("flight_number")) === dmis_flightinfo_arr("flightNumber")
        && dmis_baydetails_final("actual_reg_number") === dmis_flightinfo_arr("registrationNumber")
        && dmis_baydetails_final("act_arr_iata_station") === dmis_flightinfo_arr("arrivalStation")
        && dmis_baydetails_final("act_arr_datetime_local") === date_format(dmis_flightinfo_arr("actualDate"), "yyyy-MM-dd HH:mm:ss"), "left_outer")
      .select($"dmis_baydetails_final.*", $"dmis_flightinfo_arr.baggageBeltNumber".as("arr_bag_carousel_number"), $"dmis_flightinfo_arr.act_bay_details",
        $"dmis_flightinfo_arr.act_arr_gate_details", $"dmis_flightinfo_arr.flight_via_route")

    val core_dmis_final = dmis_flightinfo_join1.alias("dmis_flightinfo_join1").join(dmis_flightinfo_dep.alias("dmis_flightinfo_dep"), //Final dataframe after join - This is for FlightInfo feed
      concat(dmis_flightinfo_join1("airline_code"), dmis_flightinfo_join1("flight_number")) === dmis_flightinfo_dep("flightNumber")
        && dmis_flightinfo_join1("actual_reg_number") === dmis_flightinfo_dep("registrationNumber")
        && dmis_flightinfo_join1("act_dep_iata_station") === dmis_flightinfo_dep("departureStation")
        && dmis_flightinfo_join1("act_dep_datetime_local") === date_format(dmis_flightinfo_dep("actualDate"), "yyyy-MM-dd HH:mm:ss"), "left_outer")
      .select($"dmis_flightinfo_join1.*", $"dmis_flightinfo_dep.act_dep_gate_details", $"dmis_flightinfo_dep.act_bay_details".as("_act_bay_details"),
        $"dmis_flightinfo_dep.flight_via_route".as("_flight_via_route"))
      .withColumn("act_bay_details", coalesce($"act_bay_details", $"_act_bay_details"))
      .withColumn("flight_via_route", coalesce($"flight_via_route", $"_flight_via_route"))
      .drop($"_act_bay_details").drop($"_flight_via_route")
    /*============================ JOIN FLIGHT INFO FEED END==============================================================*/
    core_dmis_final

  }

}