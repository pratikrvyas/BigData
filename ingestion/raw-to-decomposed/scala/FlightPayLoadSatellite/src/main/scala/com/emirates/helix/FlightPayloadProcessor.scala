/*----------------------------------------------------------------------------
 * Created on  : 13/Mar/2018
 * Author      : Pratik Vyas (s795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlightPayloadProcessor.scala
 * Description : Secondary class file for processing Flight Payloads
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.emiraets.helix.flightpayloadarguments.FlightPayloadArgs._
import com.emirates.helix.util.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.emirates.helix.util.FlightPayloadUtils



/**
  * Companion object for FlightPayloadProcessor class
  */
object FlightPayloadProcessor {

  /**
    * FlightPayloadProcessor instance creation
    * @param args user parameter instance
    * @return FlightPayloadProcessor instance
    */
  def apply(args:  Args): FlightPayloadProcessor = {
    var processor = new FlightPayloadProcessor()
    processor.args = args
    processor
  }

  case class compartment_details(compartment_number : String, compartment_weight : String, final_version_num : String, msg_received_date : String)
  case class cabin_details(cabin_section_name : String, cabin_section_count : String, final_version_num : String, msg_received_date : String)

  /**
    * FlightPayloadProcessor class will read
    * Payload loadsheet , Lido_fsum_flight_level deduped data and populate Flight Payload.
    */
  class FlightPayloadProcessor extends SparkUtils with FlightPayloadUtils {
     var args: Args = _


    /** Method to save rejected records from payload deduped and lido_fsum_flight_level deduped.
      * This method also writes the rejected records to target location
      */
    def processRejectedRecords(payload_deduped_EK_flight_df: DataFrame,lido_fsum_flight_level_df: DataFrame,flight_master_df: DataFrame,payload_tgt_loadsheet_rejected_location : String,lidofsumflightlevel_tgt_rejected_location : String
                              )(implicit spark: SparkSession): Unit = {

      payload_deduped_EK_flight_df.createOrReplaceTempView("payload_deduped_df_tab")
      lido_fsum_flight_level_df.createOrReplaceTempView("lido_max_df_tab")
      flight_master_df.createOrReplaceTempView("flight_master_df_tab")

      // rejected payload records
      val fm_payload_not_joined_df = spark.sql(" select distinct s.FlightNumber, s.FlightDate, s.DeparturePort, s.ArrivalPort," +
        " regexp_extract(FlightNumber, '([A-Za-z]+)([0-9]+)' , 1 ) as airline_code, s.AircraftRegistration from payload_deduped_df_tab s Left outer join flight_master_df_tab t" +
        " on s.FlightNumber = t.flight_number and s.FlightDate = (substr(t.sch_dep_datetime_local,1,10))" +
        " and s.DeparturePort = t.pln_dep_iata_station and s.ArrivalPort = t.latest_pln_arr_iata_station and s.AircraftRegistration = t.actual_reg_number" +
        " and 'EK' = t.airline_code where t.flight_identifier is null ")
      writeToHDFS(fm_payload_not_joined_df, payload_tgt_loadsheet_rejected_location, "parquet",Some("overwrite"), Some(1))

      // rejected lido records
      val fm_lido_not_joined_df=spark.sql("Select s.FliNum, s.DepAirIat, s.DatOfOriUtc, s.DesAirIat, s.RegOrAirFinNum,s.EstimatedLandingWeight_drvd , s.MaxTaxWei, s.MaxLanWei, s.PlaToWei, s.MaxToWei, s.PlnZfw, s.MaxZfw, s.FliDupNo, s.tibco_message_time from lido_max_df_tab s Left outer join flight_master_df_tab t on s.FliNum=t.flight_number and  s.DepAirIat=t.pln_dep_iata_station  and  (substr(s.DatOfOriUtc,1,10))=t.flight_date and s.DesAirIat=t.latest_pln_arr_iata_station and s.RegOrAirFinNum=t.actual_reg_number where t.flight_identifier is null")
      writeToHDFS(fm_lido_not_joined_df, lidofsumflightlevel_tgt_rejected_location, "parquet",Some("overwrite"), Some(1))

    }


    /** Method to process complex column flight_act_payload_details.
      * @return DataFrame
      */
    def getComplexColumn_flight_act_payload_details(payload_fm_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

      val payload_with_flight_act_payload_details_df = toMap(payload_fm_df,COLUMNLIST_flight_act_payload_details, "flight_act_payload_details")
      return 	payload_with_flight_act_payload_details_df
    }

    /** Method to process complex column flight_pln_payload_details.
      * @return DataFrame
      */
    def getComplexColumn_flight_pln_payload_details(lido_fsum_flight_level_df: DataFrame,flight_master_df: DataFrame)(implicit spark: SparkSession): DataFrame = {


      lido_fsum_flight_level_df.createOrReplaceTempView("lido_max_df_tab")
      flight_master_df.createOrReplaceTempView("flight_master_df_tab")

      val fm_lido_df=spark.sql("Select s.FliNum, s.DepAirIat, s.DatOfOriUtc, s.DesAirIat, s.RegOrAirFinNum,	s.EstimatedLandingWeight_drvd as ELWT, s.MaxTaxWei as MAX_TAXI_WT, s.MaxLanWei as MLWT, s.PlaToWei as ETOW, s.MaxToWei as MTOW, s.PlnZfw as EZFW, s.MaxZfw as MZFW, s.FliDupNo as lido_version_num, s.tibco_message_time as msg_received_date from lido_max_df_tab s Left outer join flight_master_df_tab t on s.FliNum=t.flight_number and  s.DepAirIat=t.pln_dep_iata_station  and  (substr(s.DatOfOriUtc,1,10)) =t.flight_date and s.DesAirIat=t.latest_pln_arr_iata_station and s.RegOrAirFinNum=t.actual_reg_number")
      val flight_pln_payload_details_df = toMap(fm_lido_df,COLUMNLIST_flight_pln_payload_details, "flight_pln_payload_details")

      return flight_pln_payload_details_df

    }

    /** Method to process complex column flight_pax_payload_details.
      * @return DataFrame
      */
    def getComplexColumn_flight_pax_payload_details(payload_fm_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

      val payload_with_flight_pax_payload_details_df = toMap(payload_fm_df,COLUMNLIST_flight_pax_payload_details, "flight_pax_payload_details")
      return payload_with_flight_pax_payload_details_df
    }


    /** Method to process complex column flight_deadload_details.
      * @return DataFrame
      */
    def getComplexColumn_flight_deadload_details(payload_fm_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

      val flight_deadload_details_df = toMap(payload_fm_df,COLUMNLIST_flight_deadload_details, "flight_deadload_details")
      return flight_deadload_details_df
    }

    /** Method to process complex column flight_act_cabin_details.
      * @return DataFrame
      */
    def getComplexColumn_flight_act_cabin_details(payload_fm_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

      payload_fm_df.createOrReplaceTempView("payload_deduped_df_tab")
      val split_cab_df = spark.sql("select FlightNumber,FlightDate,DeparturePort,ArrivalPort,AircraftRegistration,FinalLoadsheetEditionNumberIdentifier as final_version_num,TibcoTimeStamp,explode(split(cabinsectiontotals,'[.]')) as cabin_sec from payload_deduped_df_tab")
      split_cab_df.createOrReplaceTempView("split_cab_df_tb")

      val cabins_df = spark.sql("select FlightNumber,cabin_sec,FlightDate,DeparturePort,ArrivalPort,AircraftRegistration,final_version_num,TibcoTimeStamp as msg_received_Date,regexp_extract(cabin_sec,'\\W*[A-Za-z]\\W*',0) as cabin_section_name,regexp_extract(cabin_sec,'\\W*([0-9]+)\\W*',0) as cabin_section_count from split_cab_df_tb where regexp_extract(cabin_sec,'\\W*[A-Za-z]\\W*',0)  !=''")
     // val cab_sec_details_udf = udf((cabin_section_name : String, cabin_section_count : String, final_version_num : String, msg_received_Date : String) => (cabin_section_name,cabin_section_count,final_version_num,msg_received_Date))

      val cab_sec_details_udf = udf((cabin_section_name : String, cabin_section_count : String, final_version_num : String, msg_received_Date : String) => cabin_details(cabin_section_name,cabin_section_count,final_version_num,msg_received_Date))

      val flight_act_cabin_details_df= cabins_df.withColumn("flight_act_cabin_details",cab_sec_details_udf(cabins_df("cabin_section_name"),cabins_df("cabin_section_count"),cabins_df("final_version_num"),cabins_df("msg_received_Date")))

      flight_act_cabin_details_df.createOrReplaceTempView("flight_act_cabin_details_df_tb")
      val flight_act_cabin_detail_df =spark.sql("select FlightNumber,FlightDate,DeparturePort,ArrivalPort,AircraftRegistration,collect_set(flight_act_cabin_details) as flight_act_cabin_details from flight_act_cabin_details_df_tb group by FlightNumber,FlightDate,DeparturePort,ArrivalPort,AircraftRegistration")


      return flight_act_cabin_detail_df
    }

    /** Method to process complex column flight_act_compartment_details.
      * @return DataFrame
      */
    def getComplexColumn_flight_act_compartment_details(payload_fm_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

      payload_fm_df.createOrReplaceTempView("payload_deduped_df_tab")

      val split_LoadInCompartmen_df = spark.sql("select FlightNumber,FlightDate,DeparturePort,ArrivalPort,AircraftRegistration,FinalLoadsheetEditionNumberIdentifier as final_version_num,TibcoTimeStamp,explode(split(LoadInCompartment,' ')) as LoadInCompartment from payload_deduped_df_tab")
      split_LoadInCompartmen_df.createOrReplaceTempView("split_LoadInCompartmen_df_tb")

      val loadInCompartmen_df = spark.sql("select FlightNumber,FlightDate,DeparturePort,ArrivalPort,AircraftRegistration,final_version_num,TibcoTimeStamp as msg_received_Date,split(LoadInCompartment,'/')[0] as compartment_number,split(LoadInCompartment,'/')[1] as compartment_weight from split_LoadInCompartmen_df_tb")

      //val loadInCompartmen_udf = udf((compartment_number : String, compartment_weight : String, final_version_num : String, msg_received_Date : String) => (compartment_number,compartment_weight,final_version_num,msg_received_Date))

      val loadInCompartmen_udf = udf((compartment_number : String, compartment_weight : String, final_version_num : String, msg_received_Date : String) => compartment_details(compartment_number,compartment_weight,final_version_num,msg_received_Date))

      val flight_act_compartment_details_df= loadInCompartmen_df.withColumn("flight_act_compartment_details",loadInCompartmen_udf(loadInCompartmen_df("compartment_number"),loadInCompartmen_df("compartment_weight"),loadInCompartmen_df("final_version_num"),loadInCompartmen_df("msg_received_Date")))

      flight_act_compartment_details_df.createOrReplaceTempView("flight_act_compartment_details_df_tb")
      val flight_act_compartment_detail_df =spark.sql("select FlightNumber,FlightDate,DeparturePort,ArrivalPort,AircraftRegistration,collect_set(flight_act_compartment_details) as flight_act_compartment_details from flight_act_compartment_details_df_tb group by FlightNumber,FlightDate,DeparturePort,ArrivalPort,AircraftRegistration")

      return flight_act_compartment_detail_df

    }

    /** Method to generate Flight_Payload.
      * @return DataFrame
      */
    def processFlightPayload(payload_src_loadsheet_dedupe_location:String,lidofsumflightlevel_src_dedupe_location:String,flightmaster_current_location:String,payload_tgt_loadsheet_rejected_location:String,lidofsumflightlevel_tgt_rejected_location:String,coalesce_value : Int)
                            (implicit spark: SparkSession): DataFrame = {

      // load payload
      val payload_deduped_all_df = readValueFromHDFS(payload_src_loadsheet_dedupe_location, "parquet", spark)
      payload_deduped_all_df.createOrReplaceTempView("payload_deduped_all_df_tab")
      val payload_deduped_EK_flight_df=spark.sql("Select * from payload_deduped_all_df_tab where FlightNumber like 'EK%'")
      payload_deduped_EK_flight_df.createOrReplaceTempView("payload_deduped_df_tab")

      // load lido fsum
      val lidoGrp = Window.partitionBy("FliNum","DepAirIat","DatOfOriUtc","DesAirIat","RegOrAirFinNum")
      val lido_fsum_flight_level_df = readValueFromHDFS(lidofsumflightlevel_src_dedupe_location, "parquet", spark)
      val lido_grp_df = lido_fsum_flight_level_df.withColumn("MAX_TIMESTAMP", max("tibco_message_time").over(lidoGrp))
      val lido_max_df = lido_grp_df.filter(lido_grp_df("MAX_TIMESTAMP")===lido_grp_df("tibco_message_time")).drop("MAX_TIMESTAMP")
      // load flight master
      val flight_master_df = readValueFromHDFS(flightmaster_current_location, "parquet", spark)
      flight_master_df.createOrReplaceTempView("flight_master_df_tab")

      processRejectedRecords(payload_deduped_EK_flight_df,lido_max_df,flight_master_df,payload_tgt_loadsheet_rejected_location,lidofsumflightlevel_tgt_rejected_location)

      //join payload with fm , process complex columns

     //val payload_joined_fm_df =spark.sql(" select t.flight_identifier,regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 )  as flight_number,s.AircraftRegistration as act_reg_number,s.FlightDate as flight_date,s.DeparturePort as dep_iata_station,s.ArrivalPort as arr_iata_station,regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 1 ) as airline_code,s.LoadsheetMessage as flight_act_loadsheet_message,s.ZeroFuelWeight as ZFW,s.MaximumZeroFuelWeight as MZFW,s.TakeOffWeight as TOW,s.MaximumTakeOffWeight as MTOW,s.LandingWeight as LWT,s.MaximumLandingWeight as MLWT,s.DryOperatingWeightValue as DOW,s.DryOperatingIndexvalue as DOI,s.LoadedIndexZFWvalue as LIZFW,s.AerodynamicChordZFWValue as MACLIZFW,s.LoadedIndexTOWvalue as LITOW,s.AerodynamicChordTOWvalue as MACTOW, TotalWeightOfAllCompartmentsValue as total_compartment_weight,s.underloadvalue as underload,'null' as total_payload,s.finalloadsheeteditionnumberidentifier as final_version_num, s.TibcoTimeStamp as msg_received_Date,case when totalpassengersonboardincludinginfants is null or  totalpassengersonboardincludinginfants  = 'null' or trim(totalpassengersonboardincludinginfants)='' then totalpassengersonboardincludinginfants else cast(   totalpassengersonboardincludinginfants as int) end as total_pax_loadsheet_count,case when split(trim(totalpassengernumbersperclassvalue),'/')[0] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[0]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[0]='' then split(trim(totalpassengernumbersperclassvalue),'/')[0] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[0] as int) end as f_pax_loadsheet_count,case when split(trim(totalpassengernumbersperclassvalue),'/')[1] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[1]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[1]='' then split(trim(totalpassengernumbersperclassvalue),'/')[1] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[1] as int) end as j_pax_loadsheet_count , case when split(trim(totalpassengernumbersperclassvalue),'/')[2] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[2]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[2]='' then split(trim(totalpassengernumbersperclassvalue),'/')[2] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[2] as int) end as y_pax_loadsheet_count,cast(( totalpassengersonboardincludinginfants - (regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[0],'null','0' ) +regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[1],'null','0' ) +regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[2],'null','0' ))) as int)as infant_count, TotalPassengerWeightValue as PWT, 'null' as baggage_weight,'null' as total_excess_bag_weight, 'null' as cargo_weight,'null' as mail_weight,'null' as transit_weight,'null' as miscl_weight from payload_deduped_df_tab s Left outer join flight_master_df_tab t on regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ) = t.flight_number and s.FlightDate = (substr(t.act_flight_off_datetime_local,1,10)) and s.DeparturePort = t.pln_dep_iata_station and s.ArrivalPort = t.pln_arr_iata_station and s.AircraftRegistration = t.actual_reg_number and 'EK' = t.airline_code where t.flight_identifier is not null ")
     //val payload_joined_fm_df =spark.sql(" select t.flight_identifier,regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 )  as flight_number,s.AircraftRegistration as act_reg_number,t.flight_date as flight_date,s.DeparturePort as dep_iata_station,s.ArrivalPort as arr_iata_station,regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 1 ) as airline_code,s.LoadsheetMessage as flight_act_loadsheet_message,s.ZeroFuelWeight as ZFW,s.MaximumZeroFuelWeight as MZFW,s.TakeOffWeight as TOW,s.MaximumTakeOffWeight as MTOW,s.LandingWeight as LWT,s.MaximumLandingWeight as MLWT,s.DryOperatingWeightValue as DOW,s.DryOperatingIndexvalue as DOI,s.LoadedIndexZFWvalue as LIZFW,s.AerodynamicChordZFWValue as MACLIZFW,s.LoadedIndexTOWvalue as LITOW,s.AerodynamicChordTOWvalue as MACTOW, TotalWeightOfAllCompartmentsValue as total_compartment_weight,s.underloadvalue as underload,'null' as total_payload,s.finalloadsheeteditionnumberidentifier as final_version_num, s.TibcoTimeStamp as msg_received_Date,case when totalpassengersonboardincludinginfants is null or  totalpassengersonboardincludinginfants  = 'null' or trim(totalpassengersonboardincludinginfants)='' then totalpassengersonboardincludinginfants else cast(   totalpassengersonboardincludinginfants as int) end as total_pax_loadsheet_count,case when split(trim(totalpassengernumbersperclassvalue),'/')[0] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[0]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[0]='' then split(trim(totalpassengernumbersperclassvalue),'/')[0] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[0] as int) end as f_pax_loadsheet_count,case when split(trim(totalpassengernumbersperclassvalue),'/')[1] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[1]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[1]='' then split(trim(totalpassengernumbersperclassvalue),'/')[1] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[1] as int) end as j_pax_loadsheet_count , case when split(trim(totalpassengernumbersperclassvalue),'/')[2] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[2]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[2]='' then split(trim(totalpassengernumbersperclassvalue),'/')[2] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[2] as int) end as y_pax_loadsheet_count,cast(( totalpassengersonboardincludinginfants - (regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[0],'null','0' ) +regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[1],'null','0' ) +regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[2],'null','0' ))) as int)as infant_count, TotalPassengerWeightValue as PWT, 'null' as baggage_weight,'null' as total_excess_bag_weight, 'null' as cargo_weight,'null' as mail_weight,'null' as transit_weight,'null' as miscl_weight from payload_deduped_df_tab s Left outer join flight_master_df_tab t on regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ) = t.flight_number and s.FlightDate = (substr(t.sch_dep_datetime_local,1,10)) and s.DeparturePort = t.pln_dep_iata_station and s.ArrivalPort = t.latest_pln_arr_iata_station and s.AircraftRegistration = t.actual_reg_number and 'EK' = t.airline_code where t.flight_identifier is not null ")

      val payload_joined_fm_df =spark.sql(" select t.flight_identifier,regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 )  as flight_number,s.AircraftRegistration as act_reg_number,t.flight_date as flight_date,s.DeparturePort as dep_iata_station,t.latest_pln_arr_iata_station as latest_pln_arr_iata_station,regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 1 ) as airline_code,s.LoadsheetMessage as flight_act_loadsheet_message,s.ZeroFuelWeight as ZFW,s.MaximumZeroFuelWeight as MZFW,s.TakeOffWeight as TOW,s.MaximumTakeOffWeight as MTOW,s.LandingWeight as LWT,s.MaximumLandingWeight as MLWT,s.DryOperatingWeightValue as DOW,s.DryOperatingIndexvalue as DOI,s.LoadedIndexZFWvalue as LIZFW,s.AerodynamicChordZFWValue as MACLIZFW,s.LoadedIndexTOWvalue as LITOW,s.AerodynamicChordTOWvalue as MACTOW, TotalWeightOfAllCompartmentsValue as total_compartment_weight,s.underloadvalue as underload,'null' as total_payload,s.finalloadsheeteditionnumberidentifier as final_version_num, s.TibcoTimeStamp as msg_received_Date,case when totalpassengersonboardincludinginfants is null or  totalpassengersonboardincludinginfants  = 'null' or trim(totalpassengersonboardincludinginfants)='' then totalpassengersonboardincludinginfants else cast(   totalpassengersonboardincludinginfants as int) end as total_pax_loadsheet_count,case when split(trim(totalpassengernumbersperclassvalue),'/')[0] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[0]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[0]='' then split(trim(totalpassengernumbersperclassvalue),'/')[0] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[0] as int) end as f_pax_loadsheet_count,case when split(trim(totalpassengernumbersperclassvalue),'/')[1] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[1]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[1]='' then split(trim(totalpassengernumbersperclassvalue),'/')[1] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[1] as int) end as j_pax_loadsheet_count , case when split(trim(totalpassengernumbersperclassvalue),'/')[2] is null or  split(trim(totalpassengernumbersperclassvalue),'/')[2]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[2]='' then split(trim(totalpassengernumbersperclassvalue),'/')[2] else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[2] as int) end as y_pax_loadsheet_count,cast(( totalpassengersonboardincludinginfants - (regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[0],'null','0' ) +regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[1],'null','0' ) +regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[2],'null','0' ))) as int)as infant_count, TotalPassengerWeightValue as PWT, 'null' as baggage_weight,'null' as total_excess_bag_weight, 'null' as cargo_weight,'null' as mail_weight,'null' as transit_weight,'null' as miscl_weight from payload_deduped_df_tab s Left outer join flight_master_df_tab t on regexp_extract(s.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ) = t.flight_number and s.FlightDate = (substr(t.sch_dep_datetime_local,1,10)) and s.DeparturePort = t.pln_dep_iata_station and s.ArrivalPort = t.latest_pln_arr_iata_station and s.AircraftRegistration = t.actual_reg_number and 'EK' = t.airline_code where t.flight_identifier is not null ")
      val complexCol_flight_pax_payload_details=getComplexColumn_flight_pax_payload_details(payload_joined_fm_df)
      val complexCol_flight_pln_payload_details_df=getComplexColumn_flight_pln_payload_details(lido_max_df,flight_master_df)
      val complexCol_flight_act_cabin_details_df=getComplexColumn_flight_act_cabin_details(payload_deduped_EK_flight_df)
      val complexCol_flight_act_compartment_details_df=getComplexColumn_flight_act_compartment_details(payload_deduped_EK_flight_df)
      val complexCol_flight_act_payload_details=getComplexColumn_flight_act_payload_details(payload_joined_fm_df)
      val complexCol_getComplexColumn_flight_deadload_details=getComplexColumn_flight_deadload_details(payload_joined_fm_df)


      // join all complex columns , generate flight_payload
      complexCol_flight_pax_payload_details.createOrReplaceTempView("payload_fm_lido_tb")
      complexCol_flight_pln_payload_details_df.createOrReplaceTempView("flt_pln_payload_details_df_tb")
      complexCol_flight_act_cabin_details_df.createOrReplaceTempView("flt_act_cabin_details_df_tb")
      complexCol_flight_act_compartment_details_df.createOrReplaceTempView("flt_act_compartment_detail_df_tb")
      complexCol_flight_act_payload_details.createOrReplaceTempView("flight_act_payload_detail_df_tb")
      complexCol_getComplexColumn_flight_deadload_details.createOrReplaceTempView("flight_bag_payload_details_df_tb")

     // val flight_payload_df=spark.sql("Select t.flight_identifier,t.flight_number,t.act_reg_number,t.flight_date,t.dep_iata_station,t.arr_iata_station,t.airline_code,s.flight_pln_payload_details,q.flight_act_payload_details,'null' as flight_deadload_details,t.flight_pax_payload_details,h.flight_act_cabin_details,r.flight_act_compartment_details,t.flight_act_loadsheet_message from payload_fm_lido_tb t left outer join flt_pln_payload_details_df_tb s on s.FliNum=t.flight_number and  s.DepAirIat=t.dep_iata_station  and  t.flight_date=(substr(s.DatOfOriUtc,1,10)) and s.DesAirIat=t.arr_iata_station and s.RegOrAirFinNum=t.act_reg_number left outer join flt_act_cabin_details_df_tb h on regexp_extract(h.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 )=t.flight_number and  h.DeparturePort=t.dep_iata_station  and  h.FlightDate=t.flight_date and h.ArrivalPort=t.arr_iata_station and h.AircraftRegistration=t.act_reg_number left outer join flt_act_compartment_detail_df_tb r on regexp_extract(r.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ) =t.flight_number and  r.DeparturePort=t.dep_iata_station  and  r.FlightDate=t.flight_date and r.ArrivalPort=t.arr_iata_station and r.AircraftRegistration=t.act_reg_number left outer join flight_act_payload_detail_df_tb q on q.flight_number=t.flight_number and q.dep_iata_station=t.dep_iata_station and q.flight_date=t.flight_date and q.arr_iata_station=t.arr_iata_station and q.act_reg_number=t.act_reg_number")
      //val flight_payload_df=spark.sql("Select t.flight_identifier,t.flight_number,t.act_reg_number,t.flight_date,t.dep_iata_station,t.arr_iata_station,t.airline_code,s.flight_pln_payload_details,q.flight_act_payload_details,x.flight_deadload_details,t.flight_pax_payload_details,h.flight_act_cabin_details,r.flight_act_compartment_details,case when t.flight_act_loadsheet_message is null or  t.flight_act_loadsheet_message  = 'null' then '' else t.flight_act_loadsheet_message end as flight_act_loadsheet_message from payload_fm_lido_tb t left outer join flt_pln_payload_details_df_tb s on s.FliNum=t.flight_number and  s.DepAirIat=t.dep_iata_station  and  t.flight_date=(substr(s.DatOfOriUtc,1,10)) and s.DesAirIat=t.arr_iata_station and s.RegOrAirFinNum=t.act_reg_number left outer join flt_act_cabin_details_df_tb h on regexp_extract(h.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 )=t.flight_number and  h.DeparturePort=t.dep_iata_station  and  h.FlightDate=t.flight_date and h.ArrivalPort=t.arr_iata_station and h.AircraftRegistration=t.act_reg_number left outer join flt_act_compartment_detail_df_tb r on regexp_extract(r.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ) =t.flight_number and  r.DeparturePort=t.dep_iata_station  and  r.FlightDate=t.flight_date and r.ArrivalPort=t.arr_iata_station and r.AircraftRegistration=t.act_reg_number left outer join flight_act_payload_detail_df_tb q on q.flight_number=t.flight_number and q.dep_iata_station=t.dep_iata_station and q.flight_date=t.flight_date and q.arr_iata_station=t.arr_iata_station and q.act_reg_number=t.act_reg_number left outer join flight_bag_payload_details_df_tb x on x.flight_number=t.flight_number and x.dep_iata_station=t.dep_iata_station and x.flight_date=t.flight_date and x.arr_iata_station=t.arr_iata_station and x.act_reg_number=t.act_reg_number")

      val flight_payload_df=spark.sql("Select t.flight_identifier,t.flight_number,t.act_reg_number,t.flight_date,t.dep_iata_station,t.latest_pln_arr_iata_station,t.airline_code,s.flight_pln_payload_details,q.flight_act_payload_details,x.flight_deadload_details,t.flight_pax_payload_details,h.flight_act_cabin_details,r.flight_act_compartment_details, case when t.flight_act_loadsheet_message is null or  t.flight_act_loadsheet_message  = 'null' then '' else t.flight_act_loadsheet_message end as flight_act_loadsheet_message from payload_fm_lido_tb t left outer join flt_pln_payload_details_df_tb s on s.FliNum=t.flight_number and  s.DepAirIat=t.dep_iata_station  and  t.flight_date=(substr(s.DatOfOriUtc,1,10)) and s.DesAirIat=t.latest_pln_arr_iata_station and s.RegOrAirFinNum=t.act_reg_number left outer join flt_act_cabin_details_df_tb h on regexp_extract(h.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 )=t.flight_number and  h.DeparturePort=t.dep_iata_station  and  h.FlightDate=t.flight_date and h.ArrivalPort=t.latest_pln_arr_iata_station and h.AircraftRegistration=t.act_reg_number left outer join flt_act_compartment_detail_df_tb r on regexp_extract(r.FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ) =t.flight_number and  r.DeparturePort=t.dep_iata_station  and  r.FlightDate=t.flight_date and r.ArrivalPort=t.latest_pln_arr_iata_station and r.AircraftRegistration=t.act_reg_number left outer join flight_act_payload_detail_df_tb q on q.flight_number=t.flight_number and q.dep_iata_station=t.dep_iata_station and q.flight_date=t.flight_date and q.latest_pln_arr_iata_station=t.latest_pln_arr_iata_station and q.act_reg_number=t.act_reg_number left outer join flight_bag_payload_details_df_tb x on x.flight_number=t.flight_number and x.dep_iata_station=t.dep_iata_station and x.flight_date=t.flight_date and x.latest_pln_arr_iata_station=t.latest_pln_arr_iata_station and x.act_reg_number=t.act_reg_number")

      return flight_payload_df
    }

    /** Method to write the  data to HDFS
      * @param out_df Dataframe to be written to HDFS
      */
    def writeFlightPayload(out_df: DataFrame,payload_tgt_history_location : String,
                           payload_tgt_current_location : String,
                           coalesce_value : Int, lookback_months: Int)
                          (implicit spark: SparkSession): Unit = {

      out_df.createOrReplaceTempView("flight_payload_tab")

      val max_flight_date = spark.sql("select max(flight_date) from flight_payload_tab").first().mkString

      val incr_sql = "select * from flight_payload_tab where to_date(flight_date) >= " +
        "add_months(to_date('" + max_flight_date + "')," + -1 * lookback_months + ")"
      val hist_sql = "select * from flight_payload_tab where to_date(flight_date) < " +
        "add_months(to_date('" + max_flight_date + "')," + -1 * lookback_months + ") "

      val output_df_last_incr = spark.sql(incr_sql).coalesce(coalesce_value)
      val output_df_last_hist = spark.sql(hist_sql).coalesce(coalesce_value)

      writeToHDFS(output_df_last_hist, payload_tgt_history_location, "parquet",Some("overwrite"), Some(coalesce_value))

      writeToHDFS(output_df_last_incr, payload_tgt_current_location, "parquet",Some("overwrite"), Some(coalesce_value))
    }
  }
}
