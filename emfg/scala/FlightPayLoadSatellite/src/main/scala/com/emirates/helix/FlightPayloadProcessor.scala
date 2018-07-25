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

/*----------------------------------------------------------------------------
 * Modified on  : 23/Apr/2018
 * Author      : Fayaz Shaik (s796466)
 * Email       : fayaz.shaik@dnata.com
 * Description : Updated the join conditions between the sources
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
    def processRejectedRecords(payload_deduped_EK_flight_df: DataFrame,lido_fsum_flight_level_df: DataFrame,
                               ldm_not_joined_df1: DataFrame, flight_master_df: DataFrame,
                               payload_tgt_loadsheet_rejected_location : String,
                               lidofsumflightlevel_tgt_rejected_location : String,
                               ldm_tgt_rejected_location : String
                              )(implicit spark: SparkSession): Unit = {

      payload_deduped_EK_flight_df.createOrReplaceTempView("payload_deduped_df_tab")
      lido_fsum_flight_level_df.createOrReplaceTempView("lido_max_df_tab")
      ldm_not_joined_df1.createOrReplaceTempView("ldm_not_joined_df1_tab")
      flight_master_df.createOrReplaceTempView("flight_master_df_tab")

      // rejected payload records
      val fm_payload_not_joined_df = spark.sql(" select distinct s.FlightNumber, s.FlightDate, s.DeparturePort, s.ArrivalPort," +
        " regexp_extract(FlightNumber, '([A-Za-z]+)([0-9]+)' , 1 ) as airline_code, s.AircraftRegistration " +
        "from payload_deduped_df_tab s Left outer join flight_master_df_tab t" +
        " on s.FlightNumber = t.flight_number and s.FlightDate = (substr(t.sch_dep_datetime_local,1,10))" +
        " and s.DeparturePort = t.pln_dep_iata_station and s.ArrivalPort = t.latest_pln_arr_iata_station " +
        " and s.AircraftRegistration = t.actual_reg_number" +
        " and 'EK' = t.airline_code where t.flight_identifier is null ")
      writeToHDFS(fm_payload_not_joined_df, payload_tgt_loadsheet_rejected_location, "parquet",Some("overwrite"), Some(1))

      // rejected lido records
      val fm_lido_not_joined_df=spark.sql("Select distinct s.FliNum, s.DepAirIat, s.DatOfOriUtc, s.DesAirIat, " +
        "s.RegOrAirFinNum,s.EstimatedLandingWeight_drvd , s.MaxTaxWei, s.MaxLanWei, s.PlaToWei, s.MaxToWei, s.PlnZfw, " +
        "s.MaxZfw, s.FliDupNo, s.tibco_message_time from lido_max_df_tab s Left outer join flight_master_df_tab t " +
        "on s.FliNum=t.flight_number and  s.DepAirIat=t.pln_dep_iata_station  and  " +
        "(substr(s.DatOfOriUtc,1,10))=t.flight_date and s.DesAirIat=t.latest_pln_arr_iata_station and " +
        "s.RegOrAirFinNum=t.actual_reg_number where t.flight_identifier is null")
      writeToHDFS(fm_lido_not_joined_df, lidofsumflightlevel_tgt_rejected_location, "parquet",Some("overwrite"), Some(1))

      // rejected ldm records
      val ldm_not_joined_df = spark.sql("select s.* from ldm_not_joined_df1_tab s Left outer join " +
        "flight_master_df_tab t on trim(flight_no) = concat(t.airline_code,t.flight_number,coalesce(t.flight_suffix,null,'')) " +
        "and translate(translate(aircraft_reg,'-',''),' ','') = actual_reg_number " +
        "and substr(firstleg_final_sch_dep_datetime_local,1,10) = substr(s.flight_date,1,10) " +
        "and trim(dep_station) = pln_dep_iata_station " +
        "and trim(arr_station) = latest_pln_arr_iata_station where flight_identifier is null")
      writeToHDFS(ldm_not_joined_df, ldm_tgt_rejected_location, "parquet",Some("overwrite"), Some(1))
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

      val fm_lido_df=spark.sql("Select distinct t.flight_identifier,s.FliNum, s.DepAirIat, s.DatOfOriUtc, s.DesAirIat, " +
        "s.RegOrAirFinNum,	cast(s.EstimatedLandingWeight_drvd as int) as ELWT, s.MaxTaxWei as MTAXIWT, s.MaxLanWei as MLWT, " +
        "s.PlaToWei as ETOW, s.MaxToWei as MTOW, s.PlnZfw as EZFW, s.MaxZfw as MZFW, s.FliDupNo as lido_version_num, " +
        "s.tibco_message_time as msg_received_date from flight_master_df_tab t Left outer join lido_max_df_tab s " +
        "on s.FliNum=t.flight_number and  s.DepAirIat=t.pln_dep_iata_station  " +
        "and  (substr(s.DatOfOriUtc,1,10)) =t.flight_date " +
        "and s.DesAirIat=t.latest_pln_arr_iata_station " +
        "and s.RegOrAirFinNum=t.actual_reg_number")

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

      payload_fm_df.createOrReplaceTempView("payload_joined_fm_df_tab")
      val split_cab_df = spark.sql("select flight_identifier,flight_number," +
        "flight_date,dep_iata_station,latest_pln_arr_iata_station,act_reg_number,final_version_num,msg_received_Date, " +
        "case when trim(cabin_sec) = '' or trim(cabin_sec) is null or trim(cabin_sec) = 'null' then '0' " +
        "when substr(trim(cabin_sec),1,1) = '0' then trim(substr(trim(cabin_sec),2,6)) else trim(cabin_sec) end as cabin_sec " +
        "from  (" +
        "select flight_identifier,flight_number,flight_date,dep_iata_station,latest_pln_arr_iata_station,act_reg_number," +
        "final_version_num,msg_received_Date,explode(split(trim(cabinsectiontotals),'[.]')) as cabin_sec " +
        "from payload_joined_fm_df_tab) A ")

      split_cab_df.createOrReplaceTempView("split_cab_df_tb")

      val cabins_df = spark.sql("select flight_identifier,flight_number,cabin_sec,flight_date,dep_iata_station," +
        "latest_pln_arr_iata_station,act_reg_number,final_version_num,msg_received_Date," +
        "regexp_extract(cabin_sec,'\\W*[A-Za-z]\\W*',0) as cabin_section_name," +
        "regexp_extract(cabin_sec,'\\W*([0-9]+)\\W*',0) as cabin_section_count " +
        "from split_cab_df_tb where regexp_extract(cabin_sec,'\\W*[A-Za-z]\\W*',0)  !=''")

      val cab_sec_details_udf = udf((cabin_section_name : String, cabin_section_count : String, final_version_num : String, msg_received_Date : String) => cabin_details(cabin_section_name,cabin_section_count,final_version_num,msg_received_Date))

      val flight_act_cabin_details_df= cabins_df.withColumn("flight_act_cabin_details",cab_sec_details_udf(cabins_df("cabin_section_name"),cabins_df("cabin_section_count"),cabins_df("final_version_num"),cabins_df("msg_received_Date")))

      flight_act_cabin_details_df.createOrReplaceTempView("flight_act_cabin_details_df_tb")
      val flight_act_cabin_detail_df =spark.sql("select distinct flight_identifier,flight_number,flight_date," +
        "dep_iata_station,latest_pln_arr_iata_station,act_reg_number," +
        "collect_set(flight_act_cabin_details) as flight_act_cabin_details " +
        "from flight_act_cabin_details_df_tb group by flight_identifier,flight_number," +
        "flight_date,dep_iata_station,latest_pln_arr_iata_station,act_reg_number")


      return flight_act_cabin_detail_df
    }

    /** Method to process complex column flight_act_compartment_details.
      * @return DataFrame
      */
    def getComplexColumn_flight_act_compartment_details(payload_fm_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

      payload_fm_df.createOrReplaceTempView("payload_deduped_df_tab")

      val split_LoadInCompartmen_df = spark.sql("select flight_identifier,flight_number,flight_date,dep_iata_station," +
        "latest_pln_arr_iata_station,act_reg_number,final_version_num,msg_received_Date," +
        "explode(split(trim(LoadInCompartment),' ')) as LoadInCompartment from payload_deduped_df_tab")

      split_LoadInCompartmen_df.createOrReplaceTempView("split_LoadInCompartmen_df_tb")

      val loadInCompartmen_df = spark.sql("select flight_identifier,flight_number,flight_date,dep_iata_station," +
        "latest_pln_arr_iata_station,act_reg_number,final_version_num,msg_received_Date as msg_received_Date," +
        "split(LoadInCompartment,'/')[0] as compartment_number,split(LoadInCompartment,'/')[1] as compartment_weight " +
        "from split_LoadInCompartmen_df_tb")

      val loadInCompartmen_udf = udf((compartment_number : String, compartment_weight : String, final_version_num : String, msg_received_Date : String) => compartment_details(compartment_number,compartment_weight,final_version_num,msg_received_Date))

      val flight_act_compartment_details_df= loadInCompartmen_df.withColumn("flight_act_compartment_details",loadInCompartmen_udf(loadInCompartmen_df("compartment_number"),loadInCompartmen_df("compartment_weight"),loadInCompartmen_df("final_version_num"),loadInCompartmen_df("msg_received_Date")))

      flight_act_compartment_details_df.createOrReplaceTempView("flight_act_compartment_details_df_tb")
      val flight_act_compartment_detail_df =spark.sql("select distinct flight_identifier,flight_number," +
        "flight_date,dep_iata_station,latest_pln_arr_iata_station,act_reg_number," +
        "collect_set(flight_act_compartment_details) as flight_act_compartment_details " +
        "from flight_act_compartment_details_df_tb " +
        "group by flight_identifier,flight_number,flight_date,dep_iata_station,latest_pln_arr_iata_station,act_reg_number")

      return flight_act_compartment_detail_df

    }

    /** Method to generate Flight_Payload.
      * @return DataFrame
      */
    def processFlightPayload(payload_src_loadsheet_dedupe_location:String,
                             lidofsumflightlevel_src_dedupe_location:String,
                             ldm_src_dedupe_location:String,
                             flightmaster_current_location:String,
                             payload_tgt_loadsheet_rejected_location:String,
                             lidofsumflightlevel_tgt_rejected_location:String,
                             ldm_tgt_rejected_location:String,
                             coalesce_value : Int)
                            (implicit spark: SparkSession): DataFrame = {

      // load payload loadsheet
      val payload_deduped_all_df = readValueFromHDFS(payload_src_loadsheet_dedupe_location, "parquet", spark)
      payload_deduped_all_df.createOrReplaceTempView("payload_deduped_all_df_tab")
      val payload_deduped_EK_flight_df=spark.sql("Select * from payload_deduped_all_df_tab where FlightNumber like 'EK%'")
      payload_deduped_EK_flight_df.createOrReplaceTempView("payload_deduped_df_tab")

      // load lido fsum
      val lidoGrp = Window.partitionBy("FliNum","DepAirIat","DatOfOriUtc","DesAirIat","RegOrAirFinNum")
      val lido_fsum_flight_level_df = readValueFromHDFS(lidofsumflightlevel_src_dedupe_location, "parquet", spark)
      val lido_grp_df = lido_fsum_flight_level_df.withColumn("MAX_TIMESTAMP", max("tibco_message_time").over(lidoGrp))
      val lido_max_df = lido_grp_df.filter(lido_grp_df("MAX_TIMESTAMP")===lido_grp_df("tibco_message_time")).drop("MAX_TIMESTAMP")

      // load ldm
      val ldm_dedupe_src_df = readValueFromHDFS(ldm_src_dedupe_location, "parquet", spark)
      ldm_dedupe_src_df.createOrReplaceTempView("ldm_dedupe_src_df_tab")
      val ldm_dedupe_df = spark.sql("select * from ldm_dedupe_src_df_tab where flight_no like 'EK%'")
      ldm_dedupe_df.createOrReplaceTempView("ldm_dedupe_df_tab")

      // load flight master
      val flight_master_df = readValueFromHDFS(flightmaster_current_location, "parquet", spark)
      flight_master_df.createOrReplaceTempView("flight_master_df_tab")


      val payload_joined_fm_df =spark.sql(" select distinct t.flight_identifier, t.flight_number  as flight_number," +
        "t.actual_reg_number as act_reg_number,t.flight_date as flight_date, t.pln_dep_iata_station as dep_iata_station," +
        "t.latest_pln_arr_iata_station as latest_pln_arr_iata_station,t.airline_code as airline_code," +
        "s.LoadsheetMessage as flight_act_loadsheet_message,s.cabinsectiontotals,s.LoadInCompartment," +
        "s.ZeroFuelWeight as ZFW,s.MaximumZeroFuelWeight as MZFW,s.TakeOffWeight as TOW,s.MaximumTakeOffWeight as RTOW," +
        "s.LandingWeight as LWT,s.MaximumLandingWeight as MLWT,s.DryOperatingWeightValue as DOW,s.DryOperatingIndexvalue as DOI," +
        "s.LoadedIndexZFWvalue as LIZFW,s.AerodynamicChordZFWValue as MACLIZFW,s.LoadedIndexTOWvalue as LITOW," +
        "s.AerodynamicChordTOWvalue as MACTOW, TotalWeightOfAllCompartmentsValue as total_compartment_weight," +
        "s.underloadvalue as underload,s.finalloadsheeteditionnumberidentifier as final_version_num," +
        " s.TibcoTimeStamp as msg_received_Date,case when totalpassengersonboardincludinginfants is null or  " +
        "totalpassengersonboardincludinginfants  = 'null' or trim(totalpassengersonboardincludinginfants)='' " +
        "then totalpassengersonboardincludinginfants else cast(   totalpassengersonboardincludinginfants as int) end as total_pax_loadsheet_count," +
        "case when split(trim(totalpassengernumbersperclassvalue),'/')[0] is null or  " +
        "split(trim(totalpassengernumbersperclassvalue),'/')[0]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[0]='' then " +
        "'' else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[0] as int) " +
        "end as f_pax_loadsheet_count,case when split(trim(totalpassengernumbersperclassvalue),'/')[1] is null or  " +
        "split(trim(totalpassengernumbersperclassvalue),'/')[1]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[1]='' then " +
        "'' else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[1] as int) " +
        "end as j_pax_loadsheet_count , case when split(trim(totalpassengernumbersperclassvalue),'/')[2] is null or  " +
        "split(trim(totalpassengernumbersperclassvalue),'/')[2]  = 'null' or split(trim(totalpassengernumbersperclassvalue),'/')[2]='' then " +
        "'' else cast(   split(trim(totalpassengernumbersperclassvalue),'/')[2] as int) " +
        "end as y_pax_loadsheet_count," +
        "cast(( totalpassengersonboardincludinginfants - (regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[0],'null','0' ) " +
        "+regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[1],'null','0' ) " +
        "+regexp_replace(split(trim(totalpassengernumbersperclassvalue),'/')[2],'null','0' ))) as int)as infant_count, " +
        "TotalPassengerWeightValue as PWT from payload_deduped_df_tab s  Left outer join  flight_master_df_tab t " +
        "on trim(s.FlightNumber) = concat(trim(t.airline_code) , trim(t.flight_number) , trim(coalesce(t.flight_suffix,''))) " +
        "and s.FlightDate = (substr(t.sch_dep_datetime_local,1,10)) " +
        "and s.DeparturePort = t.pln_dep_iata_station " +
        "and s.ArrivalPort = t.latest_pln_arr_iata_station" +
        " and s.AircraftRegistration = t.actual_reg_number " +
        "and 'EK' = t.airline_code " +
        "where t.flight_identifier is not null ")

      val ldm_joined_df1 = spark.sql("select *, " +
        "(coalesce(cast(cargo_weight as int),null,0) + coalesce(cast(total_excess_bag_weight as int),null,0) + " +
        "coalesce(cast(mail_weight as int),null,0) + coalesce(cast(pax_weight as int),null,0) + " +
        "coalesce(cast(baggage_weight as int),null,0) + coalesce(cast(miscl_weight as int),null,0)) as total_payload " +
        "from (select s.tibco_messageTime as msg_received_date, t.flight_identifier, t.flight_number  as flight_number," +
        "t.actual_reg_number as act_reg_number,t.flight_date as flight_date, t.pln_dep_iata_station as dep_iata_station," +
        "t.latest_pln_arr_iata_station as latest_pln_arr_iata_station,t.airline_code as airline_code, " +
        "s.cargo_weight as cargo_weight, '0000' as total_excess_bag_weight, s.mail_weight as mail_weight, " +
        "s.transit_weight as transit_weight, s.baggage_weight as baggage_weight, s.miscl_weight as miscl_weight,p.TotalPassengerWeightValue as pax_weight " +
        "from ldm_dedupe_df_tab s Left outer join flight_master_df_tab t on " +
        "trim(flight_no) = concat(t.airline_code,t.flight_number,coalesce(t.flight_suffix,null,'')) " +
        "and translate(translate(aircraft_reg,'-',''),' ','') = actual_reg_number " +
        "and substr(sch_dep_datetime_local,1,10) = substr(s.flight_date,1,10) " +
        "and trim(dep_station) = pln_dep_iata_station " +
        "and trim(arr_station) = latest_pln_arr_iata_station " +
        "Left outer join payload_deduped_df_tab p  " +
        "on trim(p.FlightNumber) = concat(trim(t.airline_code) , trim(t.flight_number) , trim(coalesce(t.flight_suffix,''))) " +
        "and p.FlightDate = (substr(t.sch_dep_datetime_local,1,10)) " +
        "and p.DeparturePort = t.pln_dep_iata_station " +
        "and p.ArrivalPort = t.latest_pln_arr_iata_station " +
        "and p.AircraftRegistration = t.actual_reg_number " +
        "and 'EK' = t.airline_code " +
        "where t.flight_identifier is not null ) A ")

      val ldm_not_joined_df1 = spark.sql("select s.* from ldm_dedupe_df_tab s Left outer join flight_master_df_tab t " +
        "on trim(flight_no) = concat(t.airline_code,t.flight_number,coalesce(t.flight_suffix,null,'')) " +
        "and translate(translate(aircraft_reg,'-',''),' ','') = actual_reg_number " +
        "and substr(sch_dep_datetime_local,1,10) = substr(s.flight_date,1,10) " +
        "and trim(dep_station) = pln_dep_iata_station " +
        "and trim(arr_station) = latest_pln_arr_iata_station where flight_identifier is null")

      ldm_not_joined_df1.createOrReplaceTempView("ldm_not_joined_df1_tab")

      val ldm_joined_df2 = spark.sql("select *, (coalesce(cast(cargo_weight as int),null,0) + " +
        "coalesce(cast(total_excess_bag_weight as int),null,0) + coalesce(cast(mail_weight as int),null,0) + " +
        "coalesce(cast(pax_weight as int),null,0) + coalesce(cast(baggage_weight as int),null,0) + " +
        "coalesce(cast(miscl_weight as int),null,0)) as total_payload from " +
        "(select s.tibco_messageTime as msg_received_date, t.flight_identifier, t.flight_number  as flight_number," +
        "t.actual_reg_number as act_reg_number,t.flight_date as flight_date, t.pln_dep_iata_station as dep_iata_station," +
        "t.latest_pln_arr_iata_station as latest_pln_arr_iata_station,t.airline_code as airline_code, " +
        "s.cargo_weight as cargo_weight, '0000' as total_excess_bag_weight, s.mail_weight as mail_weight, " +
        "s.transit_weight as transit_weight, s.baggage_weight as baggage_weight, s.miscl_weight as miscl_weight,p.TotalPassengerWeightValue as pax_weight " +
        "from ldm_not_joined_df1_tab s Left outer join flight_master_df_tab t on " +
        "trim(flight_no) = concat(t.airline_code,t.flight_number,coalesce(t.flight_suffix,null,'')) " +
        "and translate(translate(aircraft_reg,'-',''),' ','') = actual_reg_number " +
        "and substr(firstleg_final_sch_dep_datetime_local,1,10) = substr(s.flight_date,1,10) " +
        "and trim(dep_station) = pln_dep_iata_station " +
        "and trim(arr_station) = latest_pln_arr_iata_station  " +
        "Left outer join payload_deduped_df_tab p  " +
        "on trim(p.FlightNumber) = concat(trim(t.airline_code) , trim(t.flight_number) , trim(coalesce(t.flight_suffix,''))) " +
        "and p.FlightDate = (substr(t.sch_dep_datetime_local,1,10)) " +
        "and p.DeparturePort = t.pln_dep_iata_station " +
        "and p.ArrivalPort = t.latest_pln_arr_iata_station " +
        "and p.AircraftRegistration = t.actual_reg_number " +
        "and 'EK' = t.airline_code " +
        "where flight_identifier is not null ) A ")

      val ldm_joined_df = ldm_joined_df1.union(ldm_joined_df2)

      processRejectedRecords(payload_deduped_EK_flight_df, lido_max_df, ldm_not_joined_df1, flight_master_df,
        payload_tgt_loadsheet_rejected_location,lidofsumflightlevel_tgt_rejected_location, ldm_tgt_rejected_location)

      val complexCol_flight_pax_payload_details=getComplexColumn_flight_pax_payload_details(payload_joined_fm_df)
      val complexCol_flight_act_cabin_details_df=getComplexColumn_flight_act_cabin_details(payload_joined_fm_df)
      val complexCol_flight_act_compartment_details_df=getComplexColumn_flight_act_compartment_details(payload_joined_fm_df)
      val complexCol_flight_act_payload_details=getComplexColumn_flight_act_payload_details(payload_joined_fm_df)
      val complexCol_flight_pln_payload_details_df=getComplexColumn_flight_pln_payload_details(lido_max_df,flight_master_df)
      val complexCol_getComplexColumn_flight_deadload_details=getComplexColumn_flight_deadload_details(ldm_joined_df)

      // All complex columns from LoadSheet
      complexCol_flight_pax_payload_details.createOrReplaceTempView("flt_pax_payload_details_tb")
      complexCol_flight_act_cabin_details_df.createOrReplaceTempView("flt_act_cabin_details_df_tb")
      complexCol_flight_act_compartment_details_df.createOrReplaceTempView("flt_act_compartment_detail_df_tb")
      complexCol_flight_act_payload_details.createOrReplaceTempView("flight_act_payload_detail_df_tb")
      // Deadload - LDM
      complexCol_getComplexColumn_flight_deadload_details.createOrReplaceTempView("flight_bag_payload_details_df_tb")
      // complex column from Lido
      complexCol_flight_pln_payload_details_df.createOrReplaceTempView("flt_pln_payload_details_df_tb")

      // Building loadsheet join
      val flight_payload_loadsheet_df=spark.sql("Select a.flight_identifier,a.flight_number,a.act_reg_number," +
        "a.flight_date,a.dep_iata_station,a.latest_pln_arr_iata_station,a.airline_code,d.flight_act_payload_details," +
        "a.flight_pax_payload_details,b.flight_act_cabin_details,c.flight_act_compartment_details, " +
        "case when a.flight_act_loadsheet_message is null or  a.flight_act_loadsheet_message  = 'null' then '' " +
        "else a.flight_act_loadsheet_message end as flight_act_loadsheet_message  from flt_pax_payload_details_tb a " +
        "left outer join flt_act_cabin_details_df_tb b on b.flight_identifier=a.flight_identifier " +
        "left outer join flt_act_compartment_detail_df_tb c on c.flight_identifier=a.flight_identifier " +
        "left outer join flight_act_payload_detail_df_tb d on d.flight_identifier=a.flight_identifier")
      flight_payload_loadsheet_df.createOrReplaceTempView("flight_payload_loadsheet_df_tab")

      // Building loadsheet + ldm join
      val flight_loadsheet_ldm_df = spark.sql(" select case when (ls_flight_identifier = ldm_flight_identifier) " +
        "then ls_flight_identifier when ls_flight_identifier is not null then ls_flight_identifier " +
        "when ldm_flight_identifier is not null then ldm_flight_identifier end as flight_identifier," +
        " flight_number,act_reg_number,flight_date,dep_iata_station, latest_pln_arr_iata_station," +
        "airline_code,flight_act_payload_details,flight_pax_payload_details,flight_act_cabin_details," +
        "flight_act_compartment_details, flight_act_loadsheet_message, flight_deadload_details " +
        "from (select ls.flight_identifier as ls_flight_identifier,ls.flight_number,ls.act_reg_number," +
        "ls.flight_date,ls.dep_iata_station, ls.latest_pln_arr_iata_station,ls.airline_code," +
        "ls.flight_act_payload_details,ls.flight_pax_payload_details,ls.flight_act_cabin_details," +
        "ls.flight_act_compartment_details, ls.flight_act_loadsheet_message, " +
        "ldm.flight_identifier as ldm_flight_identifier, ldm.flight_deadload_details " +
        "from flight_payload_loadsheet_df_tab ls full outer join flight_bag_payload_details_df_tb ldm " +
        "on ls.flight_identifier = ldm.flight_identifier) A ")
      flight_loadsheet_ldm_df.createOrReplaceTempView("flight_loadsheet_ldm_df_tab")

      // Building loadsheet + ldm + lido join
      val flight_loadsheet_ldm_lido_df = spark.sql(" select case " +
        "when lsldm_flight_identifier = lido_flight_identifier then lsldm_flight_identifier when lsldm_flight_identifier is not null " +
        "then lsldm_flight_identifier when lido_flight_identifier is not null then lido_flight_identifier end as flight_identifier, " +
        "flight_number,act_reg_number,flight_date,dep_iata_station, latest_pln_arr_iata_station as arr_iata_station," +
        "airline_code,flight_act_payload_details,flight_pax_payload_details,flight_act_cabin_details," +
        "flight_act_compartment_details, flight_act_loadsheet_message, flight_deadload_details," +
        "flight_pln_payload_details from (select lsldm.flight_identifier as lsldm_flight_identifier , " +
        "lsldm.flight_number,lsldm.act_reg_number,lsldm.flight_date,lsldm.dep_iata_station, " +
        "lsldm.latest_pln_arr_iata_station,lsldm.airline_code,lsldm.flight_act_payload_details," +
        "lsldm.flight_pax_payload_details,lsldm.flight_act_cabin_details,lsldm.flight_act_compartment_details, " +
        "lsldm.flight_act_loadsheet_message, lsldm.flight_deadload_details," +
        " lido.flight_identifier as lido_flight_identifier, lido.flight_pln_payload_details " +
        "from flight_loadsheet_ldm_df_tab lsldm full outer join flt_pln_payload_details_df_tb lido " +
        "on lsldm.flight_identifier = lido.flight_identifier) B")

      return flight_loadsheet_ldm_lido_df
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


      if (args.data_load_mode.toUpperCase().equals("HISTORY")) {


        val hist_sql = "select * from flight_payload_tab where to_date(flight_date) < " +
          "add_months(to_date('" + max_flight_date + "')," + -1 * lookback_months + ") "

        val output_df_last_hist = spark.sql(hist_sql).coalesce(coalesce_value)
        writeToHDFS(output_df_last_hist, payload_tgt_history_location, "parquet", Some("overwrite"), Some(coalesce_value))


        val incr_sql = "select * from flight_payload_tab where to_date(flight_date) >= " +
          "add_months(to_date('" + max_flight_date + "')," + -1 * lookback_months + ")"


        val output_df_last_incr = spark.sql(incr_sql).coalesce(coalesce_value)
        writeToHDFS(output_df_last_incr, payload_tgt_current_location, "parquet",Some("overwrite"), Some(coalesce_value))
      }

else
      {

      writeToHDFS(out_df, payload_tgt_current_location, "parquet",Some("overwrite"), Some(coalesce_value))
      }

    }
  }
}
