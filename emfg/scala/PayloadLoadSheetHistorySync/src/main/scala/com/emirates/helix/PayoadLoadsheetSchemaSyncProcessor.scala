/*----------------------------------------------------------------------------
 * Created on  : 01/Mar/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PayoadLoadsheetSchemaSyncProcessor.scala
 * Description : Processing Payoad Loadsheet  data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix


import org.apache.spark.sql.{DataFrame, SQLContext}

import com.databricks.spark.avro._
import com.emirates.helix.udf.UDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.emirates.helix.util._



object PayoadLoadsheetSchemaSyncProcessor {


  /**
    * PayoadLoadsheetSchemaSyncProcessor instance creation
    * @param args user parameter instance
    * @return Processor instance
    */
  def apply(args:  PayoadLoadsheetSchemaSyncArguments.Args): PayoadLoadsheetSchemaSyncProcessor = {
    var processor = new PayoadLoadsheetSchemaSyncProcessor()
    processor.args = args
    processor
  }


  /**
    * PayoadLoadsheetSchemaSyncProcessor class with methods to
    * read history data of AGS Refline and perform schema sync.
    */
  class PayoadLoadsheetSchemaSyncProcessor extends SparkUtils {
    var args: PayoadLoadsheetSchemaSyncArguments.Args = _


    /** Method to read the AGS Refline historic data from HDFS
      *
      * @return DataFrame
      */


    def read_clc_flight_dtls_History(inputPath: String)(implicit spark: SparkSession): DataFrame = {


      //import org.apache.spark.sql.types.TimestampType

      val df_clc_flight_dtls_load = spark.read.avro(inputPath)
      //val df_clc_flight_dtls=df_clc_flight_dtls_load.withColumn("TibcoTimeStamp",df_clc_flight_dtls_load("CREATED").cast(TimestampType))
      df_clc_flight_dtls_load.createOrReplaceTempView("clc_flight_dtls")


    //val df_clc_flight_dtls_filtered = spark.sql("SELECT MFL_ID as MFL_ID,CREATED, from_unixtime(unix_timestamp(CREATED,'yyyy-MM-dd HH:mm:ss')) as TibcoTimeStamp,concat(trim(coalesce(OPT_COMPANY,'')),lpad(FLT_NO,4,0),trim(coalesce(OPT_SUFFIX,''))) as FlightNumber,substring(trim(org_lcl_date),9,2) as Dayofmovement,ORIGIN as DeparturePort,DESTINATION as ArrivalPort,date_format(to_date(LCL_DATE),'yyyy-MM-dd')  as FlightDate,EVENT_TYPE , 'LOADSHEET' as LoadsheetIdentifier, 'FINAL' as FinalLoadsheetIdentifier, VERSION_NUMBER as FinalLoadsheetEditionNumberIdentifier, HELIX_UUID, HELIX_TIMESTAMP FROM clc_flight_dtls where EVENT_TYPE='FLT'")

    //val df_clc_flight_dtls_filtered = spark.sql("SELECT MFL_ID as MFL_ID,CREATED, from_unixtime(unix_timestamp(CREATED,'dd/MM/yyyy HH:mm:ss')) as TibcoTimeStamp,concat(trim(coalesce(OPT_COMPANY,'')),lpad(FLT_NO,4,0),trim(coalesce(OPT_SUFFIX,''))) as FlightNumber,substring(trim(org_lcl_date),9,2) as Dayofmovement,ORIGIN as DeparturePort,DESTINATION as ArrivalPort,substr(LCL_DATE,1,10) as FlightDate ,EVENT_TYPE , 'LOADSHEET' as LoadsheetIdentifier, 'FINAL' as FinalLoadsheetIdentifier, VERSION_NUMBER as FinalLoadsheetEditionNumberIdentifier, HELIX_UUID, HELIX_TIMESTAMP FROM clc_flight_dtls where EVENT_TYPE='FLT'")
    val df_clc_flight_dtls_filtered = spark.sql("SELECT MFL_ID as MFL_ID,DESTINATION,CREATED, substr(CREATED,1,19) as TibcoTimeStamp,concat(trim(coalesce(OPT_COMPANY,'')),lpad(FLT_NO,4,0),trim(coalesce(OPT_SUFFIX,''))) as FlightNumber,substring(trim(ORG_UTC_DATE),9,2)  as Dayofmovement,ORIGIN as DeparturePort,DESTINATION as ArrivalPort,substr(LCL_DATE,1,10) as FlightDate ,EVENT_TYPE , 'LOADSHEET' as LoadsheetIdentifier, 'FINAL' as FinalLoadsheetIdentifier, VERSION_NUMBER as FinalLoadsheetEditionNumberIdentifier, HELIX_UUID, HELIX_TIMESTAMP FROM clc_flight_dtls where EVENT_TYPE='FLT'")


    return df_clc_flight_dtls_filtered
  }

    def read_clc_cabin_sec_info_History(inputPath:String)(implicit spark: SparkSession): DataFrame = {

      val df_clc_cabin_sec_info= spark.read.avro(inputPath)
      df_clc_cabin_sec_info.createOrReplaceTempView("clc_cabin_sec_info")
      val df_clc_cabin_sec_info_filtered = spark.sql("select MFL_ID,concat_ws('.',collect_list(CONCAT( CAB_SEC_NAME,TOTAL_T))) as CabinSectionTotals from clc_cabin_sec_info group by MFL_ID")

      return df_clc_cabin_sec_info_filtered

    }

    def read_clc_crew_figures_History(inputPath:String)(implicit spark: SparkSession): DataFrame = {

      val df_clc_crew_figures= spark.read.avro(inputPath)
      df_clc_crew_figures.createOrReplaceTempView("clc_crew_figures")
      val df_clc_crew_figures_filtered = spark.sql("SELECT MFL_ID as MFL_ID, 'CREW' as CrewIdentifier,CONCAT ( TOT_TCW,'/',TOT_CCW) as CrewCompliment FROM clc_crew_figures")

      return df_clc_crew_figures_filtered

    }

    def read_clc_leg_info_History(inputPath:String)(implicit spark: SparkSession): DataFrame = {

      val df_clc_leg_info= spark.read.avro(inputPath)
      df_clc_leg_info.createOrReplaceTempView("clc_leg_info")
      val df_clc_leg_info_filtered = spark.sql("SELECT MFL_ID as MFL_ID,REGNO as AircraftRegistration FROM clc_leg_info")

      return df_clc_leg_info_filtered

    }

    def read_clc_pax_bag_total_History(inputPath:String)(implicit spark: SparkSession): DataFrame = {


      val df_clc_pax_bag_total= spark.read.avro(inputPath)
      df_clc_pax_bag_total.createOrReplaceTempView("clc_pax_bag_total")
      val df_clc_pax_bag_total_filtered = spark.sql("SELECT MFL_ID , 'PW' as TotalPassengerWeightIdentifier, TOTAL_PAX_WT as TotalPassengerWeightValue FROM clc_pax_bag_total where PAX_TYPE='CUA'")

      return df_clc_pax_bag_total_filtered

    }

    def read_clc_pax_class_figures_History(inputPath:String)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val df_clc_pax_figures= spark.read.avro(inputPath)
      //df_clc_pax_figures.groupBy("MFL_ID").pivot("CLASS_DESIGNATOR").agg(sum($"TOTAL_T")).createOrReplaceTempView("clc_pax_figures")
      df_clc_pax_figures.groupBy("MFL_ID").pivot("CLASS_DESIGNATOR").agg((sum(coalesce($"TOTAL_A",lit(0))) + sum(coalesce($"TOTAL_C",lit(0))) + sum(coalesce($"TOTAL_F",lit(0))) + sum(coalesce($"TOTAL_M",lit(0))) )).createOrReplaceTempView("clc_pax_figures")
      val df_clc_pax_figures_filtered=spark.sql("SELECT MFL_ID as MFL_ID,'TTL' as TotalPassengerIdentifier,'PAX' as PassengerIdentifier ,  CONCAT (coalesce(F,'null'),'/', coalesce(J,'null') ,'/', coalesce(Y,'null')) as TotalPassengerNumbersPerClassValue FROM clc_pax_figures")

      return df_clc_pax_figures_filtered

    }

    def read_clc_pax_tot_figures_History(inputPath:String)(implicit spark: SparkSession): DataFrame = {


      val df_clc_pax_figures= spark.read.avro(inputPath)
      df_clc_pax_figures.createOrReplaceTempView("clc_pax_figures_tot")
      //val df_clc_pax_figures_tot_filtered=spark.sql("SELECT MFL_ID as MFL_ID,sum(coalesce(TOTAL_T,0)) as  TotalPassengersOnBoardIncludingInfants from clc_pax_figures_tot group by MFL_ID")

      val df_clc_pax_figures_tot_filtered=spark.sql("SELECT MFL_ID as MFL_ID, (sum(coalesce(TOTAL_A,0)) + sum(coalesce(TOTAL_C,0)) + sum(coalesce(TOTAL_F,0)) + sum(coalesce(TOTAL_I,0)) + sum(coalesce(TOTAL_M,0)) ) as  TotalPassengersOnBoardIncludingInfants from clc_pax_figures_tot group by MFL_ID")


      return df_clc_pax_figures_tot_filtered

    }

    def read_macs_load_sheet_data_History(inputPath:String)(implicit spark: SparkSession): DataFrame = {

      val df_macs_load_sheet_data= spark.read.avro(inputPath)
      df_macs_load_sheet_data.createOrReplaceTempView("macs_load_sheet_data")
      val df_macs_load_sheet_data_filtered = spark.sql("SELECT MFL_ID as MFL_ID, 'ZFW' as ZeroFuelWeightIdentifier, MACS_ACT_ZERO_FUEL_WGT as  ZeroFuelWeight,'MAX' as ZeroFuelWeightMaximumIdentifier,MACS_MAX_ZERO_FUEL_WGT as MaximumZeroFuelWeight,'TOF' as TakeOffFuelIdentifier,MACS_ACT_TAKE_OFF_FUEL_WGT as TakeOffFuel,'TOW' as TakeOffWeightIdentifier,MACS_ACT_TAKE_OFF_WGT as TakeOffWeight,MACS_ACT_LANDING_WGT as LandingWeight,MACS_DRY_OPERATING_WGT as DryOperatingWeightValue,MACS_DRY_OPERATING_INDEX as DryOperatingIndexvalue,MACS_LADEN_INDEX_ZFW as LoadedIndexZFWvalue,MACS_MAX_TAKE_OFF_WGT as MaximumTakeOffWeight,'TIF' as TripFuelIdentifier,MACS_TRIP_FUEL_WGT as TripFuel,'LAW' as LandingWeightIdentifier,'MAX' as LandingWeightMaximumIdentifier,MACS_MAX_LANDING_WGT as MaximumLandingWeight,'--' as LimitingFactorIdentifier,'BALANCE AND SEATING' as BalanceandSeatingIdentifier,'DOW' as DryOperatingWeightLabel,'DOI' as DOILabel,'LIZFW' as LIZFWLabel,'MACZFW' as AerodynamicChordZFWLabel,MACS_PERCENTAGE_MAC_ZFW as AerodynamicChordZFWValue,'LITOW' as LITOWLabel,MACS_LADEN_INDEX_TOW as LoadedIndexTOWvalue,MACS_PERCENTAGE_MAC_TOW as AerodynamicChordTOWvalue,macs_under_load_value as UnderloadValue from macs_load_sheet_data")

      return df_macs_load_sheet_data_filtered

    }

    def read_macs_cargo_dstr_cmpt_History(inputPath:String)(implicit spark: SparkSession): DataFrame = {


      val df_macs_cargo_dstr_cmpt= spark.read.avro(inputPath)
      df_macs_cargo_dstr_cmpt.createOrReplaceTempView("macs_cargo_dstr_cmpt")
      val df_macs_cargo_dstr_cmpt_filtered=spark.sql("select MFL_ID,DESTINATION," +
        "regexp_replace(regexp_replace(trim(CARGO_DSTR_CMPT_WISE), '  ', ' '),'  ', ' ') as CARGO_DSTR_CMPT_WISE,'T' as TotalWeightInCompartmentIdentifier," +
        "'UNDERLOAD' as UnderloadIdentifier from macs_cargo_dstr_cmpt")
      val df_macs_cargo_dstr_cmpt_all= df_macs_cargo_dstr_cmpt_filtered
        .withColumn("LoadInCompartment",UDF.udf_compartmentLoad(df_macs_cargo_dstr_cmpt_filtered("CARGO_DSTR_CMPT_WISE")))
        .withColumn("TotalWeightOfAllCompartmentsValue",UDF.udf_compartmentLoadTot(df_macs_cargo_dstr_cmpt_filtered("CARGO_DSTR_CMPT_WISE")))



      return df_macs_cargo_dstr_cmpt_all

    }

    def joinPayloadHistory(df_clc_flight_dtls_filtered: DataFrame,df_clc_cabin_sec_info_filtered: DataFrame,df_clc_crew_figures_filtered: DataFrame,df_clc_leg_info_filtered: DataFrame,df_clc_pax_bag_total_filtered: DataFrame,df_clc_pax_figures_filtered: DataFrame,df_clc_pax_figures_tot_filtered: DataFrame,df_macs_load_sheet_data_filtered: DataFrame,df_macs_cargo_dstr_cmpt_filtered: DataFrame): DataFrame = {

      val joinType="leftouter"

      /*
      val df_payload_history=df_clc_flight_dtls_filtered.
        join(df_clc_cabin_sec_info_filtered,(df_clc_flight_dtls_filtered.col("MFL_ID") === df_clc_cabin_sec_info_filtered.col("MFL_ID")),joinType).
        join(df_clc_crew_figures_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_crew_figures_filtered("MFL_ID"),joinType).
        join(df_clc_leg_info_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_leg_info_filtered("MFL_ID"),joinType).
        join(df_clc_pax_bag_total_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_pax_bag_total_filtered("MFL_ID"),joinType).
        join(df_clc_pax_figures_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_pax_figures_filtered("MFL_ID"),joinType).
        join(df_clc_pax_figures_tot_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_pax_figures_tot_filtered("MFL_ID"),joinType).
        join(df_macs_load_sheet_data_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_macs_load_sheet_data_filtered("MFL_ID"),joinType).
        join(df_macs_cargo_dstr_cmpt_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_macs_cargo_dstr_cmpt_filtered("MFL_ID"),joinType)
*/

      val df_payload_history=df_clc_flight_dtls_filtered.
        join(df_clc_cabin_sec_info_filtered,(df_clc_flight_dtls_filtered.col("MFL_ID") === df_clc_cabin_sec_info_filtered.col("MFL_ID")),joinType).
        join(df_clc_crew_figures_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_crew_figures_filtered("MFL_ID"),joinType).
        join(df_clc_leg_info_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_leg_info_filtered("MFL_ID"),joinType).
        join(df_clc_pax_bag_total_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_pax_bag_total_filtered("MFL_ID"),joinType).
        join(df_clc_pax_figures_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_pax_figures_filtered("MFL_ID"),joinType).
        join(df_clc_pax_figures_tot_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_clc_pax_figures_tot_filtered("MFL_ID"),joinType).
        join(df_macs_load_sheet_data_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_macs_load_sheet_data_filtered("MFL_ID"),joinType).
        join(df_macs_cargo_dstr_cmpt_filtered,df_clc_flight_dtls_filtered("MFL_ID") === df_macs_cargo_dstr_cmpt_filtered("MFL_ID") && df_clc_flight_dtls_filtered("DESTINATION") === df_macs_cargo_dstr_cmpt_filtered("DESTINATION") ,joinType)


      df_payload_history.count()



      return df_payload_history
    }


    /**  Method to sync the AGS Refline historical data
      *
      *  @return DataFrame
      */
    //def syncSchema()(implicit spark: SparkSession): DataFrame = {
    def syncSchema(
                    history_src_clc_flight_dtls:String,
                    history_src_clc_cabin_sec_info:String,
                    history_src_clc_crew_figures: String,
                    history_src_clc_leg_info: String,
                    history_src_clc_pax_bag_total: String,
                    history_src_clc_pax_figures: String,
                    history_src_macs_load_sheet_data: String,
                    history_src_macs_cargo_dstr_cmpt: String


                  )(implicit spark: SparkSession): DataFrame = {
      val df_history = joinPayloadHistory(read_clc_flight_dtls_History(history_src_clc_flight_dtls),
                                        read_clc_cabin_sec_info_History(history_src_clc_cabin_sec_info),
                                        read_clc_crew_figures_History(history_src_clc_crew_figures),
                                        read_clc_leg_info_History(history_src_clc_leg_info),
                                        read_clc_pax_bag_total_History(history_src_clc_pax_bag_total),
                                        read_clc_pax_class_figures_History(history_src_clc_pax_figures),
                                        read_clc_pax_tot_figures_History(history_src_clc_pax_figures),
                                        read_macs_load_sheet_data_History(history_src_macs_load_sheet_data),
                                        read_macs_cargo_dstr_cmpt_History(history_src_macs_cargo_dstr_cmpt))



      //val ds=df_history.as[com.emirates.helix.model.Model.Payload]

      df_history.createOrReplaceTempView("payloadHistory")

      //val df_historySync =spark.sql("SELECT FlightNumber,Dayofmovement,DeparturePort,ArrivalPort,AircraftRegistration,FlightDate,TotalPassengersOnBoardIncludingInfants,CrewIdentifier,CrewCompliment,PassengerIdentifier,TotalPassengerNumbersPerClassValue,TotalPassengerIdentifier,ZeroFuelWeightIdentifier,ZeroFuelWeight,ZeroFuelWeightMaximumIdentifier,MaximumZeroFuelWeight,TakeOffFuelIdentifier,TakeOffFuel,TakeOffWeightIdentifier,TakeOffWeight,LandingWeight,DryOperatingWeightValue,DryOperatingIndexvalue,LoadedIndexZFWvalue,TotalPassengerWeightIdentifier,TotalPassengerWeightValue,LoadsheetIdentifier,FinalLoadsheetIdentifier,FinalLoadsheetEditionNumberIdentifier,MaximumTakeOffWeight,TripFuelIdentifier,TripFuel,LandingWeightIdentifier,LandingWeightMaximumIdentifier,MaximumLandingWeight,LimitingFactorIdentifier,BalanceandSeatingIdentifier,DryOperatingWeightLabel,DOILabel,LIZFWLabel,AerodynamicChordZFWLabel,AerodynamicChordZFWValue,LITOWLabel,LoadedIndexTOWvalue,AerodynamicChordTOWvalue,CabinSectionTotals,UnderloadValue,LoadInCompartment,TotalWeightOfAllCompartmentsValue,'null' as  LMCIdentifier,'null' as  DestinationIdentifier,'null' as  SpecificationIdentifier,'null' as  PlusMinusIndicator,'null' as  WTIndicator,'null' as INDEXIndicator,'null' as SupplementaryInformationIdentifier, HELIX_UUID, HELIX_TIMESTAMP ,TibcoTimeStamp, '' as LoadsheetMessage FROM payloadHistory")
      val df_historySync =spark.sql("SELECT FlightNumber,Dayofmovement,DeparturePort,ArrivalPort,AircraftRegistration,FlightDate,TotalPassengersOnBoardIncludingInfants,CrewIdentifier,CrewCompliment,PassengerIdentifier,TotalPassengerNumbersPerClassValue,TotalPassengerIdentifier,ZeroFuelWeightIdentifier,ZeroFuelWeight,ZeroFuelWeightMaximumIdentifier,MaximumZeroFuelWeight,TakeOffFuelIdentifier,TakeOffFuel,TakeOffWeightIdentifier,TakeOffWeight,LandingWeight,DryOperatingWeightValue,DryOperatingIndexvalue,LoadedIndexZFWvalue,TotalPassengerWeightIdentifier,TotalPassengerWeightValue,LoadsheetIdentifier,FinalLoadsheetIdentifier,FinalLoadsheetEditionNumberIdentifier,'MAX' as TakeOffWeightMaximumIdentifier,MaximumTakeOffWeight,TripFuelIdentifier,TripFuel,LandingWeightIdentifier,LandingWeightMaximumIdentifier,MaximumLandingWeight,LimitingFactorIdentifier,BalanceandSeatingIdentifier,DryOperatingWeightLabel,DOILabel,LIZFWLabel,AerodynamicChordZFWLabel,AerodynamicChordZFWValue,LITOWLabel,LoadedIndexTOWvalue,'MACTOW' as AerodynamicChordTOWLabel,AerodynamicChordTOWvalue,CabinSectionTotals,'UNDERLOAD' as UnderloadIdentifier,UnderloadValue,LoadInCompartment,'T' as TotalWeightInCompartmentIdentifier,TotalWeightOfAllCompartmentsValue,'null' as  LMCIdentifier,'null' as  DestinationIdentifier,'null' as  SpecificationIdentifier,'null' as  PlusMinusIndicator,'null' as  WTIndicator,'null' as INDEXIndicator,'null' as SupplementaryInformationIdentifier, HELIX_UUID, HELIX_TIMESTAMP ,TibcoTimeStamp, '' as LoadsheetMessage FROM payloadHistory")
      return df_historySync

    }




    /**  Method to write processed data to HDFS
      *
      *  @param out_df Dataframe to be written to HDFS
      */
    def writePayloadHistorySync(out_df: DataFrame, coalesce_value : Int,tgt_payload_history_sync_location:String): Unit = {
      out_df.coalesce(coalesce_value).write.mode("overwrite").format("com.databricks.spark.avro").save(tgt_payload_history_sync_location)
    }
}
}
