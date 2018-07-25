/*----------------------------------------------------------------------------
 * Created on  : 06/Mar/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PayloadLoadsheetDedupProcessor.scala
 * Description : Dedup Payoad Loadsheet data.
 * ---------------------------------------------------------------------------*/

package com.emirates.helix


import com.emirates.helix.util._
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.TimestampType




object PayloadLoadsheetDedupProcessor {


  /**
    * PayloadLoadsheetDedupProcessor instance creation
    * @param args user parameter instance
    * @return Processor instance
    */
  def apply(args:  PayloadLoadsheetDedupArguments.Args): PayloadLoadsheetDedupProcessor = {
    var processor = new PayloadLoadsheetDedupProcessor()
    processor.args = args
    processor
  }


  class PayloadLoadsheetDedupProcessor extends SparkUtils {
    var args: PayloadLoadsheetDedupArguments.Args = _




    /** Method to read and union  loadsheet incremental and history data
      *
      * output union dataset
      */
    def readPayloadLoadsheetHistoryIncrement(implicit spark: SparkSession): DataFrame = {

         val df_loadsheet= spark.read.avro(args.payload_src_loadsheet_incremental_location)
        df_loadsheet.createOrReplaceTempView("loadsheet")

      //val df_incremenalLoadsheet =spark.sql("SELECT  regexp_replace(FlightNumber,regexp_extract(FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ),lpad(regexp_extract(FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ),4,0)) as FlightNumber,Dayofmovement,DeparturePort,ArrivalPort,AircraftRegistration, date_format(from_unixtime(unix_timestamp(FlightDate,'ddMMMyy')),'yyyy-MM-dd')  as FlightDate,TotalPassengersOnBoardIncludingInfants,CrewIdentifier,CrewCompliment,PassengerIdentifier,TotalPassengerNumbersPerClassValue,TotalPassengerIdentifier,ZeroFuelWeightIdentifier,ZeroFuelWeight,ZeroFuelWeightMaximumIdentifier,MaximumZeroFuelWeight,TakeOffFuelIdentifier,TakeOffFuel,TakeOffWeightIdentifier,TakeOffWeight,LandingWeight,DryOperatingWeightValue,DryOperatingIndexvalue,LoadedIndexZFWvalue,TotalPassengerWeightIdentifier,TotalPassengerWeightValue,LoadsheetIdentifier,FinalLoadsheetIdentifier,FinalLoadsheetEditionNumberIdentifier,MaximumTakeOffWeight,TripFuelIdentifier,TripFuel,LandingWeightIdentifier,LandingWeightMaximumIdentifier,MaximumLandingWeight,LimitingFactorIdentifier,BalanceandSeatingIdentifier,DryOperatingWeightLabel,DOILabel,LIZFWLabel,AerodynamicChordZFWLabel,AerodynamicChordZFWValue,LITOWLabel,LoadedIndexTOWvalue,AerodynamicChordTOWvalue,CabinSectionTotals,UnderloadValue,LoadInCompartment,TotalWeightOfAllCompartmentsValue,LMCIdentifier ,DestinationIdentifier,SpecificationIdentifier,PlusMinusIndicator,WTIndicator,INDEXIndicator,SupplementaryInformationIdentifier,HELIX_UUID,HELIX_TIMESTAMP, from_unixtime(unix_timestamp(substring(regexp_replace(tibco_message_time, 'T' , ' ' ),0,19),'yyyy-MM-dd HH:mm:ss')) as TibcoTimeStamp,LoadsheetMessage FROM loadsheet")
      val df_incremenalLoadsheet =spark.sql("SELECT  regexp_replace(FlightNumber,regexp_extract(FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ),lpad(regexp_extract(FlightNumber, '([A-Za-z]+)([0-9]+)' , 2 ),4,0)) as FlightNumber,Dayofmovement,DeparturePort,ArrivalPort,AircraftRegistration, date_format(from_unixtime(unix_timestamp(FlightDate,'ddMMMyy')),'yyyy-MM-dd')  as FlightDate,TotalPassengersOnBoardIncludingInfants,CrewIdentifier,CrewCompliment,PassengerIdentifier, case when trim(TotalPassengerNumbersPerClassValue) is null or trim(TotalPassengerNumbersPerClassValue) = '' or trim(TotalPassengerNumbersPerClassValue) = 'null' then 'null/null/null' when length(translate(regexp_replace(trim(TotalPassengerNumbersPerClassValue),'\\[0-9]*:?',''),'.','')) = 2 then trim(TotalPassengerNumbersPerClassValue) when length(translate(regexp_replace(trim(TotalPassengerNumbersPerClassValue),'\\[0-9]*:?',''),'.','')) = 1 then concat('null/',TotalPassengerNumbersPerClassValue) when length(translate(regexp_replace(trim(TotalPassengerNumbersPerClassValue),'\\[0-9]*:?',''),'.','')) = 0 then concat('null/null/',TotalPassengerNumbersPerClassValue) end as  TotalPassengerNumbersPerClassValue,TotalPassengerIdentifier,ZeroFuelWeightIdentifier,ZeroFuelWeight,ZeroFuelWeightMaximumIdentifier,MaximumZeroFuelWeight,TakeOffFuelIdentifier,TakeOffFuel,TakeOffWeightIdentifier,TakeOffWeight,LandingWeight,DryOperatingWeightValue,DryOperatingIndexvalue,LoadedIndexZFWvalue,TotalPassengerWeightIdentifier,TotalPassengerWeightValue,LoadsheetIdentifier,FinalLoadsheetIdentifier,FinalLoadsheetEditionNumberIdentifier,'MAX' as TakeOffWeightMaximumIdentifier,MaximumTakeOffWeight,TripFuelIdentifier,TripFuel,LandingWeightIdentifier,LandingWeightMaximumIdentifier,MaximumLandingWeight,LimitingFactorIdentifier,BalanceandSeatingIdentifier,DryOperatingWeightLabel,DOILabel,LIZFWLabel,AerodynamicChordZFWLabel,AerodynamicChordZFWValue,LITOWLabel,LoadedIndexTOWvalue,'MACTOW' as AerodynamicChordTOWLabel,AerodynamicChordTOWvalue,CabinSectionTotals,'UNDERLOAD' as UnderloadIdentifier,UnderloadValue,LoadInCompartment,'T' as TotalWeightInCompartmentIdentifier,TotalWeightOfAllCompartmentsValue,LMCIdentifier ,DestinationIdentifier,SpecificationIdentifier,PlusMinusIndicator,WTIndicator,INDEXIndicator,SupplementaryInformationIdentifier,HELIX_UUID,HELIX_TIMESTAMP, from_unixtime(unix_timestamp(substring(regexp_replace(tibco_message_time, 'T' , ' ' ),0,19),'yyyy-MM-dd HH:mm:ss')) as TibcoTimeStamp,LoadsheetMessage FROM loadsheet")

      if (args.data_load_mode.toUpperCase().equals("HISTORY")) {

        val  df_history = spark.read.avro(args.payload_src_history_sync_location)
        val loadsheet_hist_incr_df = df_incremenalLoadsheet.union(df_history)


        return loadsheet_hist_incr_df
      }
      else
      {
        val  df_history = spark.read.parquet(args.payload_src_history_sync_location)
        val loadsheet_hist_incr_df = df_incremenalLoadsheet.union(df_history)

        return loadsheet_hist_incr_df
      }




    }

    /** Method to dedup loadsheet incremental and history data
      *
      * output dedup dataset
      */
    def deDupPayloadLoadsheet(loadsheet_hist_incr_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

      import org.apache.spark.sql.types.LongType


      //val loadsheetGrp = Window.partitionBy("FlightNumber","DeparturePort","ArrivalPort","AircraftRegistration","FlightDate","FinalLoadsheetEditionNumberIdentifier")
      val loadsheetGrp = Window.partitionBy("FlightNumber","DeparturePort","ArrivalPort","AircraftRegistration","FlightDate")
      val deduped_df = loadsheet_hist_incr_df.dropDuplicates()

      //val dedup_grp_df = deduped_df.withColumn("MAX_TIMESTAMP", max(col("TibcoTimeStamp").cast(TimestampType)).over(loadsheetGrp))
      val dedup_grp_df = deduped_df.withColumn("MAX_TIMESTAMP", max(col("TibcoTimeStamp").cast(TimestampType).cast(LongType)).over(loadsheetGrp))

      //val dedup_max_df=  dedup_grp_df.filter(dedup_grp_df("MAX_TIMESTAMP") === (dedup_grp_df("TibcoTimeStamp").cast(TimestampType))).
      //                  drop("MAX_TIMESTAMP")


      val dedup_max_df=  dedup_grp_df.filter(
        dedup_grp_df("MAX_TIMESTAMP").cast(TimestampType).cast(LongType) === (dedup_grp_df("TibcoTimeStamp").cast(TimestampType).cast(LongType)))
          .drop("MAX_TIMESTAMP")

      return dedup_max_df

    }

    /** Method to write processed data to HDFS
      *
      * @param out_df Dataframe to be written to HDFS
      */
    def writePayloadLoadsheetDeDupe(out_df: DataFrame, lookback_months : Int, coalesce_value : Int) (implicit spark: SparkSession): Unit = {


      if (args.data_load_mode.toUpperCase().equals("HISTORY")) {
        out_df.createOrReplaceTempView("payload_loadsheet_temptab")

        val max_flight_date = spark.sql("select max(FlightDate) from payload_loadsheet_temptab").first().mkString

        val incr_sql = "select * from payload_loadsheet_temptab where to_date(FlightDate) >= " +
          "add_months(to_date('" + max_flight_date + "')," + -1 * lookback_months + ")"
        val hist_sql = "select * from payload_loadsheet_temptab where to_date(FlightDate) < " +
          "add_months(to_date('" + max_flight_date + "')," + -1 * lookback_months + ")"

        val output_df_last_incr = spark.sql(incr_sql).coalesce(coalesce_value)
        val output_df_last_hist = spark.sql(hist_sql).coalesce(coalesce_value)

        output_df_last_hist.write.format("parquet").save(args.payload_tgt_loadsheet_dedupe_history_location)
        output_df_last_incr.write.format("parquet").save(args.payload_tgt_loadsheet_dedupe_current_location)
      }
      else
      {
        out_df.coalesce(coalesce_value).write.format("parquet").save(args.payload_tgt_loadsheet_dedupe_current_location)
      }
    }
  }
}
