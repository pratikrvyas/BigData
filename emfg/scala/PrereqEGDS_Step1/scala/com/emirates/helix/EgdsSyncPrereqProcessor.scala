/*----------------------------------------------------------------------------
 * Created on  : 01/Mar/2018
 * Author      : Oleg Baydakov(S754344)
 * Email       : oleg.baydakov@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsSyncPrereqArgumentsArguments.scala
 * Description : Processing EDGS data
 * ----------------------------------------------------------
 */
package com.emirates.helix

import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SparkSession}
import com.databricks.spark.avro._
import org.apache.spark.sql.functions._
import com.emirates.helix.util._
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.rdd
import org.apache.spark.sql.functions.regexp_replace
import org.joda.time._
import org.joda.time.{DateTime, Period}
import java.io.FileNotFoundException

import scala.util.{Failure, Success, Try}
import org.joda.time.{DateTime, Period}
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import java.io.FileNotFoundException

import scala.util.{Failure, Success, Try}


object EgdsSyncPrereqProcessor {


  /**
    * EgdsSyncPrereqProcessor instance creation
    * @param args user parameter instance
    * @return Processor instance
    */
  def apply(args:  EgdsSyncPrereqArguments.Args): EgdsSyncPrereqProcessor = {
    var processor = new EgdsSyncPrereqProcessor()
    processor.args = args
    processor
  }

  /**
    * EgdsSyncPrereqProcessor class with methods to
    * read decomposed EGDS data , perform data cleaning and ingest into prerequisite
    */
  class EgdsSyncPrereqProcessor extends SparkUtils {


    var args: EgdsSyncPrereqArguments.Args = _


    def readAndTransform(
                    path_to_egds_incremental:String,
                    path_to_flight_master_current:String,
                    time_period:Int,
                    date_for_folder:String
                    )(implicit spark: SparkSession): DataFrame = {



      readEGDS_Decomposed_read(path_to_egds_incremental,"df_temp",date_for_folder)

      readFlightMaster(path_to_flight_master_current,"fm")

      EGDS_duplicates("df_temp","fm", "df",time_period)

      pivotEGDS("df_egds_pivot")

      final_EDGS()
      }

    /**
      * Calculate rejected records
      * @return: Dataframe with the rejected records
      */
    def regectedRecords()
                       (implicit spark: SparkSession):DataFrame ={
      // for receted records procedure
      printMessage("regectedRecords " , "BEGIN")
      val df_egds_rejected = spark.sql("select * from df_egds_rejected")
      printMessage("regectedRecords count "+df_egds_rejected.count().toString, "END")
      df_egds_rejected

    }

    /**
      * Write Dataframe with rejected records as a parquest file
      * @param df: Dataframe to be stored in hdfs
      * @param path_to_edgs_rejected: path from input parameters
      * @param coalesce_value: from input parameters
      * @param path_to_folder_yesterday: folder name (date as string)
      * @param compression: from input parameters
      */
    def writeRejected(df:DataFrame,path_to_edgs_rejected:String, coalesce_value:Int,path_to_folder_yesterday:String,compression:String)
                     (implicit spark: SparkSession):Unit ={
        df.coalesce(coalesce_value).write.mode(org.apache.spark.sql.SaveMode.Overwrite).option("compression", compression).parquet(path_to_edgs_rejected+path_to_folder_yesterday+"/step_1")

    }

    /**
      * Read Flight Master data
      * @param path_to_flight_master_current: from input parameters
      * @param sql_table_name: SQL table name to store final Dataframe
      */
    def readFlightMaster(path_to_flight_master_current:String,sql_table_name:String)
                        (implicit spark: SparkSession):Unit ={
      // flight master
      // TODO - ADD EXCEPTION HANDLING
      // CHANGE DATE
      //val path_to_folder_today   =  DateTime.now.toString("yyyy-MM-dd")
      //val path_to_folder_today="2018-04-25"
      ///data/helix/modelled/emfg/public/flightmaster/current/snapdate=2018-04-25/*/*.parquet
      printMessage("readFlightMaster", "START", "")
      printMessage("readFlightMaster", "READ FILE FROM "+path_to_flight_master_current, "")
      //println("HELIX readFlightMaster BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      try {
        //create dataframe df
        val path_array=path_to_flight_master_current.split(",")
        val fm_ini=spark.read.parquet(path_array: _*)
        printMessage("readFlightMaster", "READ FM FILE SUCCESS", "")
        fm_ini.createOrReplaceTempView("fm_ini")
        spark.catalog.cacheTable("fm_ini")
        val fm = spark.sql("""
                select * from fm_ini
                where flight_date in (select flight_days from  df_flight_days)
               """)
        fm.createOrReplaceTempView(sql_table_name)
        spark.catalog.cacheTable(sql_table_name)
        //println("HELIX readFlightMaster FM COUNT "+fm.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))

        //println("HELIX readFlightMaster END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
        val count=fm.count

        if (count == 0) {countZeroExit()}
        printMessage("readFlightMaster", "END", "FM COUNT "+count.toString)
      } catch {
         case unknown: Exception => {
           printMessage("readFlightMaster", unknown.toString, "")
           printMessage("readFlightMaster", "READ FM FILE FAILURE", "")
           countZeroExit()
        }
      }

      }


    /**
      * Read incremental EGDS data from decomposed folder
      * @param path_to_egds_incremental: path to EGDS data from input parameters
      * @param sql_table_name: SQL table name to store final Dataframe
      * @param date_for_folder: folder name (yesterday date as string)
      * @return: string with the distinct flight dates for Flight Master filtering
      */
    // TODO - PATH SHOULD INCLUDE YETERDAY DATE
    def readEGDS_Decomposed_read(path_to_egds_incremental:String,sql_table_name:String, date_for_folder:String)
                     (implicit spark: SparkSession):Unit={
      printMessage("readEGDS_Decomposed_read", "START", " ")
      val path_to_folder_yesterday=date_for_folder
      val df_temp = spark.read.format("com.databricks.spark.avro").load(path_to_egds_incremental)

      df_temp.createOrReplaceTempView(sql_table_name)
      df_temp.cache()
      val count=df_temp.count
      printMessage("readEGDS_Decomposed_read", "COUNT EGDS", count.toString)
      if (count == 0) {countZeroExit()}

      // DATES FOR FLIGHT MASTER FILTERING
      val df_flight_days_temp = spark.sql("select distinct to_date(message_date, 'ddMMMyyyy') as flight_days from df_temp")
      df_flight_days_temp.cache
      df_flight_days_temp.createOrReplaceTempView("df_flight_days_temp")

      val df_flight_days_min = spark.sql("""select date_sub(CAST(min(flight_days) as DATE), 1) as flight_days
                                         from df_flight_days_temp""")
      val df_flight_days_max = spark.sql("""select date_add(CAST(max(flight_days) as DATE), 1) as flight_days
                                        from df_flight_days_temp""")

      val df_flight_days=df_flight_days_temp.unionAll(df_flight_days_min.unionAll(df_flight_days_max))
      df_flight_days.cache
      df_flight_days.createOrReplaceTempView("df_flight_days")

      val count_df_flight_days=df_flight_days.count
      printMessage("readEGDS_Decomposed_read", "COUNT FLIGHT DATES", count_df_flight_days.toString)
      if (count_df_flight_days == 0) {countZeroExit()}
      printMessage("readEGDS_Decomposed_read", "END")
      }

    /**
      * Remove duplicates and filter EGDS records based on actual timestamp from FLight Master
      * @param sql_table_name_source: SQL table name to read EGDS data from
      * @param fm_table: SQL table name for Flight Master
      * @param sql_table_name_destination: SQL table name to store final Dataframe
      * @param time_period: filter based on act_flight_XXX
      */
    def EGDS_duplicates(sql_table_name_source:String,fm_table:String,
                     sql_table_name_destination:String,time_period:Int)
                        (implicit spark: SparkSession):Unit ={
      ///////// REMOVE DUPLICATES BASED ON tibco_message_timestamp
      printMessage("EGDS_duplicates", "START")

      // FOR REJECTED RECORDS
      val df_HELIX_UUID_init=spark.sql("""
            select tibco_messageTime as tibco_message_timestamp,flightstate,acreg_no,
            flight_no ,flight_date ,message_date,dep_stn,arr_stn,fob,zfw
            from df_temp
            """)
      df_HELIX_UUID_init.cache
      df_HELIX_UUID_init.createOrReplaceTempView("df_HELIX_UUID_init")

      // CONNECT TO FLIGHT MASTER AND CHECK MESSAGE TIME
      val FlightStatus=Map("OUT"->"act_flight_out_datetime_utc",
                            "OFF"->"act_flight_off_datetime_utc",
                            "ON"->"act_flight_on_datetime_utc",
                            "IN"->"act_flight_in_datetime_utc")

      val checkActualTime_FlightMaster: ((String,String) => DataFrame) = {(flightstatus,actual_time_field_fm)  =>
        spark.sql("""select df_temp.*,

        cast(concat(date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy-MM-dd'),' ', concat(substring(time_of_message_delivered,1,2),':', substring(time_of_message_delivered,3,2),':00')) as Timestamp) as  time_of_message_delivered_new,

        cast(concat(date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy-MM-dd'),' ', concat(substring(time_of_message_received,1,2),':', substring(time_of_message_received,3,2),':00')) as Timestamp) as  time_of_message_received_new,

        cast(concat(date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy-MM-dd'),' ', concat(substring(time_of_message_delivered,1,2),':', substring(time_of_message_delivered,3,2),':00')) as Timestamp) as  message_datetime,

        fm.actual_reg_number as fm_acreg_no, fm.flight_number as fm_flight_no,
        fm.flight_date as fm_flight_date,
        fm.act_dep_iata_station as fm_dep_stn, fm.act_arr_iata_station as fm_arr_stn, """ + actual_time_field_fm+""" as actual_time_field_fm,

        aircraft_subtype

        from df_temp
        join fm on trim(regexp_replace(df_temp.acreg_no,'-','')) = trim(fm.actual_reg_number)
        where flightstate='"""+flightstatus+"""'
        and
        (unix_timestamp(cast(concat(date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy-MM-dd') ,' ', concat(substring(time_of_message_delivered,1,2),':', substring(time_of_message_delivered,3,2),':00')) as Timestamp))
        BETWEEN unix_timestamp("""+actual_time_field_fm+""")-"""+time_period+""" and  unix_timestamp("""+actual_time_field_fm+""")+"""+time_period+""")
        and """+actual_time_field_fm+""" is not null
                                     """)
      }
      //and flight_no like '%"""+flight_no+ """%' and acreg_no='"""+acreg_no+"""'
      val df1:DataFrame=FlightStatus.map(m => checkActualTime_FlightMaster(m._1,m._2)).reduce((df1, df2) => df1.unionAll(df2))
      df1.cache()
      df1.createOrReplaceTempView("df_new")

      // REJECTED LINES
      val df_HELIX_UUID_after=spark.sql("""
              select tibco_messageTime as tibco_message_timestamp,flightstate,acreg_no,
              flight_no ,flight_date ,message_date,dep_stn,arr_stn,fob,zfw
              from df_new
              """)
      df_HELIX_UUID_after.cache
      df_HELIX_UUID_after.createOrReplaceTempView("df_HELIX_UUID_after")

      val df_egds_rejected = df_HELIX_UUID_init.except(df_HELIX_UUID_after)
      df_egds_rejected.cache
      df_egds_rejected.createOrReplaceTempView("df_egds_rejected")


      if (df1.count() == 0) {countZeroExit()}
      printMessage("EGDS_duplicates checkActualTime_FlightMaster ", "END")

      printMessage("EGDS_duplicates df_remove_duplicates ", "START")
      //  DUPLICATES NEW
      val df_remove_duplicates=spark.sql("""
            select df_new.tibco_messageTime as tibco_message_timestamp,
            flightstate,acreg_no,flight_no,flight_date,message_date,time_of_message_received,
            time_of_message_delivered, fm_dep_stn as dep_stn, fm_arr_stn as arr_stn,fob,
            nvl(trim(zfw),'') as zfw,
            time_of_message_delivered_new,time_of_message_received_new,message_datetime,
            fm_acreg_no, fm_flight_no, fm_flight_date,
            fm_dep_stn,fm_arr_stn,actual_time_field_fm, aircraft_subtype
            from df_new

            join
            (select max(tibco_messageTime) as A_tibco_message_timestamp, flightstate as A_flightstate,
            acreg_no as A_acreg_no,flight_no as A_flight_no,flight_date as A_flight_date,
            message_date as A_message_date, max(time_of_message_received) as A_time_of_message_received ,
            max(time_of_message_delivered) as A_time_of_message_delivered,
            fm_dep_stn as A_dep_stn, fm_arr_stn as A_arr_stn, nvl(trim(zfw),'') as A_zfw

            from df_new
            group by acreg_no,flight_no,flight_date,message_date,flightstate,
            zfw, fm_acreg_no, fm_flight_no, fm_flight_date,
            fm_dep_stn,fm_arr_stn,aircraft_subtype ) A
            on df_new.tibco_messageTime=A_tibco_message_timestamp
            and df_new.acreg_no=A_acreg_no
            and df_new.flight_no=A_flight_no
            and df_new.flight_date=A_flight_date
            and df_new.message_date=A_message_date
            and df_new.flightstate=A_flightstate
            and df_new.fm_dep_stn=A_dep_stn
            and df_new.fm_arr_stn=A_arr_stn

            """)
      df_remove_duplicates.cache
      df_remove_duplicates.createOrReplaceTempView("df_remove_duplicates")
      printMessage("EGDS_duplicates df_remove_duplicates ", "END")
    }

    /**
      * Pivot EGDS data based on flightstate column
      * @param sql_table_name_destination: SQL table name to store data to
      */
    def pivotEGDS(sql_table_name_destination:String)
                 (implicit spark: SparkSession):Unit= {

      printMessage("pivotEGDS  ", "START")

      val df_egds_pivot: (String => Unit) = { s =>

        val ds:DataFrame = spark.sql("""select fm_acreg_no as acreg_no ,
                    fm_flight_no as flight_no , fm_flight_date as flight_date ,
                    fm_dep_stn as dep_stn, fm_arr_stn as arr_stn,
                    flightstate,
                    max(time_of_message_delivered_new) as time_of_message_delivered,
                    max(time_of_message_received_new) as time_of_message_received,
                    max(tibco_message_timestamp) as tibco_message_timestamp,

                    max(actual_time_field_fm) as actual_time_field_fm,

                    max(message_datetime) as message_datetime,

                    zfw,
                    sum(cast(fob*100 as bigint)) as fob
                    from df_remove_duplicates
                    where (fm_acreg_no is not null and fm_flight_no is not null  and fm_flight_date is not null )

                    group by fm_acreg_no, fm_flight_no, fm_flight_date,  fm_dep_stn,fm_arr_stn, flightstate, zfw  order by fm_acreg_no, fm_flight_date, time_of_message_delivered  """)
          .groupBy("acreg_no", "flight_no", "flight_date", "dep_stn","arr_stn")
          .pivot("flightstate",Seq("OUT","OFF", "ON", "IN"))
          .agg( max("tibco_message_timestamp"), max("time_of_message_received"), max("message_datetime"), max("time_of_message_delivered"),max("actual_time_field_fm"),max("zfw"),sum("fob"))
          .toDF("acreg_no","flight_no", "flight_date", "dep_stn","arr_stn",
            "tibco_message_timestamp_FUEL_OUT","time_of_message_received_FUEL_OUT", "msgDeliveredDateTime_FUEL_OUT", "time_of_message_delivered_out", "act_flight_OUT_datetime_utc", "zfw_out", "FUEL_OUT",
            "tibco_message_timestamp_FUEL_OFF","time_of_message_received_FUEL_OFF", "msgDeliveredDateTime_FUEL_OFF", "time_of_message_delivered_off", "act_flight_OFF_datetime_utc","zfw_off", "FUEL_OFF",
            "tibco_message_timestamp_FUEL_ON","time_of_message_received_FUEL_ON", "msgDeliveredDateTime_FUEL_ON", "time_of_message_delivered_on", "act_flight_ON_datetime_utc" , "zfw_on", "FUEL_ON",
            "tibco_message_timestamp_FUEL_IN","time_of_message_received_FUEL_IN", "msgDeliveredDateTime_FUEL_IN", "time_of_message_delivered_in", "act_flight_IN_datetime_utc", "zfw_in", "FUEL_IN"
          )
        ds.createOrReplaceTempView(s)
      }
      df_egds_pivot(sql_table_name_destination)
      printMessage("pivotEGDS  ", "END")
      }


    /**
      * Deduplication and splitting EGDS dataframe to today+yesterday  and tomorrow flights
      * @param df_egds_fm: EGDS dataframe to read data from
      * @return: Dataframe tuple (final EGDS dataframe and tomorrow flights)
      */
    def final_EDGS()
                   (implicit spark: SparkSession):DataFrame= {
      //println("HELIX final_EDGS BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("final_EDGS  ", "START")
     // DEDUPLICATION

      val df_dedup=spark.sql("""
        select acreg_no,	flight_no,	flight_date,	dep_stn,	arr_stn,

        max(concat(nvl(zfw_out,''),nvl(zfw_off,''),nvl(zfw_on,''),nvl(zfw_in,''))) as zfw,

        max(tibco_message_timestamp_FUEL_OUT) as tibco_message_timestamp_FUEL_OUT ,
        max(time_of_message_received_FUEL_OUT) as time_of_message_received_FUEL_OUT,
        max(msgDeliveredDateTime_FUEL_OUT) as msgDeliveredDateTime_FUEL_OUT,
        max(act_flight_out_datetime_utc) as act_flight_OUT_datetime_utc,
        sum(FUEL_OUT) as OOOI_FUEL_OUT ,

        max(tibco_message_timestamp_FUEL_OFF) as tibco_message_timestamp_FUEL_OFF,
        max(time_of_message_received_FUEL_OFF) as time_of_message_received_FUEL_OFF,
        max(msgDeliveredDateTime_FUEL_OFF) as msgDeliveredDateTime_FUEL_OFF,
        max(act_flight_off_datetime_utc) as act_flight_OFF_datetime_utc,
        sum(FUEL_OFF) as OOOI_FUEL_OFF,

        max(tibco_message_timestamp_FUEL_ON) as tibco_message_timestamp_FUEL_ON,
        max(time_of_message_received_FUEL_ON) as time_of_message_received_FUEL_ON,
        max(msgDeliveredDateTime_FUEL_ON) as msgDeliveredDateTime_FUEL_ON,
        max(act_flight_on_datetime_utc) as act_flight_ON_datetime_utc,
        sum(FUEL_ON) as OOOI_FUEL_ON,

        max(tibco_message_timestamp_FUEL_IN) as tibco_message_timestamp_FUEL_IN,
        max(time_of_message_received_FUEL_IN) as time_of_message_received_FUEL_IN,
        max(msgDeliveredDateTime_FUEL_IN) as msgDeliveredDateTime_FUEL_IN,
        max(act_flight_in_datetime_utc) as act_flight_IN_datetime_utc,
        sum(FUEL_IN) as OOOI_FUEL_IN

        from df_egds_pivot
        group by acreg_no,	flight_no,	flight_date,	dep_stn,	arr_stn
        """)
      df_dedup.createOrReplaceTempView("df_dedup")
      spark.catalog.cacheTable("df_dedup")
      val df_final=df_dedup
      df_final.cache()
      //println("HELIX final_EDGS df_final COUNT "+df_final.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val count=df_final.count
      printMessage("final_EDGS  ", "df_final COUNT ",count.toString)
      if (count == 0) {countZeroExit()}
      printMessage("final_EDGS  ", "END ")
      df_final
    }

    /**
      * Write final EGDS dataframe to prerequisite /step_1/data
      * @param df_egds_fm: EGDS dataframe
      * @param path_to_edgs_prerequisite: path from input parameters
      * @param coalesce_vale: from input parameters
      * @param path_to_folder_date: folder name from input parameters (yesterday)
      * @param compression: from input parameters
      */
    def writeEDGSprerequisite(df_egds_fm:DataFrame,path_to_edgs_prerequisite:String,coalesce_vale:Int,path_to_folder_date:String, compression:String)
                             (implicit spark: SparkSession):Unit ={
      // TODO - ADD EXCEPTION HANDLING
     // println("HELIX writeEDGSprerequisite BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("final_EDGS WRITE TO "+path_to_edgs_prerequisite+path_to_folder_date.toString, "START ")
      df_egds_fm.cache()
      printMessage("final_EDGS  WRITING df_final_all "+df_egds_fm.count, " ")
      df_egds_fm.coalesce(coalesce_vale).write.mode(org.apache.spark.sql.SaveMode.Overwrite).option("compression", compression).parquet(path_to_edgs_prerequisite+path_to_folder_date+"/step_1/data/")
      printMessage("final_EDGS  ", "END ")
    }


    //def writeEDGSprerequisite_tomorrow(df_egds_tomorrow:DataFrame,path_to_edgs_prerequisite:String,coalesce_vale:Int,path_to_folder_date:String, compression:String)
    //                         (implicit spark: SparkSession):Unit ={
      // TODO - ADD EXCEPTION HANDLING
      //val df_final = df_egds_tomorrow.drop("df_egds_acreg_no" ,"df_egds_flight_no" , "df_egds_flight_date" ,"df_egds_message_date", "df_egds_dep_stn", "df_egds_arr_stn")
    //  printMessage("writeEDGSprerequisite_tomorrow  WRITING df_final_all "+df_egds_tomorrow.count, " START ")
    //  df_egds_tomorrow.coalesce(coalesce_vale).write.mode(org.apache.spark.sql.SaveMode.Overwrite).option("compression", compression).parquet(path_to_edgs_prerequisite+path_to_folder_date+"/step_1/from_yesterday")
    //  printMessage("writeEDGSprerequisite_tomorrow  ", " END ")
    //}

    def printMessage(FunctionName:String, message:String, Count:String=""):Unit={
      println("HELIX LOG "+FunctionName+" ; "+message+" ; "+" "+Count+" ; "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))

    }

   def  countZeroExit()={
     println("HELIX LOG Count is zero, exiting....."+" ; "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))

   }

    // READ FILE EXCEPTIOn
    def readDataFrame(fileName: String, fileFormat:String)(implicit spark: SparkSession): Try[DataFrame] = {
      try {
        //create dataframe df
        val df = spark.read.format(fileFormat).load(fileName)
        Success(df)
      } catch {
        case ex: AnalysisException => {
          //println(s"File $fileName not found")
          printMessage("readDataFrame "+fileName+" not found", " ")
          Failure(ex)
        }
        case unknown: Exception => {
          //println(s"Unknown exception: $unknown")
          printMessage("readDataFrame Unknown exception: "+unknown, " ")
          Failure(unknown)
        }
      }
    }
  }
}


