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


object EgdsSyncPrereqProcessor_OLD {


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

      pivotEGDS("df","df_egds_temp")

      joinDateTimeEDGS("df_egds_temp","df_egds")
      val df_egds_fm = joinEDGS_FM("df_egds","df_egds_fm",time_period)
      final_EDGS(df_egds_fm)
      }

    /**
      * Calculate rejected records
      * @return: Dataframe with the rejected records
      */
    def regectedRecords()
                       (implicit spark: SparkSession):DataFrame ={
      // for receted records procedure
      val df_egds_short = spark.sql("select acreg_no ,flight_no , flight_date ,message_date, dep_stn, arr_stn from df_egds")
      val df_egds_fm_short = spark.sql("select df_egds_acreg_no ,df_egds_flight_no , df_egds_flight_date ,df_egds_message_date, df_egds_dep_stn, df_egds_arr_stn from df_egds_fm")
      val df_egds_rejected=df_egds_short.except(df_egds_fm_short)
      df_egds_fm_short

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

      println("HELIX readFlightMaster BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val fm_ini=spark.read.parquet(path_to_flight_master_current)
      printMessage("readFlightMaster", "START", "")

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
      printMessage("readFlightMaster", "END", "FM COUNT "+count.toString)
      if (count == 0) {countZeroExit()}
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
      //println("HELIX readEGDS_Decomposed_read BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("readEGDS_Decomposed_read", "START", " ")
      // CHANGE DATE
      //val path_to_folder_yesterday   =  DateTime.now.minusDays(1).toString("yyyy-MM-dd")
      val path_to_folder_yesterday=date_for_folder
      val df_temp = spark.read.format("com.databricks.spark.avro").load(path_to_egds_incremental)
      // DELETE LATER
      //df_temp.cache
      //df_temp.createOrReplaceTempView("df_temp")
      //val df_temp=spark.sql("select * from df_temp1 where message_date='01JAN2018' and flight_no like '%029%' ")

      //val df_temp=spark.sql("select * from df_temp1 where (message_date='31DEC2018' OR message_date='01JAN2018' OR message_date='02JAN2018') and (flight_no like '%029' or flight_no like '%030') ")
      //val df_temp=spark.sql("select * from df_temp1 where date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy')='2018' ")

      ////////////////////////
      df_temp.createOrReplaceTempView(sql_table_name)
      df_temp.cache()
      val count=df_temp.count
      printMessage("readEGDS_Decomposed_read", "COUNT EGDS", count.toString)
      if (count == 0) {countZeroExit()}
      //println("HELIX readEGDS_Decomposed_read COUNT EGDS  "+df_temp.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))

      // CHECK WHAT DATE WE HAVE IN THE BATCH TO FILTER RECORDS FROM FLIGHT MASTER
      import spark.implicits._
      //val flight_days = spark.sql("select distinct flight_date, date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy-MM') from df_temp").map(r => r.getString(1)+"-"+r.getString(0)).collect.toList
      //val flight_dates_fm=flight_days.map(date => "flight_date='"+date.toString).mkString("' or ")
      val df_flight_days = spark.sql("select distinct to_date(message_date, 'ddMMMyyyy') as flight_days from df_temp")
      df_flight_days.cache
      df_flight_days.createOrReplaceTempView("df_flight_days")
      //println("HELIX readEGDS_Decomposed_read COUNT FLIGHT DATES  "+df_flight_days.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val count_df_flight_days=df_flight_days.count
      printMessage("readEGDS_Decomposed_read", "COUNT FLIGHT DATES", count_df_flight_days.toString)
      if (count_df_flight_days == 0) {countZeroExit()}
      printMessage("readEGDS_Decomposed_read", "END")
      //println("HELIX readEGDS_Decomposed_read END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
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
      //println("HELIX EGDS_duplicates BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("EGDS_duplicates", "START")
      val df_remove_duplicates=spark.sql("""
               select df_temp.tibco_messageTime as tibco_message_timestamp,
               flightstate,acreg_no,flight_no,flight_date,message_date,time_of_message_received,
               time_of_message_delivered,dep_stn,arr_stn,fob,
               nvl(trim(zfw), '') as zfw
               from """+sql_table_name_source+""" join
               (select max(tibco_messageTime) as A_tibco_message_timestamp, flightstate as A_flightstate,
               acreg_no as A_acreg_no,flight_no as A_flight_no,flight_date as A_flight_date,
               message_date as A_message_date, max(time_of_message_received) as A_time_of_message_received ,
               max(time_of_message_delivered) as A_time_of_message_delivered,
               dep_stn as A_dep_stn, arr_stn as A_arr_stn, nvl(trim(zfw),'') as A_zfw
               from """+sql_table_name_source+"""
               group by acreg_no,flight_no,flight_date,message_date,flightstate,
               dep_stn,arr_stn,zfw) A
               on df_temp.tibco_messageTime=A_tibco_message_timestamp
               and df_temp.acreg_no=A_acreg_no
               and df_temp.flight_no=A_flight_no
               and df_temp.flight_date=A_flight_date
               and df_temp.message_date=A_message_date
               and df_temp.flightstate=A_flightstate
               and df_temp.dep_stn=A_dep_stn
               and df_temp.arr_stn=A_arr_stn
                    """)

      df_remove_duplicates.createOrReplaceTempView("df_remove_duplicates")
      //val asd = spark.sql("select * from df_remove_duplicates where message_date='01JAN2018' and flight_no like '%029%' ")
     // printMessage("EGDS_duplicates  ASD  COUNT  "+asd.count(), " ")

      spark.catalog.cacheTable("df_remove_duplicates")
      // CONNECT TO FLIGHT MASTER AND CHECK MESSAGE TIME
      val FlightStatus=Map("OUT"->"act_flight_out_datetime_utc",
                            "OFF"->"act_flight_off_datetime_utc",
                            "ON"->"act_flight_on_datetime_utc",
                            "IN"->"act_flight_in_datetime_utc")

      val checkActualTime_FlightMaster: ((String,String) => DataFrame) = {(flightstatus,actual_time_field_fm)  =>
        val df_checkActualTime_FlightMaster = spark.sql("""select df_remove_duplicates.*
                    from df_remove_duplicates
                    join """+fm_table+""" on trim(regexp_replace(df_remove_duplicates.acreg_no,'-','')) = trim(fm.actual_reg_number)
                    where flightstate='"""+flightstatus+"""'
                    and
                    (unix_timestamp(cast(concat(date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy-MM-dd') ,' ', concat(substring(time_of_message_delivered,1,2),':', substring(time_of_message_delivered,3,2),':00')) as Timestamp))
                    BETWEEN unix_timestamp("""+actual_time_field_fm+""")-"""+time_period+""" and  unix_timestamp("""+actual_time_field_fm+""")+"""+time_period+""")
                    """)
        println("HELIX checkActualTime_FlightMaster  "+flightstatus+" ; "+actual_time_field_fm+"  "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))


        println("HELIX  "+ """select df_remove_duplicates.*
                    from df_remove_duplicates
                    join """+fm_table+""" on trim(regexp_replace(df_remove_duplicates.acreg_no,'-','')) = trim(fm.actual_reg_number)
                    where flightstate='"""+flightstatus+"""'
                    and
                    (unix_timestamp(cast(concat(date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy-MM-dd') ,' ', concat(substring(time_of_message_delivered,1,2),':', substring(time_of_message_delivered,3,2),':00')) as Timestamp))
                    BETWEEN unix_timestamp("""+actual_time_field_fm+""")-"""+time_period+""" and  unix_timestamp("""+actual_time_field_fm+""")+"""+time_period+""")
                                                                                                                                                               """)
        df_checkActualTime_FlightMaster
      }
      //println("HELIX EGDS_duplicates + checkActualTime_FlightMaster  "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("EGDS_duplicates checkActualTime_FlightMaster", "START")
      val df=FlightStatus.map(m => checkActualTime_FlightMaster(m._1,m._2)).reduce((df1, df2) => df1.unionAll(df2))
      df.createOrReplaceTempView(sql_table_name_destination)
      spark.catalog.cacheTable(sql_table_name_destination)
      //println("HELIX EGDS_duplicates DF COUNT "+df.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val count=df.count
      printMessage("EGDS_duplicates checkActualTime_FlightMaster ", "DF COUNT",count.toString)

     // val asd1 = spark.sql("select * from df where message_date='01JAN2018' and flight_no like '%029%' ")
     // printMessage("EGDS_duplicates  ASD1  COUNT  "+asd1.count(), " ")


      if (count == 0) {countZeroExit()}
      //if (df.count==0) {System.exit(1)}
      //println("HELIX EGDS_duplicates END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("EGDS_duplicates checkActualTime_FlightMaster ", "END")
    }

    /**
      * Pivot EGDS data based on flightstate column
      * @param sql_table_name_source: SQL table name to select data from
      * @param sql_table_name_destination: SQL table name to store data to
      */
    def pivotEGDS(sql_table_name_source:String,sql_table_name_destination:String)
                 (implicit spark: SparkSession):Unit= {
      println("HELIX pivotEGDS BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("pivotEGDS  ", "START")
      // DataFrame Pivot
      val ds:DataFrame = spark.sql("""select acreg_no, flight_no, flight_date, flightstate, max(date_format(to_date(message_date, 'ddMMMyyyy'),'yyyy-MM-dd')) as message_date,
                max(time_of_message_delivered) as time_of_message_delivered,
                max(time_of_message_received) as time_of_message_received,
                max(tibco_message_timestamp) as tibco_message_timestamp,
                dep_stn,arr_stn,
                zfw,
                sum(cast(fob*100 as bigint)) as fob from """+sql_table_name_source+"""
                where (acreg_no is not null and flight_no is not null and  message_date is not null )
                 group by message_date, acreg_no, flight_no, flight_date,  dep_stn,arr_stn, flightstate, zfw  order by acreg_no,message_date,time_of_message_delivered  """)
              .groupBy("acreg_no","message_date", "flight_no", "flight_date", "dep_stn","arr_stn","zfw")
              .pivot("flightstate",Seq("OUT","OFF", "ON", "IN"))
              .agg( max("tibco_message_timestamp"), max("time_of_message_received"), max("message_date"), max("time_of_message_delivered"),max("zfw"), sum("fob"))
              .toDF("acreg_no","message_date","flight_no", "flight_date", "dep_stn","arr_stn", "zfw",
                "tibco_message_timestamp_out","time_of_message_received_out", "message_date_out", "time_of_message_delivered_out", "zfw_out", "fob_out",
                "tibco_message_timestamp_off","time_of_message_received_off", "message_date_off", "time_of_message_delivered_off", "zfw_off", "fob_off",
                "tibco_message_timestamp_on","time_of_message_received_on", "message_date_on", "time_of_message_delivered_on", "zfw_on", "fob_on",
                "tibco_message_timestamp_in","time_of_message_received_in", "message_date_in", "time_of_message_delivered_in", "zfw_in", "fob_in"
        )

      ds.createOrReplaceTempView(sql_table_name_destination)
      spark.catalog.cacheTable(sql_table_name_destination)
      val count=ds.count
      printMessage("pivotEGDS  ", "DS COUNT",count.toString)
     // val asd = spark.sql("select * from df_egds_temp where flight_date='2018-01-01' and flight_no like '%029%' ")
     // printMessage("pivotEGDS ASD  COUNT  "+asd.count(), " ")
      if (count == 0) {countZeroExit()}
      //println("HELIX pivotEGDS DS COUNT "+ds.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("pivotEGDS  ", "END")
      //println("HELIX pivotEGDS END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
    }

    /**
      * Convert date and time column to timestamp column
      * @param sql_table_name_source: SQL table name to select data from
      * @param sql_table_name_destination: SQL table name to store data to
      * @param spark
      */
    def joinDateTimeEDGS(sql_table_name_source:String,sql_table_name_destination:String)
                        (implicit spark: SparkSession):Unit= {
     // println("HELIX joinDateTimeEDGS BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("joinDateTimeEDGS  ", "START")
      val ds=spark.sql("""select
                          acreg_no, flight_no, flight_date, message_date, dep_stn, arr_stn,
                          zfw_out,zfw_off,zfw_on,zfw_in,
                          fob_out as FUEL_OUT,fob_off as FUEL_OFF,fob_on as FUEL_ON, fob_in as FUEL_IN,
                          cast(concat(message_date_out,' ', concat(substring(time_of_message_delivered_out,1,2),':', substring(time_of_message_delivered_out,3,2),':00')) as Timestamp) as  message_datetime_out,
                          cast(concat(message_date_off,' ', concat(substring(time_of_message_delivered_off,1,2),':', substring(time_of_message_delivered_off,3,2),':00')) as Timestamp) as  message_datetime_off,
                          cast(concat(message_date_on,' ', concat(substring(time_of_message_delivered_on,1,2),':', substring(time_of_message_delivered_on,3,2),':00')) as Timestamp) as  message_datetime_on,
                          cast(concat(message_date_in,' ', concat(substring(time_of_message_delivered_in,1,2),':', substring(time_of_message_delivered_in,3,2),':00')) as Timestamp) as  message_datetime_in,
                          time_of_message_received_out, time_of_message_received_off , time_of_message_received_on,time_of_message_received_in,
                          tibco_message_timestamp_in, tibco_message_timestamp_out,tibco_message_timestamp_on,tibco_message_timestamp_off
                          from """+sql_table_name_source+""" order by flight_date, message_date, acreg_no, flight_no""")
      ds.createOrReplaceTempView(sql_table_name_destination)
      spark.catalog.cacheTable(sql_table_name_destination)
      //println("HELIX joinDateTimeEDGS DS COUNT "+ds.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
     val count=ds.count
      printMessage("joinDateTimeEDGS  ", "DS COUNT",count.toString)

      if (count == 0) {countZeroExit()}
     // println("HELIX joinDateTimeEDGS END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("joinDateTimeEDGS  ", "END")
      //ds
    }

    /**
      * Join EGDS and Flight Mater data and filter by actual_flight_XXX datetime
      * @param sql_table_name_source: SQL table name to select data from
      * @param sql_table_name_destination: SQL table name to store data to
      * @param time_period: for message datetime filter
      * @return - EGDS dataframe
      */
    def joinEDGS_FM(sql_table_name_source:String,sql_table_name_destination:String,time_period:Int)
                        (implicit spark: SparkSession):DataFrame= {
      //println("HELIX joinEDGS_FM BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("joinEDGS_FM  ", "START")
      val df_egds_fm=spark.sql("""select fm.actual_reg_number as acreg_no, fm.flight_number as flight_no, fm.flight_date as flight_date,
        fm.act_dep_iata_station as dep_stn, fm.act_arr_iata_station as arr_stn,

        concat(nvl(zfw_out,' '),nvl(zfw_off,' '),nvl(zfw_on,' '),nvl(zfw_in,' ')) as zfw,

        df_egds.acreg_no as df_egds_acreg_no  ,df_egds.flight_no as df_egds_flight_no , df_egds.flight_date as df_egds_flight_date ,
        df_egds.message_date as df_egds_message_date, df_egds.dep_stn as  df_egds_dep_stn, df_egds.arr_stn as df_egds_arr_stn,

        tibco_message_timestamp_out as tibco_message_timestamp_FUEL_OUT,
        time_of_message_received_out as time_of_message_received_FUEL_OUT,
        message_datetime_out as msgDeliveredDateTime_FUEL_OUT, act_flight_out_datetime_utc, FUEL_OUT ,

        tibco_message_timestamp_off as tibco_message_timestamp_FUEL_OFF,
        time_of_message_received_off as time_of_message_received_FUEL_OFF,
        message_datetime_off as msgDeliveredDateTime_FUEL_OFF, act_flight_off_datetime_utc, FUEL_OFF ,

        tibco_message_timestamp_on as tibco_message_timestamp_FUEL_ON,
        time_of_message_received_on as time_of_message_received_FUEL_ON,
        message_datetime_on as msgDeliveredDateTime_FUEL_ON, act_flight_on_datetime_utc, FUEL_ON,

        tibco_message_timestamp_in as tibco_message_timestamp_FUEL_IN,
        time_of_message_received_in as time_of_message_received_FUEL_IN,
        message_datetime_in as msgDeliveredDateTime_FUEL_IN, act_flight_in_datetime_utc, FUEL_IN
        from """+sql_table_name_source+"""
        join fm on trim(regexp_replace(df_egds.acreg_no,'-','')) = trim(fm.actual_reg_number)
        and fm.act_dep_iata_station=dep_stn and fm.act_arr_iata_station=arr_stn
        where
        ((unix_timestamp(message_datetime_on) BETWEEN unix_timestamp(act_flight_on_datetime_utc)-"""+time_period+""" and  unix_timestamp(act_flight_on_datetime_utc)+"""+time_period+""")
        OR  (unix_timestamp(message_datetime_off) BETWEEN unix_timestamp(act_flight_off_datetime_utc)-"""+time_period+""" and  unix_timestamp(act_flight_off_datetime_utc)+"""+time_period+""")
        OR (unix_timestamp(message_datetime_in) BETWEEN unix_timestamp(act_flight_in_datetime_utc)-"""+time_period+""" and  unix_timestamp(act_flight_in_datetime_utc)+"""+time_period+""")
        OR (unix_timestamp(message_datetime_out) BETWEEN unix_timestamp(act_flight_out_datetime_utc)-"""+time_period+""" and  unix_timestamp(act_flight_out_datetime_utc)+"""+time_period+""")
         )
        """)
      df_egds_fm.createOrReplaceTempView(sql_table_name_destination)
      df_egds_fm.cache()
      val count=df_egds_fm.count
      printMessage("joinEDGS_FM  ", "COUNT",count.toString)

      //val asd = spark.sql("select * from df_egds where flight_date='2018-01-01' and flight_no like '%029%' ")
     // printMessage("joinEDGS_FM ASD  COUNT  "+asd.count(), " ")

      if (count == 0) {countZeroExit()}
      //println("HELIX joinEDGS_FM df_egds_fm COUNT "+df_egds_fm.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))

     // println("HELIX joinEDGS_FM END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("joinEDGS_FM  ", "END")
      df_egds_fm
    }

    /**
      * Deduplication and splitting EGDS dataframe to today+yesterday  and tomorrow flights
      * @param df_egds_fm: EGDS dataframe to read data from
      * @return: Dataframe tuple (final EGDS dataframe and tomorrow flights)
      */
    def final_EDGS(df_egds_fm:DataFrame)
                   (implicit spark: SparkSession):DataFrame= {
      //println("HELIX final_EDGS BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      printMessage("final_EDGS  ", "START")
      val df_egds_fm_clean = df_egds_fm.drop("df_egds_acreg_no" ,"df_egds_flight_no" , "df_egds_flight_date" , "df_egds_dep_stn", "df_egds_arr_stn","df_egds_message_date")
      // DEDUPLICATION
      df_egds_fm_clean.createOrReplaceTempView("df_egds_fm_clean")
      val df_dedup=spark.sql("""
        select acreg_no,	flight_no,	flight_date,	dep_stn,	arr_stn,	max(zfw) as zfw,

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

        from df_egds_fm_clean
        group by acreg_no,	flight_no,	flight_date,	dep_stn,	arr_stn
        """)
      df_dedup.createOrReplaceTempView("df_dedup")
      spark.catalog.cacheTable("df_dedup")
      printMessage("final_EDGS  ", "df_dedup COUNT ",df_dedup.count.toString)
      //println("HELIX final_EDGS df_dedup COUNT "+df_dedup.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))

      // COMMENTED OLEG 29.05
      // TOMORROW RECORDS
      //val df_tomorrow =spark.sql("""
      //    select * from df_dedup
      //    where
      //    (OOOI_FUEL_OUT is not NULL and  OOOI_FUEL_OFF is  NULL and   OOOI_FUEL_ON is  NULL and OOOI_FUEL_IN is null) OR
      //    (OOOI_FUEL_OUT is not NULL and  OOOI_FUEL_OFF is not  NULL and   OOOI_FUEL_ON is  NULL and OOOI_FUEL_IN is null) OR
      //    (OOOI_FUEL_OUT is not NULL and  OOOI_FUEL_OFF is not  NULL and   OOOI_FUEL_ON is  not NULL and OOOI_FUEL_IN is null)
      //                           """)
      //val path_to_folder_tomorrow   =  DateTime.now.toString("yyyy-MM-dd")
      //df_tomorrow.coalesce(1).write.mode(org.apache.spark.sql.SaveMode.Overwrite).option("compression", "snappy").parquet("/data/helix/modelled/emfg/prerequisite/egds/"+path_to_folder_tomorrow+"/step_1/")
      // FINAL DF
      //println("HELIX final_EDGS df_tomorrow COUNT "+df_tomorrow.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      //printMessage("final_EDGS  ", "df_tomorrow COUNT ",df_tomorrow.count.toString)

      // COMMENTED OLEG 29.05
      //val df_final=df_dedup.except(df_tomorrow)
      val df_final=df_dedup
      df_final.cache()
      //println("HELIX final_EDGS df_final COUNT "+df_final.count+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val count=df_final.count
      printMessage("final_EDGS  ", "df_final COUNT ",count.toString)
      if (count == 0) {countZeroExit()}
     // println("HELIX final_EDGS END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
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
      //val df_final = df_egds_fm.drop("df_egds_acreg_no" ,"df_egds_flight_no" , "df_egds_flight_date" ,"df_egds_message_date", "df_egds_dep_stn", "df_egds_arr_stn")
      //df_egds_fm.createOrReplaceTempView("df_final")
      // READ TODAY  RECORDS FROM YESTERDAY
      // COMMENTED OLEG 29.05
      //val path_to_file=path_to_edgs_prerequisite+path_to_folder_date.toString +"/step_1/from_yesterday"
      //var df_final_all=df_egds_fm
      //readDataFrame(path_to_file,"parquet") match {
      //  case Success(df) => {
          //df.cache()
      //    df_final_all=df_egds_fm.union(df)
      //    printMessage("final_EDGS  data from yesterday was found", "")
      //  }
      //  case Failure(ex) => {
      //    printMessage("final_EDGS  data from yesterday was not found", "")
      //  }
      //}
      //val sd = spark.sql("select * from df_final where flight_date='2018-01-01' and flight_no like '%029%' ")
      //printMessage("ASD  COUNT  "+asd.count(), " ")
      df_egds_fm.cache()
      printMessage("final_EDGS  WRITING df_final_all "+df_egds_fm.count, " ")
      df_egds_fm.coalesce(coalesce_vale).write.mode(org.apache.spark.sql.SaveMode.Overwrite).option("compression", compression).parquet(path_to_edgs_prerequisite+path_to_folder_date+"/step_1/data/")
      //df_final.coalesce(1).write.mode(org.apache.spark.sql.SaveMode.Overwrite).option("compression", "snappy").parquet("/data/helix/uat/modelled/emfg/prerequisite/egds_1_oleg/2018-04-16/step_1/data/")
      // println("HELIX writeEDGSprerequisite END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
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


