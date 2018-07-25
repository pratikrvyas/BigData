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

import com.emirates.helix.util._
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.rdd
import org.apache.spark.sql.functions.regexp_replace

import org.apache.spark.sql.DataFrame

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.rdd
import org.apache.spark.sql.functions.regexp_replace

import org.joda.time.format.{DateTimeFormat}
import org.joda.time.format.{DateTimeFormatter}

import org.joda.time.{DateTime, Period}
import org.joda.time.Days

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

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
    * read history data of AGS Refline and perform schema sync.
    */
  class EgdsSyncPrereqProcessor extends SparkUtils {


    var args: EgdsSyncPrereqArguments.Args = _




    def readAndTransform( path_to_edgs_prerequisite:String,
                    path_to_flight_master_current:String,
                    path_to_taxi_fuel_flow:String,
                    path_to_ags_current:String,
                    path_to_altea_current:String,
                    coalesce_value:Int,
                    time_period:Int,
                    number_of_months:Int
                    )(implicit spark: SparkSession): (DataFrame,DataFrame) = {

      readTaxiFuelFlow(path_to_taxi_fuel_flow,"df_taxi_fuel_flow")
      readFlightMaster(path_to_flight_master_current, "fm_ini")

      readAGS(path_to_ags_current:String, "ags")
      readALTEA(path_to_altea_current, "altea")
      readEGDS_3months(path_to_edgs_prerequisite, "egds",number_of_months)

      joinFM_EGDS("fm_ini","egds","df_egds_fm")

      appliedRules_Step2("df_rules_applied")

      val df_consol=consolidationRules_Step2("df_consol")
      val df_final=finalRules_Step3("df_final")
      df_final.cache()
      df_consol.cache()
      println("HELIX df_consol and df_final")
      (df_final,df_consol)
    }

    def readTaxiFuelFlow(path_to_taxi_fuel_flow:String, destination_table_name:String)
                        (implicit spark: SparkSession):Unit = {
      println("HELIX readTaxiFuelFlow START "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val df_taxi_fuel_flow = spark.read.format("csv").option("header", "true").load(path_to_taxi_fuel_flow)
      df_taxi_fuel_flow.createOrReplaceTempView(destination_table_name)
      spark.catalog.cacheTable(destination_table_name)
      println("HELIX readTaxiFuelFlow END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
    }

    def readFlightMaster(path_to_flight_master_current:String, destination_table_name:String)
                        (implicit spark: SparkSession):Unit = {

      println("HELIX readFlightMaster START "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      //val fm = spark.read.parquet(path_to_flight_master_current+"/snapdate="+currentDate+"/*/")
      val path_array=path_to_flight_master_current.split(",")
      val fm=spark.read.parquet(path_array: _*)
      //val fm=spark.read.parquet("/data/helix/modelled/emfg/public/flightmaster/current/snapdate="+currentDate+"/snaptime=00-00/*.parquet")

      fm.createOrReplaceTempView(destination_table_name)
      spark.catalog.cacheTable(destination_table_name)
      println("HELIX readFlightMaster END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
    }

    def readAGS(path_to_ags_current:String, destination_table_name:String)
                (implicit spark: SparkSession):Unit = {
      println("HELIX readAGS BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val ags = spark.read.parquet(path_to_ags_current)
      ags.createOrReplaceTempView(destination_table_name)
      spark.catalog.cacheTable(destination_table_name)
      println("HELIX readAGS END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
    }

    def readALTEA(path_to_altea_current:String, destination_table_name:String)
               (implicit spark: SparkSession):Unit = {
      println("HELIX readALTEA BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val altea_temp = spark.read.parquet(path_to_altea_current)
      altea_temp.createOrReplaceTempView("altea_temp")
      val altea= spark.sql("""
                   select substr(FlightNumber,3,length(FlightNumber)) as FlightNumber,
                   FlightDate, DeparturePort, ArrivalPort, AircraftRegistration,
                   TakeOffFuel  from altea_temp
                   where substr(FlightNumber,1,2)=='EK'
                           """)
      altea.createOrReplaceTempView(destination_table_name)
      spark.catalog.cacheTable(destination_table_name)
      println("HELIX readALTEA END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
    }

    def readEGDS_3months(path_to_edgs_prerequisite:String, destination_table_name:String,number_of_months:Int)
    (implicit spark: SparkSession):Unit = {
      val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      def checkPathExist(path: String): Boolean = {
        val p = new Path(path)
        hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
      }

      val start = DateTime.now.minusDays(1).minusMonths(number_of_months)
      val end   = DateTime.now.minusDays(1)
      val daysCount = Days.daysBetween(start, end).getDays()+1

      val paths= (0 until daysCount).map(days =>end.plusDays(-days).toString("YYYY-MM-dd")).toList.map(path_to_edgs_prerequisite+_+"/step_1/data/")

      //println("HELIX paths "+ paths)

      val filteredPaths = paths.filter(p => checkPathExist(p))

      println("HELIX readEGDS_3months BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      //val egds_step_1 =spark.read.parquet("/data/helix/modelled/emfg/prerequisite/egds/2018-04-21/step_1/data/","/data/helix/modelled/emfg/prerequisite/egds/2018-04-22/step_1/data/","/data/helix/modelled/emfg/prerequisite/egds/2018-04-23/step_1/data/","/data/helix/modelled/emfg/prerequisite/egds/2018-04-24/step_1/data/")
      val egds_step_0 = spark.read.parquet(filteredPaths: _*).toDF()

      val egds_step_1=egds_step_0.dropDuplicates()

      egds_step_1.createOrReplaceTempView("egds_step_1")
      spark.catalog.cacheTable("egds_step_1")
      // DEDUPLICATION
      val egds =spark.sql("""
                  select acreg_no,	flight_no,	flight_date,	dep_stn,	arr_stn,	max(zfw) as zfw,

                  max(tibco_message_timestamp_FUEL_OUT) as tibco_message_timestamp_FUEL_OUT ,
                  max(time_of_message_received_FUEL_OUT) as time_of_message_received_FUEL_OUT,
                  max(msgDeliveredDateTime_FUEL_OUT) as msgDeliveredDateTime_FUEL_OUT,
                  max(act_flight_out_datetime_utc) as act_flight_OUT_datetime_utc,
                  sum(OOOI_FUEL_OUT) as OOOI_FUEL_OUT ,

                  max(tibco_message_timestamp_FUEL_OFF) as tibco_message_timestamp_FUEL_OFF,
                  max(time_of_message_received_FUEL_OFF) as time_of_message_received_FUEL_OFF,
                  max(msgDeliveredDateTime_FUEL_OFF) as msgDeliveredDateTime_FUEL_OFF,
                  max(act_flight_off_datetime_utc) as act_flight_OFF_datetime_utc,
                  sum(OOOI_FUEL_OFF) as OOOI_FUEL_OFF,

                  max(tibco_message_timestamp_FUEL_ON) as tibco_message_timestamp_FUEL_ON,
                  max(time_of_message_received_FUEL_ON) as time_of_message_received_FUEL_ON,
                  max(msgDeliveredDateTime_FUEL_ON) as msgDeliveredDateTime_FUEL_ON,
                  max(act_flight_on_datetime_utc) as act_flight_ON_datetime_utc,
                  sum(OOOI_FUEL_ON) as OOOI_FUEL_ON,

                  max(tibco_message_timestamp_FUEL_IN) as tibco_message_timestamp_FUEL_IN,
                  max(time_of_message_received_FUEL_IN) as time_of_message_received_FUEL_IN,
                  max(msgDeliveredDateTime_FUEL_IN) as msgDeliveredDateTime_FUEL_IN,
                  max(act_flight_in_datetime_utc) as act_flight_IN_datetime_utc,
                  sum(OOOI_FUEL_IN) as OOOI_FUEL_IN

                  from egds_step_1
                  group by acreg_no,	flight_no,	flight_date,	dep_stn,	arr_stn
                    """)
      egds.createOrReplaceTempView(destination_table_name)
      spark.catalog.cacheTable(destination_table_name)
      println("HELIX readEGDS_3months END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
    }

    def joinFM_EGDS(fm_sql_source:String,egds_sql_source:String, destination_table_name:String)
                     (implicit spark: SparkSession):Unit = {
      println("HELIX joinFM_EGDS BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))

      val flight_days = spark.sql("select distinct flight_date from "+egds_sql_source+" ")
      flight_days.cache
      flight_days.createOrReplaceTempView("df_flight_days")
      val fm = spark.sql("""
                        select * from fm_ini
                        where flight_date in (select flight_date from  df_flight_days)
                         """)
      fm.cache()
      fm.createOrReplaceTempView("fm")

      val df_egds_fm=spark.sql("""
                select fm.actual_reg_number as acreg_no, fm.flight_number as flight_no ,
                fm.flight_date as flight_date,
                fm.act_dep_iata_station as dep_stn ,fm.act_arr_iata_station as arr_stn,
                fm.aircraft_subtype as aircraft_subtype, egds.zfw,

                airline_code, flight_number,flight_suffix,sch_dep_datetime_local,
                pln_dep_iata_station,latest_pln_arr_iata_station,actual_reg_number,

                to_date(fm.act_flight_off_datetime_utc) as act_flight_off_date_utc ,

                egds.tibco_message_timestamp_FUEL_OUT, egds.time_of_message_received_FUEL_OUT,
                egds.msgDeliveredDateTime_FUEL_OUT, fm.act_flight_OUT_datetime_utc, egds.OOOI_FUEL_OUT,

                egds.tibco_message_timestamp_FUEL_OFF, egds.time_of_message_received_FUEL_OFF,
                egds.msgDeliveredDateTime_FUEL_OFF, fm.act_flight_OFF_datetime_utc, egds.OOOI_FUEL_OFF,

                egds.tibco_message_timestamp_FUEL_ON, egds.time_of_message_received_FUEL_ON,
                egds.msgDeliveredDateTime_FUEL_ON, fm.act_flight_ON_datetime_utc, egds.OOOI_FUEL_ON,

                egds.tibco_message_timestamp_FUEL_IN, egds.time_of_message_received_FUEL_IN,
                egds.msgDeliveredDateTime_FUEL_IN, fm.act_flight_IN_datetime_utc, egds.OOOI_FUEL_IN
                from fm
                left outer join """ +egds_sql_source+"""
                on fm.actual_reg_number=egds.acreg_no
                and fm.flight_number = egds.flight_no
                and fm.flight_date=egds.flight_date
                and fm.act_dep_iata_station = egds.dep_stn  and fm.act_arr_iata_station = egds.arr_stn
                where
                (fm.act_flight_OUT_datetime_utc is not null and fm.act_flight_OFF_datetime_utc is not null
                and fm.act_flight_ON_datetime_utc is not null and fm.act_flight_IN_datetime_utc is not null)
                """)
      df_egds_fm.createOrReplaceTempView(destination_table_name)
      spark.catalog.cacheTable(destination_table_name)
      println("HELIX joinFM_EGDS END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))

    }

    def appliedRules_Step2(destination_table_name:String)
                          (implicit spark: SparkSession):Unit=
    {
      println("HELIX appliedRules_Step2 BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val df_rules_applied =spark.sql("""
          select acreg_no, flight_no, flight_date, dep_stn, arr_stn,zfw, aircraft_subtype,

           OOOI_FUEL_OUT,OOOI_FUEL_OFF,OOOI_FUEL_ON,OOOI_FUEL_IN,
           act_flight_OFF_datetime_utc, act_flight_OUT_datetime_utc,  act_flight_ON_datetime_utc, act_flight_IN_datetime_utc,

           pln_dep_iata_station, latest_pln_arr_iata_station,

           tibco_message_timestamp_FUEL_OUT, time_of_message_received_FUEL_OUT, msgDeliveredDateTime_FUEL_OUT,

           tibco_message_timestamp_FUEL_OFF, time_of_message_received_FUEL_OFF, msgDeliveredDateTime_FUEL_OFF,

           tibco_message_timestamp_FUEL_ON, time_of_message_received_FUEL_ON, msgDeliveredDateTime_FUEL_ON,

           tibco_message_timestamp_FUEL_IN,time_of_message_received_FUEL_IN, msgDeliveredDateTime_FUEL_IN,

          CASE WHEN DRVD_FUEL_OUT < 0 OR DRVD_FUEL_OUT==0
            THEN NULL
            ELSE DRVD_FUEL_OUT
            END AS DRVD_FUEL_OUT,

            CASE WHEN DRVD_FUEL_OFF < 0 OR DRVD_FUEL_OFF==0
            THEN NULL
            ELSE DRVD_FUEL_OFF
            END AS DRVD_FUEL_OFF,

            CASE WHEN DRVD_FUEL_ON < 0 OR DRVD_FUEL_ON==0
            THEN NULL
            ELSE DRVD_FUEL_ON
            END AS DRVD_FUEL_ON,

            CASE WHEN DRVD_FUEL_IN < 0 OR DRVD_FUEL_IN==0
            THEN NULL
            ELSE DRVD_FUEL_IN
            END AS DRVD_FUEL_IN,

          ags_FUEL_OFF,
          ags_FUEL_ON,
          TakeOffFuel,
          taxi_fuel_flow,
          taxi_out_minutes

          from (
           select acreg_no, flight_no, flight_date, dep_stn, arr_stn,df_egds_fm.zfw, df_egds_fm.aircraft_subtype,

           df_egds_fm.OOOI_FUEL_OUT,df_egds_fm.OOOI_FUEL_OFF,df_egds_fm.OOOI_FUEL_ON,df_egds_fm.OOOI_FUEL_IN,
           act_flight_OFF_datetime_utc, act_flight_OUT_datetime_utc,  act_flight_ON_datetime_utc, act_flight_IN_datetime_utc,

           pln_dep_iata_station, latest_pln_arr_iata_station,

           tibco_message_timestamp_FUEL_OUT, time_of_message_received_FUEL_OUT, msgDeliveredDateTime_FUEL_OUT,

           tibco_message_timestamp_FUEL_OFF, time_of_message_received_FUEL_OFF, msgDeliveredDateTime_FUEL_OFF,

           tibco_message_timestamp_FUEL_ON, time_of_message_received_FUEL_ON, msgDeliveredDateTime_FUEL_ON,

           tibco_message_timestamp_FUEL_IN,time_of_message_received_FUEL_IN, msgDeliveredDateTime_FUEL_IN,

           CASE WHEN df_egds_fm.OOOI_FUEL_OUT is NULL AND TakeOffFuel is not NULL
                    THEN TakeOffFuel+(unix_timestamp(act_flight_OFF_datetime_utc)-unix_timestamp(act_flight_OUT_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow
                   ELSE
                   df_egds_fm.OOOI_FUEL_OUT
                   END AS DRVD_FUEL_OUT,

          CASE WHEN df_egds_fm.OOOI_FUEL_OFF is NULL AND ags.FUEL_OFF is not NULL THEN ags.FUEL_OFF
                   ELSE
                     CASE WHEN df_egds_fm.OOOI_FUEL_OFF is NULL AND ags.FUEL_OFF is  NULL   THEN TakeOffFuel  ELSE df_egds_fm.OOOI_FUEL_OFF END
                   END AS DRVD_FUEL_OFF,

          CASE WHEN   df_egds_fm.OOOI_FUEL_ON is NULL AND ags.FUEL_ON is not NULL THEN ags.FUEL_ON
               WHEN   df_egds_fm.OOOI_FUEL_ON is NULL AND ags.FUEL_ON is  NULL AND df_egds_fm.OOOI_FUEL_IN IS NOT NULL AND ((unix_timestamp(act_flight_IN_datetime_utc)-unix_timestamp(act_flight_ON_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow) >0
                THEN
               df_egds_fm.OOOI_FUEL_IN + (unix_timestamp(act_flight_IN_datetime_utc)-unix_timestamp(act_flight_ON_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow
               ELSE   df_egds_fm.OOOI_FUEL_ON
               END AS  DRVD_FUEL_ON,

           CASE   WHEN  df_egds_fm.OOOI_FUEL_IN is NULL AND df_egds_fm.OOOI_FUEL_ON is not NULL AND
           ((unix_timestamp(act_flight_IN_datetime_utc)-unix_timestamp(act_flight_ON_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow) >0
                   THEN df_egds_fm.OOOI_FUEL_ON - (unix_timestamp(act_flight_IN_datetime_utc)-unix_timestamp(act_flight_ON_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow
                   WHEN df_egds_fm.OOOI_FUEL_IN is NULL AND ags.FUEL_ON is not NULL AND
                   ((unix_timestamp(act_flight_IN_datetime_utc)-unix_timestamp(act_flight_ON_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow) >0
                   THEN ags.FUEL_ON - (unix_timestamp(act_flight_IN_datetime_utc)-unix_timestamp(act_flight_ON_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow
                   ELSE
                   df_egds_fm.OOOI_FUEL_IN
                   END AS DRVD_FUEL_IN,

          ags.FUEL_OFF as ags_FUEL_OFF,
          ags.FUEL_ON as ags_FUEL_ON,
          TakeOffFuel,
          df_taxi_fuel_flow.taxi_fuel_flow as taxi_fuel_flow,
          (unix_timestamp(act_flight_OFF_datetime_utc)-unix_timestamp(act_flight_OUT_datetime_utc))/60 as taxi_out_minutes

          from df_egds_fm
          join df_taxi_fuel_flow on
          df_egds_fm.aircraft_subtype=df_taxi_fuel_flow.aircraft_subtype

          left outer join altea on
          trim(altea.FlightNumber) = df_egds_fm.flight_number
          and altea.FlightDate = (substr(df_egds_fm.sch_dep_datetime_local,1,10))
          and altea.DeparturePort = df_egds_fm.pln_dep_iata_station and altea.ArrivalPort = df_egds_fm.latest_pln_arr_iata_station
          and altea.AircraftRegistration = df_egds_fm.actual_reg_number and df_egds_fm.airline_code ='EK'

           left outer  join ags on
           df_egds_fm.flight_no=ags.FlightNumber
           and df_egds_fm.acreg_no=ags.Tail
           and df_egds_fm.act_flight_off_date_utc=to_date(ags.FltDate)
           and df_egds_fm.dep_stn=ags.Origin and df_egds_fm.arr_stn = ags.Destination) A
           """)

      df_rules_applied.createOrReplaceTempView(destination_table_name)
      spark.catalog.cacheTable(destination_table_name)
      println("HELIX appliedRules_Step2 END "+destination_table_name+" "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      //println("HELIX appliedRules_Step2 "+df_rules_applied.printSchema)

    }

    def consolidationRules_Step2(destination_table_name:String)
                          (implicit spark: SparkSession):DataFrame={
      println("HELIX consolidationRules_Step2 BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val df_consol = spark.sql("""
          select df_rules_applied.*,

          CASE WHEN DRVD_FUEL_IN IS NOT NULL AND DRVD_FUEL_ON IS NOT NULL AND DRVD_FUEL_IN >= DRVD_FUEL_ON
              AND ((unix_timestamp(act_flight_IN_datetime_utc)-unix_timestamp(act_flight_ON_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow)>0
            THEN
                DRVD_FUEL_ON - (unix_timestamp(act_flight_IN_datetime_utc)-unix_timestamp(act_flight_ON_datetime_utc))/60 *
                    df_taxi_fuel_flow.taxi_fuel_flow
                ELSE
                  CASE WHEN DRVD_FUEL_IN > DRVD_FUEL_ON THEN  NULL END
          END AS  DRVD_FUEL_IN_CONSOL

          from df_rules_applied
          join df_taxi_fuel_flow on
          df_rules_applied.aircraft_subtype=df_taxi_fuel_flow.aircraft_subtype
          """)

      df_consol.createOrReplaceTempView(destination_table_name)
      println("HELIX consolidationRules_Step2 END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      df_consol

    }

    def finalRules_Step3(destination_table_name:String)
                        (implicit spark: SparkSession):DataFrame={
      println("HELIX finalRules_Step3 BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val df_final_full = spark.sql("""
            select B.*,
            CURRENT_TIMESTAMP as processing_date
            FROM
            (select A.*,
            CASE
              WHEN A.FF_OUT_ACTUAL IS NOT NULL and A.FF_PLAN IS NOT NULL AND (A.FF_OUT_ACTUAL < A.FF_PLAN * 0.5 OR A.FF_OUT_ACTUAL> 1.5*A.FF_PLAN)
                THEN
                  CASE
                    WHEN ((unix_timestamp(act_flight_OFF_datetime_utc)  - unix_timestamp(act_flight_OUT_datetime_utc))/60)*FF_PLAN  > 0
                      THEN
                        DRVD_FUEL_OUT - ((unix_timestamp(act_flight_OFF_datetime_utc)  - unix_timestamp(act_flight_OUT_datetime_utc))/60)*FF_PLAN
                    ELSE A.DRVD_FUEL_OFF
                    END
            ELSE
            A.DRVD_FUEL_OFF
            END AS FUEL_OFF,

            CASE
              WHEN A.FF_IN_ACTUAL IS NOT NULL and A.FF_PLAN IS NOT NULL AND (A.FF_IN_ACTUAL < A.FF_PLAN * 0.5 OR A.FF_IN_ACTUAL> 1.5*A.FF_PLAN)
                  THEN
                      CASE
                        WHEN ((unix_timestamp(act_flight_IN_datetime_utc)  - unix_timestamp(act_flight_ON_datetime_utc))/60)*FF_PLAN >0
                          THEN
                            NVL(DRVD_FUEL_IN_CONSOL,DRVD_FUEL_IN) + ((unix_timestamp(act_flight_IN_datetime_utc)   - unix_timestamp(act_flight_ON_datetime_utc))/60)*FF_PLAN
                        ELSE A.DRVD_FUEL_ON
                        END
              ELSE
                A.DRVD_FUEL_ON
            END AS FUEL_ON

            from
            (select df_consol.*,

            case when taxi_fuel_flow is not null THEN
            taxi_fuel_flow
            ELSE
            NULL
            END AS FF_PLAN,

            case when DRVD_FUEL_OUT is not null and DRVD_FUEL_OFF is not null AND act_flight_OFF_datetime_utc is not NULL AND act_flight_OUT_datetime_utc is not NULL
            THEN
            ROUND((DRVD_FUEL_OUT - DRVD_FUEL_OFF) / ((unix_timestamp(act_flight_OFF_datetime_utc)               - unix_timestamp(act_flight_OUT_datetime_utc))/60),0)
            ELSE
            NULL
            END AS FF_OUT_ACTUAL,

            case when NVL(DRVD_FUEL_IN_CONSOL,DRVD_FUEL_IN) is not null and DRVD_FUEL_ON is not null AND act_flight_IN_datetime_utc is not NULL AND act_flight_ON_datetime_utc is not NULL
            THEN
            ROUND((DRVD_FUEL_ON - NVL(DRVD_FUEL_IN_CONSOL,DRVD_FUEL_IN) / (unix_timestamp(act_flight_IN_datetime_utc) - unix_timestamp(act_flight_ON_datetime_utc))/60),0)

            ELSE
            NULL
            END AS FF_IN_ACTUAL

            from df_consol

            ) A ) B

            """)

      df_final_full.createOrReplaceTempView("df_final_full")
      df_final_full.createOrReplaceTempView("df_final_full")
      spark.catalog.cacheTable("df_final_full")
      println("HELIX "+ df_final_full.printSchema)
      // FINAL STEP 3 TO WRITE TO PARQUET
      val df_final=spark.sql("""
              select acreg_no,
              flight_no,
              flight_date,
              dep_stn,
              arr_stn,
              zfw,
              tibco_message_timestamp_FUEL_OUT,
              time_of_message_received_FUEL_OUT,
              msgDeliveredDateTime_FUEL_OUT,
              OOOI_FUEL_OUT,
              tibco_message_timestamp_FUEL_OFF,
              time_of_message_received_FUEL_OFF,
              msgDeliveredDateTime_FUEL_OFF,
              OOOI_FUEL_OFF,
              tibco_message_timestamp_FUEL_ON,
              time_of_message_received_FUEL_ON,
              msgDeliveredDateTime_FUEL_ON,
              OOOI_FUEL_ON,
              tibco_message_timestamp_FUEL_IN,
              time_of_message_received_FUEL_IN,
              msgDeliveredDateTime_FUEL_IN,
              OOOI_FUEL_IN,

              CASE WHEN round(DRVD_FUEL_OUT,0) < 0 OR round(DRVD_FUEL_OUT,0) == 0 THEN NULL ELSE round(DRVD_FUEL_OUT,0) END as DRVD_FUEL_OUT ,

              CASE WHEN round(DRVD_FUEL_OFF,0) < 0 OR round(DRVD_FUEL_OFF,0) == 0 THEN NULL ELSE round(DRVD_FUEL_OFF,0) END as DRVD_FUEL_OFF ,

              CASE WHEN round(DRVD_FUEL_ON,0) < 0 OR round(DRVD_FUEL_ON,0) == 0 THEN NULL ELSE round(DRVD_FUEL_ON,0) END as DRVD_FUEL_ON ,

              CASE WHEN round(DRVD_FUEL_IN,0) < 0 OR round(DRVD_FUEL_IN,0) == 0 THEN NULL ELSE round(DRVD_FUEL_IN,0) END as DRVD_FUEL_IN ,

              CASE WHEN round(DRVD_FUEL_OUT,0) < 0 OR round(DRVD_FUEL_OUT,0) == 0 THEN NULL ELSE round(DRVD_FUEL_OUT,0) END as FUEL_OUT ,

              CASE WHEN round(FUEL_OFF,0) < 0 OR round(FUEL_OFF,0) == 0 THEN NULL ELSE round(FUEL_OFF,0) END as FUEL_OFF ,

              CASE WHEN round(FUEL_ON,0) < 0 OR round(FUEL_ON,0) == 0 THEN NULL ELSE round(FUEL_ON,0) END as FUEL_ON ,

              CASE WHEN round(NVL(DRVD_FUEL_IN_CONSOL,DRVD_FUEL_IN),0) < 0 OR round(NVL(DRVD_FUEL_IN_CONSOL,DRVD_FUEL_IN),0) == 0 THEN NULL ELSE round(NVL(DRVD_FUEL_IN_CONSOL,DRVD_FUEL_IN),0) END as FUEL_IN ,

              processing_date

              from df_final_full
              """)

      println("HELIX finalRules_Step3 END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      df_final
    }

    def writeEGDS_Step3(df_final:DataFrame,path_to_edgs_prerequisite:String,coalesce_vale:Int,compression:String)
                             (implicit spark: SparkSession):Unit ={
      println("HELIX writeEGDS_Step3 BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val path_to_folder_yesterday   =  DateTime.now.minusDays(1).toString("yyyy-MM-dd")
      df_final.coalesce(coalesce_vale).write.mode(org.apache.spark.sql.SaveMode.Overwrite).option("compression", "snappy").parquet(path_to_edgs_prerequisite+path_to_folder_yesterday+"/step_3/data/")
      println("HELIX writeEGDS_Step3 END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
    }

    def writeEGDS_Step2(df_consol:DataFrame,path_to_edgs_prerequisite:String,
                        coalesce_vale:Int,compression:String)
                       (implicit spark: SparkSession):Unit = {
      println("HELIX writeEGDS_Step2 BEGIN "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
      val path_to_folder_yesterday   =  DateTime.now.minusDays(1).toString("yyyy-MM-dd")
      df_consol.coalesce(coalesce_vale).write.mode(org.apache.spark.sql.SaveMode.Overwrite).option("compression", "snappy").parquet(path_to_edgs_prerequisite+path_to_folder_yesterday+"/step_2/data")
      println("HELIX writeEGDS_Step2 END "+DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(DateTime.now()))
    }

  }
}
