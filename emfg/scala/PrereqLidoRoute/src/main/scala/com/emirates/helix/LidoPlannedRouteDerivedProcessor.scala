/*----------------------------------------------------------
 * Created on  : 18/03/2018
 * Author      : Manu Mukundan(S795217), Ravindra Kumar(S794645)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : LidoPlannedRouteDerivedProcessor.scala
 * Description : Secondary class file for creating and writing derived parameters for lido route level
 * ----------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.model.LidoRouteModel._
import com.emirates.helix.util.SparkUtils
import com.emirates.helix.udf._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DecimalType

/**
  * Companion object for LidoPlannedRouteDerivedProcessor class
  */
object LidoPlannedRouteDerivedProcessor {
  /**
    * MultiSourceR2DProcessor instance creation
    *
    * @param args user parameter instance
    * @return MultiSourceR2DProcessor instance
    */
  def apply(args: LidoRouteArgs): LidoPlannedRouteDerivedProcessor = {
    var processor = new LidoPlannedRouteDerivedProcessor()
    processor.args = args
    processor
  }
}

/**
  * LidoPlannedRouteDerivedProcessor class with methods to read, process and write the data
  */
class LidoPlannedRouteDerivedProcessor  extends SparkUtils {
  private var args: LidoRouteArgs = _

  /** Method which does the processing
    *
    * @param in_df Dataframe holding input data
    * @return DataFrame Spark dataframe for processed R2D data
    */
  def processData(in_df: DataFrame, master_df: DataFrame, arinc_runway: DataFrame, arinc_airport: DataFrame, flight_mstr_df: DataFrame)(implicit hiveContext: HiveContext, sc: SparkContext): DataFrame = {
    import hiveContext.implicits._

    val window1 = Window.partitionBy($"FliNum", $"DepAirIat", $"DatOfOriUtc", $"DesAirIat", $"FliDupNo", $"tibco_message_time", $"RegOrAirFinNum").orderBy($"waypoint_seq_no".cast("Int"))
    val window2 = Window.partitionBy($"FliNum", $"DepAirIat", $"DatOfOriUtc", $"DesAirIat", $"FliDupNo", $"tibco_message_time", $"RegOrAirFinNum")

    val route_level_df = in_df.select($"AirDes",$"CosIndEcoCru",$"DepRun",$"ArrRun",$"FliNum",$"DepAirIat",$"DatOfOriUtc",
      $"DesAirIat",$"RegOrAirFinNum",$"IatAirTyp",$"OfpNum",$"ConFue",$"NoOfWayOfMaiRouDepDes",$"DepAirIca",$"DesAirIca",$"FliDupNo",
      $"PlnZfw",$"tibco_message_time",$"RouOptCri",$"helix_uuid", $"helix_timestamp", explode($"WayPoints").as("WayPoints"))

    val route_level_flat_df = route_level_df.select($"AirDes",$"CosIndEcoCru",$"PlnZfw",$"DepRun",$"ArrRun",$"FliNum",$"DepAirIat",
    $"DatOfOriUtc",$"DesAirIat",$"RegOrAirFinNum",$"IatAirTyp",$"OfpNum",$"ConFue",$"NoOfWayOfMaiRouDepDes",$"DepAirIca",$"DesAirIca",
    $"FliDupNo",$"WayPoints.altitude",$"WayPoints.dstToNxtWaypnt",$"WayPoints.estElpTimeOvrWaypnt",$"WayPoints.estFltTimeToNxtWaypnt",
    $"WayPoints.estFuelOnBrdOvrWaypnt",$"WayPoints.estTotFuelBurnOvrWaypnt",$"WayPoints.moraMea",$"WayPoints.outbndTrueTrk",$"WayPoints.SegMachNum",
    $"WayPoints.SegTemp",$"WayPoints.segWndCmptCrs",$"WayPoints.segWndCmptTrk",$"WayPoints.waypntLat",$"WayPoints.waypntLon",$"WayPoints.waypointName",$"tibco_message_time",$"RouOptCri",$"helix_uuid", $"helix_timestamp")
      .withColumn("in_flight_status",when(trim($"RouOptCri") === lit("INFLT"), "Y").otherwise(lit("")))


    val machno_master = master_df.withColumn("COST_INDEX", regexp_replace($"COST_INDEX", ",", ""))
      .withColumn("WIND_COMP", regexp_replace($"WIND_COMP", ",", ""))
      .withColumn("ALTITUDE", regexp_replace($"ALTITUDE", ",", ""))
      .withColumn("WEIGHT", regexp_replace($"WEIGHT", ",", ""))
    val metadata = machno_master.rdd.map(x => Row(cost_index = x.getAs[String]("COST_INDEX").toInt,
      wind_comp = x.getAs[String]("WIND_COMP").toInt,
      altitude = x.getAs[String]("ALTITUDE").toInt,
      weight = x.getAs[String]("WEIGHT").toInt,
      mach_no = x.getAs[String]("MACH_NO"),
      aircraft_type = x.getAs[String]("AIRCRAFT_TYPE")))
      .collect

    val broadcast_var = sc.broadcast(metadata)

    //=====================================================
    //	WAYPOINT SEQUENCE NO
    //=====================================================

    route_level_flat_df.registerTempTable("route_level_flat_tbl")

    val rank_df = hiveContext.sql("SELECT *,RANK() OVER (PARTITION BY cast(trim(FliDupNo) as int),trim(FliNum),trim(DatOfOriUtc),trim(DepAirIat),trim(DesAirIat),trim(RegOrAirFinNum),trim(tibco_message_time) ORDER BY estFuelOnBrdOvrWaypnt DESC) as waypoint_seq_no FROM route_level_flat_tbl")

    rank_df.registerTempTable("rank_tbl")

    val final_rank_df = hiveContext.sql("select *,row_number() over (partition by cast(trim(FliDupNo) as int),trim(FliNum),trim(DatOfOriUtc),trim(DepAirIat),trim(DesAirIat),trim(RegOrAirFinNum),trim(tibco_message_time),waypoint_seq_no ORDER BY waypoint_seq_no) as rno from rank_tbl")

    val upd_waypnt_seq_no = final_rank_df.withColumn("waypoint_seq_no1",when($"rno" >= 2, $"waypoint_seq_no" + 0.5) otherwise ($"waypoint_seq_no"))

    upd_waypnt_seq_no.registerTempTable("upd_waypnt_seq_no")

    val waypoint_seq_no_df = hiveContext.sql("SELECT *,RANK() OVER (PARTITION BY cast(trim(FliDupNo) as int),trim(FliNum),trim(DatOfOriUtc),trim(DepAirIat),trim(DesAirIat),trim(RegOrAirFinNum),trim(tibco_message_time) ORDER BY waypoint_seq_no1) as waypoint_seq_no2 FROM upd_waypnt_seq_no").withColumn("waypoint_seq_no",$"waypoint_seq_no2").drop($"waypoint_seq_no1")

    //======================================================
    //	TRACKWINDSEGMENT_DRVD
    //======================================================

    waypoint_seq_no_df.registerTempTable("temp1")

    //hiveContext.cacheTable("temp1")

    val TrackWindSegment_drvd_df = hiveContext.sql("SELECT *, CASE WHEN substring(trim(segWndCmptTrk), 1, 1) = '-' THEN concat('M',lpad(substring(trim(segWndCmptTrk),2,3),3,'0')) WHEN substring(trim(segWndCmptTrk), 1, 1) = '+' THEN concat('P',lpad(substring(trim(segWndCmptTrk),2,3),3,'0')) end TrackWindSegment_drvd from temp1")

    //=====================================================
    //	MORA_OR_MEA_IN_FEET_DRVD
    //=====================================================

    val mora_or_mea_in_feet_drvd_df = TrackWindSegment_drvd_df.withColumn("mora_or_mea_drvd", round(($"moraMea" * 3.2808) / 100))

    //=====================================================
    //	UPDATE WAYPOINTNAME
    //=====================================================
    mora_or_mea_in_feet_drvd_df.registerTempTable("temp2")
    val waypointName_update_df = hiveContext.sql("select in_flight_status,RouOptCri,AirDes,CosIndEcoCru,PlnZfw,DepRun, ArrRun, FliNum, DepAirIat, DatOfOriUtc, DesAirIat, RegOrAirFinNum, IatAirTyp, OfpNum, ConFue, NoOfWayOfMaiRouDepDes, DepAirIca, DesAirIca, FliDupNo, altitude, dstToNxtWaypnt, estElpTimeOvrWaypnt, estFltTimeToNxtWaypnt, estFuelOnBrdOvrWaypnt, estTotFuelBurnOvrWaypnt, moraMea, outbndTrueTrk, SegMachNum, SegTemp, segWndCmptCrs, segWndCmptTrk, waypntLat, waypntLon, waypoint_seq_no, TrackWindSegment_drvd, mora_or_mea_drvd, case when waypoint_seq_no = 1 then concat(trim(waypointName),'/',DepRun) when waypoint_seq_no = cast(trim(NoOfWayOfMaiRouDepDes) as int) then concat(trim(waypointName),'/',ArrRun) else trim(waypointName) end waypointName,tibco_message_time,helix_uuid,helix_timestamp from temp2").where($"in_flight_status" === "")


    //=====================================================
    //FIDING ELEVATION TO NEW COLUMN ALTITUDE_DRVD
    //======================================================
    arinc_runway.registerTempTable("arinc_runway_input_tbl")
    arinc_airport.registerTempTable("arinc_airport_input_tbl")
    waypointName_update_df.registerTempTable("temp3")

    val dep_elev_drvd_df = hiveContext.sql("select t1.*,t2.ThrElev as dep_runway_elev,t3.elev as dep_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DepAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.DepRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DepAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))) >= to_date(trim(t2.cycle_start_date)) and to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))) <= to_date(trim(t2.cycle_end_date))")

    val dep_elev_drvd1_df = if(dep_elev_drvd_df.count == 0) hiveContext.sql("select t1.*,t2.ThrElev as dep_runway_elev,t3.elev as dep_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DepAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.DepRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DepAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-28) >= to_date(trim(t2.cycle_start_date)) and date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-28) <= to_date(trim(t2.cycle_end_date))") else if(dep_elev_drvd_df.count < waypointName_update_df.count) hiveContext.sql("select t1.*,t2.ThrElev as dep_runway_elev,t3.elev as dep_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DepAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.DepRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DepAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-28) >= to_date(trim(t2.cycle_start_date)) and date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-28) <= to_date(trim(t2.cycle_end_date))") else dep_elev_drvd_df

    val dep_elev_drvd2_df = if(dep_elev_drvd1_df.count == 0) hiveContext.sql("select t1.*,t2.ThrElev as dep_runway_elev,t3.elev as dep_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DepAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.DepRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DepAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-56) >= to_date(trim(t2.cycle_start_date)) and date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-56) <= to_date(trim(t2.cycle_end_date))") else if(dep_elev_drvd1_df.count < waypointName_update_df.count) hiveContext.sql("select t1.*,t2.ThrElev as dep_runway_elev,t3.elev as dep_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DepAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.DepRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DepAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-56) >= to_date(trim(t2.cycle_start_date)) and date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-56) <= to_date(trim(t2.cycle_end_date))") else dep_elev_drvd1_df

    // 77777777777
    val arr_elev_drvd_df = hiveContext.sql("select t1.*,t2.ThrElev as arr_runway_elev,t3.elev as arr_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DesAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.ArrRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DesAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))) >= to_date(trim(t2.cycle_start_date)) and to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))) <= to_date(trim(t2.cycle_end_date))")

    val arr_elev_drvd1_df = if(arr_elev_drvd_df.count == 0) hiveContext.sql("select t1.*,t2.ThrElev as arr_runway_elev,t3.elev as arr_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DesAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.ArrRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DesAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-28) >= to_date(trim(t2.cycle_start_date)) and date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-28) <= to_date(trim(t2.cycle_end_date))") else if(arr_elev_drvd_df.count < waypointName_update_df.count) hiveContext.sql("select t1.*,t2.ThrElev as arr_runway_elev,t3.elev as arr_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DesAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.ArrRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DesAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-28) >= to_date(trim(t2.cycle_start_date)) and date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-28) <= to_date(trim(t2.cycle_end_date))") else arr_elev_drvd_df

    val arr_elev_drvd2_df = if(arr_elev_drvd1_df.count == 0) hiveContext.sql("select t1.*,t2.ThrElev as arr_runway_elev,t3.elev as arr_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DesAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.ArrRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DesAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-56) >= to_date(trim(t2.cycle_start_date)) and date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-56) <= to_date(trim(t2.cycle_end_date))") else if(arr_elev_drvd1_df.count < waypointName_update_df.count) hiveContext.sql("select t1.*,t2.ThrElev as arr_runway_elev,t3.elev as arr_airport_elev from temp3 t1 left outer join arinc_runway_input_tbl t2 on trim(t1.DesAirIca) = regexp_replace(trim(t2.Port),'\"','') and trim(t1.ArrRun) = substr(regexp_replace(trim(Ident),'\"',''),3) left outer join arinc_airport_input_tbl t3 on trim(t1.DesAirIca) = regexp_replace(trim(t3.port),'\"','') and to_date(trim(t2.cycle_start_date)) = to_date(trim(t3.cycle_start_date)) where date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-56) >= to_date(trim(t2.cycle_start_date)) and date_add(to_date(from_unixtime(unix_timestamp(trim(DatOfOriUtc),'ddMMMyyyy'))),-56) <= to_date(trim(t2.cycle_end_date))") else arr_elev_drvd1_df

    //77777777777777


    //1500,100 should be configured
    dep_elev_drvd2_df.registerTempTable("temp4")
    arr_elev_drvd2_df.registerTempTable("temp5")
    //hiveContext.cacheTable("temp4")
    //hiveContext.cacheTable("temp5")
    val altitude_drvd = hiveContext.sql("select t1.*,t2.arr_runway_elev,t2.arr_airport_elev,case when t1.waypoint_seq_no = 1 then 1500 + coalesce(t1.dep_runway_elev,t1.dep_airport_elev,0) when t1.waypoint_seq_no = cast(trim(t1.NoOfWayOfMaiRouDepDes) as int) then 1500 + coalesce(t2.arr_runway_elev,t2.arr_airport_elev,0) when (t1.waypoint_seq_no > 1 and t1.waypoint_seq_no < cast(trim(t1.NoOfWayOfMaiRouDepDes) as int)) and (trim(t1.altitude) != 'CLB' and trim(t1.altitude) != 'DSC') then cast(trim(t1.altitude) as int) * 100 else trim(t1.altitude) end altitude_drvd from temp4 t1 left outer join temp5 t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time and t1.waypoint_seq_no = t2.waypoint_seq_no").dropDuplicates()


    //===================================================
    //	LATITUDE DERIVED COL AND LONGTITUDE DERIVED COL
    //===================================================
    altitude_drvd.registerTempTable("altitude_drvd_tbl")

    val latlong_drvd = hiveContext.sql("select *,case when substring(trim(waypntLat),-1,1) = 'N' then round(cast(substring(trim(waypntLat),1,4) as int) + cast(substring(trim(waypntLat),5,2) as int)/60 , 1) else round(-1*(cast(substring(trim(waypntLat),1,4) as int) + cast(substring(trim(waypntLat),5,2) as int)/60) , 1) end latitude_drvd,case when substring(trim(waypntLon),-1,1) = 'E' then round(cast(substring(trim(waypntLon),1,5) as int) + cast(substring(trim(waypntLon),6,2) as int)/60 , 1) else round(-1*(cast(substring(trim(waypntLon),1,5) as int) + cast(substring(trim(waypntLon),6,2) as int)/60) , 1) end longitude_drvd  from altitude_drvd_tbl")

    //=====================================================
    //	ACCUMULATED GROUND DISTANCE LOGIC
    //=====================================================
    val dstToNxtWaypnt_drvd = latlong_drvd.withColumn("dstToNxtWaypnt_drvd", lag($"dstToNxtWaypnt", 1, "0").over(window1)).withColumn("mora_or_mea_drvd", lag($"mora_or_mea_drvd", 1, null).over(window1)).withColumn("estFltTimeToNxtWaypnt_drvd", lag($"estFltTimeToNxtWaypnt", 1, null).over(window1))
    val accum_distance_drvd = dstToNxtWaypnt_drvd.withColumn("accum_distance_drvd", (sum(trim($"dstToNxtWaypnt_drvd").cast("Int")).over(window1)).cast("String"))

    //=====================================================
    // REMAINING FUEL
    //=====================================================

    val remain_fuel_drvd = accum_distance_drvd.withColumn("remaining_fuel_drvd",trim($"estFuelOnBrdOvrWaypnt").as[Int] - trim($"ConFue").as[Int])

    //=====================================================
    // ALTITUDE_DRVD_UPDATE FOR CLB - JOIN WITH remain_fuel_tbl
    //=====================================================
    remain_fuel_drvd.registerTempTable("remain_fuel_tbl")

    hiveContext.cacheTable("remain_fuel_tbl")

    val t_o_c = hiveContext.sql("select FliNum,DepAirIat,DatOfOriUtc,DesAirIat,FliDupNo,tibco_message_time,altitude_drvd as T_O_C_altitude,accum_distance_drvd as T_O_C_accum_dist from remain_fuel_tbl where trim(waypointName) = 'T_O_C'")

    //val t_o_d = sqlContext.sql("select FliNum,DepAirIat,DatOfOriUtc,DesAirIat,FliDupNo,tibco_message_time,altitude_drvd as T_O_D_altitude,accum_distance_drvd as T_O_D_accum_dist from remain_fuel_tbl where trim(waypointName) = 'T_O_D'")

    t_o_c.registerTempTable("toc")
    hiveContext.cacheTable("toc")
    //t_o_d.registerTempTable("tod")

    // BUSINESS LOGIC REPEATING --> ALTITUDE AT EACH WAYPOINT FOR CLB = (PREVIOUS WAYPOINT ALTITUDE + ( ALTITUDE AT T_O_C WAYPOINT - PREVIOUS WAYPOINT ALTITUDE) / ACCUMULATED GROUND DISTANCE AT T_O_C WAYPOINT * ACCUMULATED GROUND DISTANCE FOR THE CURRENT WAYPOINT )

    val altitude_drvd_upd1 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  remain_fuel_tbl t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd1.registerTempTable("upd_tbl1")

    val altitude_drvd_upd2 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl1 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd2.registerTempTable("upd_tbl2")

    val altitude_drvd_upd3 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl2 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd3.registerTempTable("upd_tbl3")

    val altitude_drvd_upd4 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl3 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")


    altitude_drvd_upd4.registerTempTable("upd_tbl4")

    val altitude_drvd_upd5 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl4 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd5.registerTempTable("upd_tbl5")

    val altitude_drvd_upd6 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl5 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd6.registerTempTable("upd_tbl6")

    val altitude_drvd_upd7 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl6 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd7.registerTempTable("upd_tbl7")

    val altitude_drvd_upd8 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl7 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")


    altitude_drvd_upd8.registerTempTable("upd_tbl8")

    val altitude_drvd_upd9 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl8 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd9.registerTempTable("upd_tbl9")

    val altitude_drvd_upd10 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl9 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd10.registerTempTable("upd_tbl10")

    val altitude_drvd_upd11 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl10 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd11.registerTempTable("upd_tbl11")

    val altitude_drvd_upd12 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl11 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd12.registerTempTable("upd_tbl12")

    val altitude_drvd_upd13 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl12 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd13.registerTempTable("upd_tbl13")

    val altitude_drvd_upd14 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl13 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd14.registerTempTable("upd_tbl14")

    val altitude_drvd_upd15 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl14 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_upd15.registerTempTable("upd_tbl15")

    val altitude_drvd_upd16 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'CLB' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.T_O_C_altitude as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/cast(t2.T_O_C_accum_dist as int) * cast(t1.accum_distance_drvd as int)) else t1.altitude_drvd end altitude_drvd_upd,waypoint_seq_no from  upd_tbl15 t1 left outer join toc t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")
    //=====================================================

    //=====================================================
    // ALTITUDE_DRVD_UPDATE FOR DSC - JOIN WITH REMAINING FULE TABLE
    //=====================================================


    val alt_at_arrival = hiveContext.sql("select FliNum,DatOfOriUtc,DepAirIat,DesAirIat,FliDupNo,tibco_message_time,RegOrAirFinNum,waypointName,altitude_drvd as alt_at_arrival,waypoint_seq_no,dstToNxtWaypnt_drvd from remain_fuel_tbl where waypoint_seq_no = cast(trim(NoOfWayOfMaiRouDepDes) as int)")

    altitude_drvd_upd16.registerTempTable("upd_tbl16")
    hiveContext.cacheTable("upd_tbl16")
    alt_at_arrival.registerTempTable("alt_at_arrival_tbl")
    hiveContext.cacheTable("alt_at_arrival_tbl")

    //BUSINESS LOGIC --> Altitude at each waypoint for DSC = (Altitude at pervious waypoint + (Altitude at Arrival Station - Altitude at pervious waypoint) / sum of Distance to next waypoint in NM value only for waypoints which are having altitude value as ‘DSC’  * Distance to next waypoint in NM for the current waypoint)

    val altitude_drvd_dsc_upd1 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/ sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  upd_tbl16 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd1.registerTempTable("dsc_upd_tbl1")

    val altitude_drvd_dsc_upd2 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl1 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd2.registerTempTable("dsc_upd_tbl2")

    val altitude_drvd_dsc_upd3 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl2 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd3.registerTempTable("dsc_upd_tbl3")

    val altitude_drvd_dsc_upd4 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl3 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd4.registerTempTable("dsc_upd_tbl4")

    val altitude_drvd_dsc_upd5 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl4 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd5.registerTempTable("dsc_upd_tbl5")

    val altitude_drvd_dsc_upd6 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl5 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd6.registerTempTable("dsc_upd_tbl6")

    val altitude_drvd_dsc_upd7 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl6 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")


    altitude_drvd_dsc_upd7.registerTempTable("dsc_upd_tbl7")

    val altitude_drvd_dsc_upd8 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl7 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd8.registerTempTable("dsc_upd_tbl8")

    val altitude_drvd_dsc_upd9 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl8 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd9.registerTempTable("dsc_upd_tbl9")

    val altitude_drvd_dsc_upd10 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl9 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd10.registerTempTable("dsc_upd_tbl10")

    val altitude_drvd_dsc_upd11 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl10 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd11.registerTempTable("dsc_upd_tbl11")

    val altitude_drvd_dsc_upd12 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl11 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd12.registerTempTable("dsc_upd_tbl12")

    val altitude_drvd_dsc_upd13 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl12 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd13.registerTempTable("dsc_upd_tbl13")

    val altitude_drvd_dsc_upd14 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl13 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd14.registerTempTable("dsc_upd_tbl14")

    val altitude_drvd_dsc_upd15 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd,t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.RegOrAirFinNum,t1.FliDupNo,t1.altitude,t1.altitude_drvd,t1.accum_distance_drvd,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no from  dsc_upd_tbl14 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    altitude_drvd_dsc_upd15.registerTempTable("dsc_upd_tbl15")

    val altitude_drvd_dsc_upd16 = hiveContext.sql("select t1.dstToNxtWaypnt_drvd as dstToNxtWaypnt_drvd1,t1.FliNum as FliNum1,t1.DatOfOriUtc as DatOfOriUtc1,t1.DepAirIat as DepAirIat1,t1.DesAirIat as DesAirIat1,t1.tibco_message_time as tibco_message_time1,t1.RegOrAirFinNum as RegOrAirFinNum1,t1.FliDupNo as FliDupNo1,t1.altitude as altitude1,t1.altitude_drvd as altitude_drvd1,t1.accum_distance_drvd as accum_distance_drvd1,case when trim(t1.altitude_drvd) = 'DSC' and trim(t1.altitude_drvd_upd) is null then round(cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int) + (cast(t2.alt_at_arrival as int) - cast(lag(t1.altitude_drvd_upd, 1, 0) over (PARTITION BY cast(trim(t1.FliDupNo) as int),trim(t1.FliNum),trim(t1.DatOfOriUtc),trim(t1.DepAirIat),trim(t1.DesAirIat),trim(t1.RegOrAirFinNum),trim(t1.tibco_message_time) ORDER BY t1.waypoint_seq_no) as int))/(t2.dstToNxtWaypnt_drvd + sum(t1.dstToNxtWaypnt_drvd) over (PARTITION BY t1.FliNum,t1.DatOfOriUtc,t1.DepAirIat,t1.DesAirIat,t1.tibco_message_time,t1.altitude_drvd_upd)) * t1.dstToNxtWaypnt_drvd) else t1.altitude_drvd_upd end altitude_drvd_upd,t1.waypoint_seq_no as waypoint_seq_no1 from  dsc_upd_tbl15 t1 left outer join alt_at_arrival_tbl t2 on t1.FliNum = t2.FliNum and t1.DepAirIat = t2.DepAirIat and t1.DatOfOriUtc = t2.DatOfOriUtc and t1.DesAirIat = t2.DesAirIat and t1.tibco_message_time = t2.tibco_message_time")

    //=====================================================

    //======================================================
    // JOIN WITH remain_fuel_tbl
    //======================================================
    altitude_drvd_dsc_upd16.registerTempTable("dsc_upd_tbl16")

    val clb_dsc_altitude_tbl =  hiveContext.sql("select tt1.*,round(tt2.altitude_drvd_upd) as altitude_drvd_upd from remain_fuel_tbl tt1 left outer join dsc_upd_tbl16 tt2 on tt1.FliNum = tt2.FliNum1 and tt1.DepAirIat = tt2.DepAirIat1 and tt1.DatOfOriUtc = tt2.DatOfOriUtc1 and tt1.DesAirIat = tt2.DesAirIat1 and tt1.tibco_message_time = tt2.tibco_message_time1 and tt1.waypoint_seq_no = tt2.waypoint_seq_no1").dropDuplicates()

    //===================================================================================
    //FSUM MESSAGES DOES NOT GIVE 77L AIRCRAFT TYPE – NEED LOADED FROM FLIGHTMASTER//
    //====================================================================================
    //READING FLIGHTMASTER DATA//
    //val flight_mstr_df = hiveContext.read.parquet("/data/helix/modelled/emfg/public/flightmaster/current/snapdate=2018-02-28/snaptime=10-00")
    flight_mstr_df.registerTempTable("flight_mstr_tbl")
    clb_dsc_altitude_tbl.registerTempTable("clb_dsc_altitude")

    val aircraft_type_drvd_df = hiveContext.sql("select t1.*,case when (trim(IatAirTyp) is null) or (trim(IatAirTyp) = '') or (trim(IatAirTyp) = 'null') then t2.aircraft_subtype else trim(IatAirTyp) end aircraft_type_drvd from clb_dsc_altitude t1 left outer join flight_mstr_tbl t2 on trim(t1.FliNum) = t2.flight_number and trim(t1.DepAirIat) = t2.pln_dep_iata_station and trim(t1.DatOfOriUtc) = t2.flight_date and trim(t1.DesAirIat) = t2.latest_pln_arr_iata_station and t2.actual_reg_number = trim(t1.RegOrAirFinNum)")

    val aircraft_type_renamed = aircraft_type_drvd_df.withColumn("IatAirTyp",$"aircraft_type_drvd")

    //===================================================================================

    //=======================================================
    //  SR_TDEV AND ISA_DEV UPDATES FOR ALL WAYPOINTS
    //=======================================================

    val temp_isa_drvd = aircraft_type_renamed.withColumn("temp_isa_drvd",when($"altitude_drvd_upd" < 36089,lit(15) - lit(0.0019812) * $"altitude_drvd_upd").otherwise(lit(-56.50)))

    val isa_dev_drvd = temp_isa_drvd.withColumn("isa_dev_drvd",round(trim($"SegTemp") - $"temp_isa_drvd".cast(DecimalType(10,2))))

    val sr_tdev_drvd = isa_dev_drvd.withColumn("sr_tdev_drvd",when(instr($"isa_dev_drvd","-") === lit(1),concat(lit("/M"),lpad(regexp_replace($"isa_dev_drvd","-",""),2,"0"))).otherwise(concat(lit("/P"),lpad($"isa_dev_drvd",2,"0"))))

    //sr_tdev_drvd.write.mode("overwrite").parquet("/user/ops_eff/lido/manu")

    //=======================================================
    //  LOGIC FOR WIND SPEED, WIND DIRECTION, WIND,  AVTRK, AVISA & GROSS WEIGHT
    //=======================================================

    val df_stage1 = sr_tdev_drvd.withColumn("estFltTimeToNxtWaypnt_drvd", lag($"estFltTimeToNxtWaypnt", 1, null).over(window1)).withColumn("segWndCmptCrs", when(trim($"segWndCmptCrs") === "", null) otherwise ($"segWndCmptCrs"))
    df_stage1.registerTempTable("df_table1")


    val df_stage2 = hiveContext.sql(
      """ select sub1.*, cast (cast (round(sqrt((power(substr(trim(sub1.TrackWindSegment_drvd1), 2) , 2)) + (power(trim(sub1.segWndCmptCrs_drvd), 2)))) as int)
        |as string) as wind_speed_drvd, case when substr(trim(sub1.TrackWindSegment_drvd1),1,1) = 'P' then substr(trim(sub1.TrackWindSegment_drvd1),2)
        |else cast( (substr(trim(sub1.TrackWindSegment_drvd1),2))*-1 as int) end as TempTrackWindSegment_drvd from (select *,  case waypoint_seq_no when '1'
        |then null else coalesce(TrackWindSegment_drvd,first_value(TrackWindSegment_drvd,true) over
        |( partition by FliNum,DepAirIat, DatOfOriUtc, DesAirIat,FliDupNo, tibco_message_time,RegOrAirFinNum order by waypoint_seq_no rows between
        |current row and unbounded following)) end as  TrackWindSegment_drvd1, case waypoint_seq_no when '1' then null else
        |coalesce(segWndCmptCrs,first_value(segWndCmptCrs,true) over ( partition by FliNum,DepAirIat, DatOfOriUtc, DesAirIat,FliDupNo, tibco_message_time,
        |RegOrAirFinNum order by waypoint_seq_no rows between current row and unbounded following)) end as segWndCmptCrs_drvd from df_table1) sub1""".stripMargin)
      .withColumn("wind_direction", when(($"wind_speed_drvd".cast("Int") !== 0) && ($"TempTrackWindSegment_drvd".isNotNull) && ($"TempTrackWindSegment_drvd".cast("Int") != 0),
        round(trim($"outbndTrueTrk") + (atan(trim($"segWndCmptCrs_drvd") / $"TempTrackWindSegment_drvd") * (180 / 3.14159))).cast("Int")) otherwise null)
      .withColumn("wind_direction_drvd", when($"wind_direction" < 0, ($"wind_direction" + 360).cast("String")) otherwise $"wind_direction".cast("String"))
      .withColumn("flight_level_avtrk_drvd", round((sum(trim($"outbndTrueTrk") * trim($"dstToNxtWaypnt_drvd")).over(window2)) / (sum(trim($"dstToNxtWaypnt_drvd")).over(window2))).cast("Int").cast("String"))
      .withColumn("flight_level_avisa_drvd", round((sum($"isa_dev_drvd" * trim($"dstToNxtWaypnt_drvd")).over(window2)) / (sum(trim($"dstToNxtWaypnt_drvd")).over(window2))).cast("Int").cast("String"))
      .withColumn("toctrop_tmp", when(trim($"waypointName") === "T_O_C", round((($"isa_dev_drvd" / 1.982) * 1000) + 36089).cast("Int").cast("String")) otherwise null) // Temporary column need to drop at the end
      .withColumn("crztemp_tmp", when(trim($"waypointName") === "T_O_C", $"SegTemp") otherwise null) // Temporary column need to drop at the end
      .withColumn("gross_weight_drvd", (coalesce(trim($"remaining_fuel_drvd"), lit(0)) + coalesce(trim($"ConFue"), lit(0)) + coalesce(trim($"PlnZfw"), lit(0))).cast("Int").cast("String"))
    df_stage2.registerTempTable("df_table2")

    //=======================================================
    //  LOGIC FOR TOCTROP_DRVD and CRZTEMP_DRVD
    //=======================================================
    val df_stage3 = hiveContext.sql(
      """select *, first_value(toctrop_tmp,true) over (partition by FliNum,DepAirIat, DatOfOriUtc, DesAirIat,FliDupNo, tibco_message_time,RegOrAirFinNum)
        |as flight_level_toctrop_drvd, first_value(crztemp_tmp,true) over (partition by FliNum,DepAirIat, DatOfOriUtc, DesAirIat,FliDupNo, tibco_message_time,RegOrAirFinNum)
        |as flight_level_crztemp_drvd from df_table2""".stripMargin)
      .drop($"toctrop_tmp")
      .drop($"crztemp_tmp")

    //=======================================================
    //  LOGIC FOR MACH_NO
    //=======================================================
    val df_machno = df_stage3.withColumn("v_actype", when(trim($"IatAirTyp") === "332", "330") otherwise trim($"IatAirTyp"))
      .withColumn("v_windcomp", when($"TempTrackWindSegment_drvd" > 100, "100") otherwise (when($"TempTrackWindSegment_drvd" < -100, "-100") otherwise $"TempTrackWindSegment_drvd"))
    val df_machno_final = df_machno.alias("df_machno").join(broadcast(machno_master.alias("machno_master")), trim(df_machno("CosIndEcoCru")) === trim(machno_master("COST_INDEX"))
      && trim(df_machno("v_windcomp")) === trim(machno_master("WIND_COMP")) && df_machno("altitude_drvd_upd").cast("Int").cast("String") === trim(machno_master("ALTITUDE"))
      && df_machno("gross_weight_drvd") === trim(machno_master("WEIGHT")) && trim(df_machno("v_actype")) === trim(machno_master("AIRCRAFT_TYPE")), "left_outer")
      .select($"df_machno.*", $"machno_master.MACH_NO".as("v_machno1"))
      .withColumn("v_ci", when($"v_machno1".isNull && UDF.isPresent(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")), trim($"CosIndEcoCru")) otherwise null)
      .withColumn("v_wc", when($"v_machno1".isNull && UDF.isPresent(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"v_windcomp") otherwise null)
      .withColumn("v_machno2", when($"v_ci".isNotNull && $"v_wc".isNotNull,
        UDF.getMachno(broadcast_var.value)(trim($"CosIndEcoCru"), $"v_windcomp", $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype"))) otherwise null)
      .withColumn("v_machno3", when($"v_ci".isNotNull && $"v_wc".isNull,
        UDF.getMachno(broadcast_var.value)(trim($"CosIndEcoCru"), UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")),
          $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) + ((($"v_windcomp" - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP"))) /
          (UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")) - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")))) *
          (UDF.getMachno(broadcast_var.value)(trim($"CosIndEcoCru"), UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")),
            $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) - UDF.getMachno(broadcast_var.value)(trim($"CosIndEcoCru"), UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")),
            $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype"))))) otherwise null)
      .withColumn("v_machno4", when($"v_ci".isNull && $"v_wc".isNotNull, UDF.getMachno(broadcast_var.value)(UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")), $"v_windcomp",
        $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) + (((trim($"CosIndEcoCru") - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX"))) /
        (UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")) - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")))) *
        (UDF.getMachno(broadcast_var.value)(UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")), $"v_windcomp",
          $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) - UDF.getMachno(broadcast_var.value)(UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")), $"v_windcomp",
          $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype"))))) otherwise null)
      .withColumn("v_machno5", when($"v_ci".isNull && $"v_wc".isNull, (UDF.getMachno(broadcast_var.value)(UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
        UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) +
        ((($"v_windcomp" - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP"))) / (UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")) -
          UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")))) * (UDF.getMachno(broadcast_var.value)(UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
          UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) -
          UDF.getMachno(broadcast_var.value)(UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
            UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype"))))) +
        (((trim($"CosIndEcoCru") - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX"))) /
          (UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")) - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")))) *
          ((UDF.getMachno(broadcast_var.value)(UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
            UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) +
            ((($"v_windcomp" - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP"))) / (UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")) -
              UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")))) * (UDF.getMachno(broadcast_var.value)(UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
              UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) -
              UDF.getMachno(broadcast_var.value)(UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
                UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype"))))) -
            (UDF.getMachno(broadcast_var.value)(UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
              UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) +
              ((($"v_windcomp" - UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP"))) / (UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")) -
                UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")))) * (UDF.getMachno(broadcast_var.value)(UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
                UDF.getMinValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")) -
                UDF.getMachno(broadcast_var.value)(UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), trim($"CosIndEcoCru"), lit("COST_INDEX")),
                  UDF.getMaxValue(broadcast_var.value)(trim($"v_actype"), $"v_windcomp", lit("WIND_COMP")), $"altitude_drvd_upd".cast("Int").cast("String"), $"gross_weight_drvd", trim($"v_actype")))))))) otherwise null)
      .withColumn("machno_drvd", (round(coalesce($"v_machno1", $"v_machno2", $"v_machno3", $"v_machno4", $"v_machno5"), 3) * 1000).cast(DecimalType(10,0)).cast("String"))
      .withColumn("last_seq_no", max(trim($"waypoint_seq_no").cast("Int")).over(window2))
      .withColumn("first_seq_no", min(trim($"waypoint_seq_no").cast("Int")).over(window2))
      .withColumn("trop_drvd", round((($"isa_dev_drvd" / 1.982) * 1000) + 36089, 2))
      .withColumn("alttheta_drvd", ((lit(288.15) - ($"altitude_drvd_upd" * 0.0019812)) + $"isa_dev_drvd") / 288.15)
      .withColumn("delta_drvd", when($"altitude_drvd_upd" < $"trop_drvd", pow((lit(288.15) - ($"altitude_drvd_upd" * 0.0019812)) / 288.15, 5.25588)) otherwise (exp((lit(36089.24) - $"altitude_drvd_upd") / 20805.1) * 0.22336))
      .withColumn("cas_drvd1", when(trim($"waypoint_seq_no") === $"first_seq_no".cast("String"), 210) otherwise null)
      .withColumn("machno_drvd", when(trim($"waypoint_seq_no") === $"last_seq_no".cast("String"), lag($"machno_drvd", 1, null).over(window1)) otherwise
        (when(trim($"waypoint_seq_no") === $"first_seq_no".cast("String"), ((sqrt((pow(((lit(1) / $"delta_drvd") * (pow((((pow(($"cas_drvd1" / 661.4786), 2)) * 0.2) + 1), 3.5) - 1) + 1), (1 / 3.5)) - 1) * 5) * 1000)).cast(DecimalType(10,0)).cast("String")) otherwise $"machno_drvd"))


    /*====================== END MACH NO LOGIC ============================*/

    val final_df = df_machno_final
      .withColumn("tas_drvd", when(trim($"waypoint_seq_no") === $"last_seq_no".cast("String"), (lag((($"machno_drvd" / 1000) * 661.4786 * sqrt($"alttheta_drvd")), 1, null).over(window1)).cast(DecimalType(10,0)).cast("String"))
        otherwise (($"machno_drvd" / 1000) * 661.4786 * sqrt($"alttheta_drvd")).cast(DecimalType(10,0)).cast("String"))
      .withColumn("cas_drvd2", when(trim($"waypoint_seq_no") !== $"first_seq_no".cast("String"), sqrt(((pow(($"delta_drvd" * ((pow(((pow(($"machno_drvd" / 1000), 2) * 0.2) + 1), 3.5)) - 1) + 1), (1 / 3.5))) - 1)) * 1479.1) otherwise null)
      .withColumn("cas_drvd", round((coalesce($"cas_drvd1", $"cas_drvd2"))).cast(DecimalType(10,0)).cast("String"))
      .withColumn("TrackWindSegment_drvd", when(trim($"waypoint_seq_no") === $"first_seq_no".cast("String"), lead($"TrackWindSegment_drvd1", 1, null).over(window1)) otherwise $"TrackWindSegment_drvd1")
      .withColumn("wind_direction_drvd", when(trim($"waypoint_seq_no") === $"first_seq_no".cast("String"), lead($"wind_direction_drvd", 1, null).over(window1)) otherwise
        (when($"TempTrackWindSegment_drvd".cast("Int") === 0, $"outbndTrueTrk") otherwise $"wind_direction_drvd"))
      .withColumn("wind_speed_drvd", when(trim($"waypoint_seq_no") === $"first_seq_no".cast("String"), "0") otherwise $"wind_speed_drvd")
      .withColumn("wind_drvd", concat_ws("/", $"wind_direction_drvd", $"wind_speed_drvd"))
      .withColumn("estElpTimeOvrWaypnt", when(trim($"waypoint_seq_no") === $"first_seq_no".cast("String"), "1") otherwise $"estElpTimeOvrWaypnt")
      .withColumn("estFltTimeToNxtWaypnt_drvd", when(trim($"waypoint_seq_no") === $"first_seq_no".cast("String"), "1")
        otherwise (when(trim($"waypoint_seq_no") === ($"first_seq_no" + 1).cast("String"), ($"estFltTimeToNxtWaypnt_drvd" - 1).cast("String")) otherwise $"estFltTimeToNxtWaypnt_drvd"))
      .withColumn("airdist_drvd", round((trim($"dstToNxtWaypnt_drvd") * ($"tas_drvd" / ($"tas_drvd" + $"TempTrackWindSegment_drvd"))), 2))
      .withColumn("acc_airdist_drvd", round(sum($"airdist_drvd").over(window1), 2))
      .withColumn("altitude_drvd_upd",round($"altitude_drvd_upd").cast("Int"))
      .drop($"v_machno1").drop($"v_machno2").drop($"v_machno3").drop($"v_machno4").drop($"v_machno5").drop($"alttheta_drvd").drop($"delta_drvd").drop($"cas_drvd1").drop($"cas_drvd2")
      .drop($"TrackWindSegment_drvd1").drop($"alttheta_drvd").drop($"delta_drvd").drop($"last_seq_no").drop($"first_seq_no").drop($"v_actype").drop($"v_windcomp").drop($"v_ci").drop($"v_wc")

    final_df
  }
}