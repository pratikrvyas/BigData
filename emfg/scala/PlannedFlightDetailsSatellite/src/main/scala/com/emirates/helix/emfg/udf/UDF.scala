/*----------------------------------------------------------
 * Created on  : 04/11/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UDF.scala
 * Description : Scala file to keep all UDF functions
 * ----------------------------------------------------------
 */

package com.emirates.helix.emfg.udf

import java.io.Serializable
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row

/**
  * UDF class
  */
object UDF extends Serializable{
  /**
    * UDF function to get latest entry in Lido Flight/Route column array of structs
    *
    */
  case class latest_flight_pln_time_details (pln_block_time: String = null,
                                             est_flight_time: String = null,
                                             pln_trip_time: String = null,
                                             pln_contingency_time: String = null,
                                             pln_altn_time: String = null,
                                             pln_frsv_time: String = null,
                                             pln_additional_time: String = null,
                                             pln_taxi_out_time: String = null,
                                             pln_taxi_in_time: String = null,
                                             pln_ramp_time: String = null,
                                             est_time_mins: String = null,
                                             altn_1_time: String = null,
                                             altn_2_time: String = null,
                                             altn_3_time: String = null,
                                             altn_4_time: String = null,
                                             extra_1_time: String = null,
                                             extra_2_time: String = null,
                                             extra_3_time: String = null,
                                             extra_4_time: String = null,
                                             lido_version_num: String = null,
                                             msg_received_datetime: String = null)

  case class latest_flight_pln_fuel_details(touch_down_fuel: String = null,
                                            trip_fuel: String = null,
                                            contingency_fuel: String = null,
                                            altn_fuel: String = null,
                                            frsv_fuel: String = null,
                                            additional_fuel: String = null,
                                            taxi_out_fuel: String = null,
                                            taxi_in_fuel: String = null,
                                            block_fuel: String = null,
                                            t_o_fuel: String = null,
                                            ramp_fuel: String = null ,
                                            altn_1_fuel: String = null,
                                            altn_2_fuel: String = null,
                                            altn_3_fuel: String = null,
                                            altn_4_fuel: String = null,
                                            addl_1_fuel: String = null,
                                            rsn_addl_1_fuel: String = null,
                                            addl_2_fuel: String = null,
                                            rsn_addl_2_fuel: String = null,
                                            addl_3_fuel: String = null,
                                            rsn_addl_3_fuel: String = null,
                                            addl_4_fuel: String = null,
                                            rsn_addl_4_fuel: String = null,
                                            econ_fuel: String = null,
                                            fuel_over_destn: String = null,
                                            tank_capacity: String = null,
                                            saving_usd: String = null,
                                            perf_correction: String = null,
                                            contingency_coverage: String = null,
                                            contingency_percent: String = null,
                                            enr_fuel_altn: String = null,
                                            enr_altn_prec: String = null,
                                            tripfuel_fl_blw: String = null,
                                            zfw_1000_minus: String = null,
                                            zfw_1000_plus: String = null,
                                            lido_version_num: String = null,
                                            msg_received_datetime: String = null)

  case class latest_flight_pln_distance_details(air_dist_nm: String = null,
                                                alt_toc: String = null,
                                                alt_tod: String = null,
                                                great_circle_dist_nm: String = null,
                                                grnd_dist: String = null,
                                                sid_dist_nm: String = null,
                                                star_dist_nm: String = null,
                                                route_optimiz: String = null,
                                                cruize_Deg: String = null,
                                                cruize: String = null,
                                                cruize_temp: String = null,
                                                TOCTROP: String = null,
                                                AVTRK: String = null,
                                                AVWC: String = null,
                                                AVISA: String = null,
                                                CID: String = null,
                                                gain_loss: String = null,
                                                SID: String = null,
                                                SID_exitpoint: String = null,
                                                STAR: String = null,
                                                STAR_entrypoint: String = null,
                                                no_altn_stations: String = null,
                                                lido_version_num: String = null,
                                                msg_received_datetime: String = null)

  case class latest_flight_pln_alternate_details(altn_1_icao_code: String = null,
                                                        altn_1_iata_code: String = null,
                                                        altn_1_dist: String = null,
                                                        altn_1_time: String = null,
                                                        altn_1_fuel: String = null,
                                                        altn_2_icao_code: String = null,
                                                        altn_2_iata_code: String = null,
                                                        altn_2_dist: String = null,
                                                        altn_2_time: String = null,
                                                        altn_2_fuel: String = null,
                                                        altn_3_icao_code: String = null,
                                                        altn_3_iata_code: String = null,
                                                        altn_3_dist: String = null,
                                                        altn_3_time: String = null,
                                                        altn_3_fuel: String = null,
                                                        altn_4_icao_code: String = null,
                                                        altn_4_iata_code: String = null,
                                                        altn_4_dist: String = null,
                                                        altn_4_time: String = null,
                                                        altn_4_fuel: String = null,
                                                        enr_altn_prec: String = null,
                                                        enr_altn_fuel: String = null,
                                                        lido_version_num: String = null,
                                                        msg_received_datetime: String = null)

  case class latest_flight_arr_weather_message(arr_metar_message: String = null,
                                               arr_metar_message_time: String = null,
                                               arr_metar_station_iata_code: String = null,
                                               arr_metar_station_icao_code: String = null,
                                               arr_metar_received_time: String = null )

  case class latest_flight_dep_weather_message(dep_metar_message: String = null,
                                               dep_metar_message_time: String = null,
                                               dep_metar_station_iata_code: String = null,
                                               dep_metar_station_icao_code: String = null,
                                               dep_metar_received_time: String = null)

  /**
    * UDF function to get latest entry in Lido time column array of structs
    *
    */
  val getLatestLidoTime = udf((fields: Seq[Row]) =>{
    val out = fields.sortWith(_.getAs[Long]("timestamp") > _.getAs[Long]("timestamp"))(0)

    latest_flight_pln_time_details(pln_block_time = out.getAs[String]("pln_block_time"),
      est_flight_time = out.getAs[String]("est_flight_time"),
      pln_trip_time = out.getAs[String]("pln_trip_time"),
      pln_contingency_time = out.getAs[String]("pln_contingency_time"),
      pln_altn_time = out.getAs[String]("pln_altn_time"),
      pln_frsv_time = out.getAs[String]("pln_frsv_time"),
      pln_additional_time = out.getAs[String]("pln_additional_time"),
      pln_taxi_out_time = out.getAs[String]("pln_taxi_out_time"),
      pln_taxi_in_time = out.getAs[String]("pln_taxi_in_time"),
      pln_ramp_time = out.getAs[String]("pln_ramp_time"),
      est_time_mins = out.getAs[String]("est_time_mins"),
      altn_1_time = out.getAs[String]("altn_1_time"),
      altn_2_time = out.getAs[String]("altn_2_time"),
      altn_3_time = out.getAs[String]("altn_3_time"),
      altn_4_time = out.getAs[String]("altn_4_time"),
      extra_1_time = out.getAs[String]("extra_1_time"),
      extra_2_time = out.getAs[String]("extra_2_time"),
      extra_3_time = out.getAs[String]("extra_3_time"),
      extra_4_time = out.getAs[String]("extra_4_time"),
      lido_version_num = out.getAs[String]("lido_version_num"),
      msg_received_datetime = out.getAs[String]("msg_received_datetime"))
  })

  /**
    * UDF function to get latest entry in Lido fuel column array of structs
    *
    */
  val getLatestLidoFuel = udf((fields: Seq[Row]) =>{
    val out = fields.sortWith(_.getAs[Long]("timestamp") > _.getAs[Long]("timestamp"))(0)

    latest_flight_pln_fuel_details(touch_down_fuel = out.getAs[String]("touch_down_fuel"),
      trip_fuel = out.getAs[String]("trip_fuel"),
      contingency_fuel = out.getAs[String]("contingency_fuel"),
      altn_fuel = out.getAs[String]("altn_fuel"),
      frsv_fuel = out.getAs[String]("frsv_fuel"),
      additional_fuel = out.getAs[String]("additional_fuel"),
      taxi_out_fuel = out.getAs[String]("taxi_out_fuel"),
      taxi_in_fuel = out.getAs[String]("taxi_in_fuel"),
      block_fuel = out.getAs[String]("block_fuel"),
      t_o_fuel = out.getAs[String]("t_o_fuel"),
      ramp_fuel = out.getAs[String]("ramp_fuel"),
      altn_1_fuel = out.getAs[String]("altn_1_fuel"),
      altn_2_fuel = out.getAs[String]("altn_2_fuel"),
      altn_3_fuel = out.getAs[String]("altn_3_fuel"),
      altn_4_fuel = out.getAs[String]("altn_4_fuel"),
      addl_1_fuel = out.getAs[String]("addl_1_fuel"),
      rsn_addl_1_fuel = out.getAs[String]("rsn_addl_1_fuel"),
      addl_2_fuel = out.getAs[String]("addl_2_fuel"),
      rsn_addl_2_fuel = out.getAs[String]("rsn_addl_2_fuel"),
      addl_3_fuel = out.getAs[String]("addl_3_fuel"),
      rsn_addl_3_fuel = out.getAs[String]("rsn_addl_3_fuel"),
      addl_4_fuel = out.getAs[String]("addl_4_fuel"),
      rsn_addl_4_fuel = out.getAs[String]("rsn_addl_4_fuel"),
      econ_fuel = out.getAs[String]("econ_fuel"),
      fuel_over_destn = out.getAs[String]("fuel_over_destn"),
      tank_capacity = out.getAs[String]("tank_capacity"),
      saving_usd = out.getAs[String]("saving_usd"),
      perf_correction = out.getAs[String]("perf_correction"),
      contingency_coverage = out.getAs[String]("contingency_coverage"),
      contingency_percent = out.getAs[String]("contingency_percent"),
      enr_fuel_altn = out.getAs[String]("enr_fuel_altn"),
      enr_altn_prec = out.getAs[String]("enr_altn_prec"),
      tripfuel_fl_blw = out.getAs[String]("tripfuel_fl_blw"),
      zfw_1000_minus = out.getAs[String]("zfw_1000_minus"),
      zfw_1000_plus = out.getAs[String]("zfw_1000_plus"),
      lido_version_num = out.getAs[String]("lido_version_num"),
      msg_received_datetime = out.getAs[String]("msg_received_datetime"))
  })

  /**
    * UDF function to get latest entry in Lido distance column array of structs
    *
    */
  val getLatestLidoDistance = udf((fields: Seq[Row]) =>{
    val out = fields.sortWith(_.getAs[Long]("timestamp") > _.getAs[Long]("timestamp"))(0)

    latest_flight_pln_distance_details(air_dist_nm = out.getAs[String]("air_dist_nm"),
      alt_toc = out.getAs[String]("alt_toc"),
      alt_tod = out.getAs[String]("alt_tod"),
      great_circle_dist_nm = out.getAs[String]("great_circle_dist_nm"),
      grnd_dist = out.getAs[String]("grnd_dist"),
      sid_dist_nm = out.getAs[String]("sid_dist_nm"),
      star_dist_nm = out.getAs[String]("star_dist_nm"),
      route_optimiz = out.getAs[String]("route_optimiz"),
      cruize_Deg = out.getAs[String]("cruize_Deg"),
      cruize = out.getAs[String]("cruize"),
      cruize_temp = out.getAs[String]("cruize_temp"),
      TOCTROP = out.getAs[String]("TOCTROP"),
      AVTRK = out.getAs[String]("AVTRK"),
      AVWC = out.getAs[String]("AVWC"),
      AVISA = out.getAs[String]("AVISA"),
      CID = out.getAs[String]("CID"),
      gain_loss = out.getAs[String]("gain_loss"),
      SID = out.getAs[String]("SID"),
      SID_exitpoint = out.getAs[String]("SID_exitpoint"),
      STAR = out.getAs[String]("STAR"),
      STAR_entrypoint = out.getAs[String]("STAR_entrypoint"),
      no_altn_stations = out.getAs[String]("no_altn_stations"),
      lido_version_num = out.getAs[String]("lido_version_num"),
      msg_received_datetime = out.getAs[String]("msg_received_datetime"))
  })

  /**
    * UDF function to get latest entry in Lido waypoint alternate column array of structs
    *
    */
  val getLatestLidoWpAlt = udf((fields: Seq[Row]) =>{
    val out = fields.sortWith(_.getAs[Long]("timestamp") > _.getAs[Long]("timestamp"))(0)

    latest_flight_pln_alternate_details(altn_1_icao_code = out.getAs[String]("altn_1_icao_code"),
      altn_1_iata_code = out.getAs[String]("altn_1_iata_code"),
      altn_1_dist = out.getAs[String]("altn_1_dist"),
      altn_1_time = out.getAs[String]("altn_1_time"),
      altn_1_fuel = out.getAs[String]("altn_1_fuel"),
      altn_2_icao_code = out.getAs[String]("altn_2_icao_code"),
      altn_2_iata_code = out.getAs[String]("altn_2_iata_code"),
      altn_2_dist = out.getAs[String]("altn_2_dist"),
      altn_2_time = out.getAs[String]("altn_2_time"),
      altn_2_fuel = out.getAs[String]("altn_2_fuel"),
      altn_3_icao_code = out.getAs[String]("altn_3_icao_code"),
      altn_3_iata_code = out.getAs[String]("altn_3_iata_code"),
      altn_3_dist = out.getAs[String]("altn_3_dist"),
      altn_3_time = out.getAs[String]("altn_3_time"),
      altn_3_fuel = out.getAs[String]("altn_3_fuel"),
      altn_4_icao_code = out.getAs[String]("altn_4_icao_code"),
      altn_4_iata_code = out.getAs[String]("altn_4_iata_code"),
      altn_4_dist = out.getAs[String]("altn_4_dist"),
      altn_4_time = out.getAs[String]("altn_4_time"),
      altn_4_fuel = out.getAs[String]("altn_4_fuel"),
      enr_altn_prec = out.getAs[String]("enr_altn_prec"),
      enr_altn_fuel = out.getAs[String]("enr_altn_fuel"),
      lido_version_num = out.getAs[String]("lido_version_num"),
      msg_received_datetime = out.getAs[String]("msg_received_datetime"))
  })

  /**
    * UDF function to get latest entry in Arrival station Weather column array of structs
    *
    */
  val getLatestWeatherArr = udf((fields: Seq[Row]) => {
    val out = fields.sortWith(_.getAs[Long]("timestamp") > _.getAs[Long]("timestamp"))(0)
    latest_flight_arr_weather_message(arr_metar_message = out.getAs[String]("arr_metar_message"),
      arr_metar_message_time = out.getAs[String]("arr_metar_message_time"),
      arr_metar_station_iata_code = out.getAs[String]("arr_metar_station_iata_code"),
      arr_metar_station_icao_code = out.getAs[String]("arr_metar_station_icao_code"),
      arr_metar_received_time = out.getAs[String]("arr_metar_received_time"))
  })

  /**
    * UDF function to get latest entry in Departure station Weather column array of structs
    *
    */
  val getLatestWeatherDep = udf((fields: Seq[Row]) => {
    val out = fields.sortWith(_.getAs[Long]("timestamp") > _.getAs[Long]("timestamp"))(0)
    latest_flight_dep_weather_message(dep_metar_message = out.getAs[String]("dep_metar_message"),
      dep_metar_message_time = out.getAs[String]("dep_metar_message_time"),
      dep_metar_station_iata_code = out.getAs[String]("dep_metar_station_iata_code"),
      dep_metar_station_icao_code = out.getAs[String]("dep_metar_station_icao_code"),
      dep_metar_received_time = out.getAs[String]("dep_metar_received_time"))
  })
}
