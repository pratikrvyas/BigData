/*----------------------------------------------------------------------------
 * Created on  : 02/27/2018
 * Author      : Fayaz Shaik (s796466)
 * Email       : fayazbasha.shaik@emirates.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : ActualFuelBurnUtils.scala
 * Description : Util class for ActualFuelBurn.
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix.util

/**
  * The common methods for operation efficiency project will be available in this Trait
  */

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, col, lit, udf}

trait ActualFuelBurnUtils {

  val COLUMNLIST_flight_act_waypnt_fuel_details = List("file_name", "seq_no", "latitude", "longitude",
    "latitude_received", "longitude_received", "baro_setting", "gross_weight", "fuel_burned", "fob", "fob_t1",
    "fob_t2", "fob_t3", "fob_t4", "fob_t5", "fob_t6", "fob_t7", "fob_t8", "fob_t9", "fob_t10", "fob_t11", "fob_t12")

  val COLUMNLIST_flight_act_waypnt_distance_speed_details = List( "file_name", "seq_no", "latitude", "longitude",
    "latitude_received", "longitude_received", "true_heading",  "baro_setting", "alt_std", "alt_qnh", "CAS", "TAS",
    "mach", "phase", "track_dist", "N11", "N12", "N13", "N14", "IVV", "air_grnd_discrete", "tran_alt_discrete", "CG",
    "isa_deviation_actuals", "wind_direction_actuals", "wind_speed_actuals", "TAT", "SAT", "cr_wind_comp",
    "cr_wind_vector", "wind_comp_actuals", "wind_comp_navigation", "altitude_actuals", "height_actuals",
    "gnd_speed_actuals", "air_distance_actuals", "acc_dist_nm_actuals", "acc_air_dist_nm_actuals", "acc_time_mins" )

  /**
    * To convert a set of columns as Map <String,String>
    */
  val asMap: UserDefinedFunction = udf((keys: Seq[String], values: Seq[String]) => keys.zip(values).toMap)

  /**
    * Method to convert struct to Map
    */
  def toMap(df: DataFrame, colList: List[String], newColName: String): DataFrame = {

    val keys = array(colList.map(lit): _*)
    val values = array(colList.map(col): _*)
    val mapDF = df.withColumn(newColName, asMap(keys, values))
    mapDF
  }
}
