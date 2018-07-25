/*----------------------------------------------------------------------------
 * Created on  : 13/Mar/2018
 * Author      : Pratik Vyas (s795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlightPayloadUtils.scala
 * Description : Util class for FlightPayload
 * ---------------------------------------------------------------------------
 */


package com.emirates.helix.util


import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, col, lit, udf}
trait FlightPayloadUtils {

  val COLUMNLIST_flight_act_payload_details= List("ZFW","MZFW","TOW","RTOW","LWT","MLWT","DOW","DOI","LIZFW","MACLIZFW","LITOW","MACTOW","total_compartment_weight","underload","final_version_num","msg_received_date")

  val COLUMNLIST_flight_pln_payload_details = List("ELWT", "MTAXIWT", "MLWT","ETOW", "MTOW", "EZFW", "MZFW", "lido_version_num", "msg_received_date")

  val COLUMNLIST_flight_pax_payload_details = List("total_pax_loadsheet_count","f_pax_loadsheet_count","j_pax_loadsheet_count","y_pax_loadsheet_count","infant_count","PWT","final_version_num","msg_received_date")

  val COLUMNLIST_flight_deadload_details = List("baggage_weight","total_excess_bag_weight","cargo_weight","mail_weight","pax_weight","miscl_weight","total_payload","msg_received_date")
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
