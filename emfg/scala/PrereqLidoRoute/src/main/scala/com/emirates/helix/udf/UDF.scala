/*----------------------------------------------------------
 * Created on  : 18/03/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UDF.scala
 * Description : Scala file to keep all UDF functions
 * ----------------------------------------------------------
 */
package com.emirates.helix.udf

import java.io.Serializable
import org.apache.spark.sql.functions.udf
import com.emirates.helix.model.LidoRouteModel.Row
object UDF extends Serializable{

  /**
    * UDF function for getting minimum value of column
    *
    */
  def getMinValue(metadata: Array[Row]) = udf((aircrafttype: String, other_col: String,col_name: String) => {
    if (other_col == null || other_col.trim == "") null.asInstanceOf[String]
    else {
      try {
        col_name match {
          case "COST_INDEX" => {
            val filtered_collection = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index >= other_col.toInt)
            if (filtered_collection.length != 0) filtered_collection.reduceLeft((x,y) => if(x.cost_index < y.cost_index) x else y).cost_index.toString else null.asInstanceOf[String]
          }
          case _ => {
            val filtered_collection = metadata.filter(_.aircraft_type == aircrafttype).filter(_.wind_comp >= other_col.toInt)
            if (filtered_collection.length != 0) filtered_collection.reduceLeft((x,y) => if(x.wind_comp < y.wind_comp) x else y).wind_comp.toString else null.asInstanceOf[String]
          }
        }
      }
      catch {
        case e: Exception => null.asInstanceOf[String]
      }
    }
  })

  /**
    * UDF function for getting maximum value of column
    *
    */
  def getMaxValue(metadata: Array[Row]) = udf((aircrafttype: String, other_col: String,col_name: String) => {
    if (other_col == null || other_col.trim == "") null.asInstanceOf[String]
    else {
      try {
        col_name match {
          case "COST_INDEX" => {
            val filtered_collection = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index <= other_col.toInt)
            if (filtered_collection.length != 0) filtered_collection.reduceLeft((x,y) => if(x.cost_index > y.cost_index) x else y).cost_index.toString else null.asInstanceOf[String]
          }
          case _ => {
            val filtered_collection = metadata.filter(_.aircraft_type == aircrafttype).filter(_.wind_comp <= other_col.toInt)
            if (filtered_collection.length != 0) filtered_collection.reduceLeft((x,y) => if(x.wind_comp > y.wind_comp) x else y).wind_comp.toString else null.asInstanceOf[String]
          }
        }
      }
      catch {
        case e: Exception => null.asInstanceOf[String]
      }
    }
  })

  /**
    * UDF function for checking whether a column value is present in master dataset
    *
    */

  def isPresent(metadata: Array[Row]) = udf((aircrafttype: String, other_col: String,col_name: String) => {
    if (other_col == null || other_col.trim == "") false
    else {
      col_name match {
        case "COST_INDEX" => {
          val filtered_collection = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == other_col.toInt)
          if (filtered_collection.length != 0) true else false
      }
        case _ => {
          val filtered_collection = metadata.filter(_.aircraft_type == aircrafttype).filter(_.wind_comp == other_col.toInt)
          if (filtered_collection.length != 0) true else false
        }
      }
    }
  })

  /**
    * UDF function for calculating the machno. For details please refer omega stored procedure or te document
    *
    */
  def getMachno(metadata: Array[Row]) = udf((costindex: String, windcomp: String, altitude: String, weight: String, aircrafttype: String ) => {
    if(costindex == null || costindex.trim == "" || windcomp == null || windcomp.trim == "" || altitude == null || altitude.trim == "" || weight == null || weight.trim == "") {
      null.asInstanceOf[Float]
    }
    else {
      try {
        var (v_mach_11,v_mach_12,v_mach_21,v_mach_22,v_mach_1,v_mach_2,v_mach_wind) = (0.0,0.0,0.0,0.0,0.0,0.0,0.0)
        val filtered_collection1 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.weight >= weight.toInt)
        val v_max_wt = if (filtered_collection1.length != 0) filtered_collection1.reduceLeft((x,y) => if(x.weight < y.weight) x else y).weight else 0
        val filtered_collection2 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.weight <= weight.toInt)
        val v_min_wt = if (filtered_collection2.length != 0) filtered_collection2.reduceLeft((x,y) => if(x.weight > y.weight) x else y).weight else 0
        val v_int_wt = if(v_max_wt - v_min_wt != 0) (weight.toFloat -  v_min_wt.toFloat)/(v_max_wt.toFloat - v_min_wt.toFloat) else 0.0
        val filtered_collection3 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.altitude >= altitude.toInt)
        val v_max_alt = if (filtered_collection3.length != 0) filtered_collection3.reduceLeft((x,y) => if(x.altitude < y.altitude) x else y).altitude else 0
        val filtered_collection4 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.altitude <= altitude.toInt)
        val v_min_alt = if (filtered_collection4.length != 0) filtered_collection4.reduceLeft((x,y) => if(x.altitude > y.altitude) x else y).altitude else 0
        val v_int_alt = if(v_max_alt - v_min_alt != 0) (altitude.toFloat -  v_min_alt.toFloat)/(v_max_alt.toFloat - v_min_alt.toFloat) else 0.0
        if(0 != (v_max_wt - v_min_wt) && 0!= (v_max_alt - v_min_alt)) {
          val filtered_collection5 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == v_min_wt).filter(_.altitude == v_min_alt)
          v_mach_11 = if(filtered_collection5.length !=0) filtered_collection5(0).mach_no.toFloat else 0.0
          val filtered_collection6 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == v_max_wt).filter(_.altitude == v_min_alt)
          v_mach_12 = if(filtered_collection6.length !=0) filtered_collection6(0).mach_no.toFloat else 0.0
          val filtered_collection7 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == v_min_wt).filter(_.altitude == v_max_alt)
          v_mach_21 = if(filtered_collection7.length !=0) filtered_collection7(0).mach_no.toFloat else 0.0
          val filtered_collection8 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == v_max_wt).filter(_.altitude == v_max_alt)
          v_mach_22 = if(filtered_collection8.length !=0) filtered_collection8(0).mach_no.toFloat else 0.0
          v_mach_1 = v_mach_11 + (v_int_wt * (v_mach_12 - v_mach_11))
          v_mach_2 = v_mach_21 + (v_int_wt * (v_mach_22 - v_mach_21))
          v_mach_wind = v_mach_1 + (v_int_alt * (v_mach_2 - v_mach_1))
        }
        else if(0 == (v_max_wt - v_min_wt) && 0!= (v_max_alt - v_min_alt)) {
          val filtered_collection9 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == v_min_wt).filter(_.altitude == v_min_alt)
          v_mach_11 = if(filtered_collection9.length !=0) filtered_collection9(0).mach_no.toFloat else 0.0
          val filtered_collection10 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == v_min_wt).filter(_.altitude == v_max_alt)
          v_mach_12 = if(filtered_collection10.length !=0) filtered_collection10(0).mach_no.toFloat else 0.0
          v_mach_wind = v_mach_11 + (v_int_alt * (v_mach_12 - v_mach_11))
        }
        else if(0 != (v_max_wt - v_min_wt) && 0== (v_max_alt - v_min_alt)) {
          val filtered_collection11 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == v_min_wt).filter(_.altitude == v_min_alt)
          v_mach_11 = if(filtered_collection11.length !=0) filtered_collection11(0).mach_no.toFloat else 0.0
          val filtered_collection12 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == v_max_wt).filter(_.altitude == v_min_alt)
          v_mach_12 = if(filtered_collection12.length !=0) filtered_collection12(0).mach_no.toFloat else 0.0
          v_mach_wind = v_mach_11 + (v_int_wt * (v_mach_12 - v_mach_11))
        }
        else {
          val filtered_collection13 = metadata.filter(_.aircraft_type == aircrafttype).filter(_.cost_index == costindex.toInt).filter(_.wind_comp == windcomp.toInt)
            .filter(_.weight == weight.toInt).filter(_.altitude == altitude.toInt)
          v_mach_wind = if(filtered_collection13.length !=0) filtered_collection13(0).mach_no.toFloat else 0.0
        }
        v_mach_wind
      }
      catch {
        case e: Exception => null.asInstanceOf[Float]
      }
    }
  })
}
