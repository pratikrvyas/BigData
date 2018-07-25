/*----------------------------------------------------------
 * Created on  : 04/24/2018
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
    * UDF function to calculate total weight for a specific category (multileg flights)
    * @param load_type (baggage_weight, cargo_weight, miscl_weight, mail_weight, transit_weight)
    * @return total weight
    */
  def custom_sum(load_type: String) = udf((rows: Seq[Row], arr_station: String) => {
    var total_load:Long = 0
    val max_leg = rows.map(_.getAs[Int]("flight_leg_number")).max
    rows.count(_.getAs[Int]("flight_leg_number") == max_leg) match {
      case 1 =>             // Last leg
        total_load = rows.filter(_.getAs[String]("off_point") == arr_station).map(_.getAs[String](load_type).toLong).sum
      case _ =>             // All legs except the last leg
        total_load = rows.filter(_.getAs[Int]("flight_leg_number") == max_leg).map(_.getAs[String]("off_point")).map(off_point =>
          rows.filter(_.getAs[String]("off_point") == off_point).map(_.getAs[String](load_type).toLong).sum).sum
    }
    total_load
  })
}
