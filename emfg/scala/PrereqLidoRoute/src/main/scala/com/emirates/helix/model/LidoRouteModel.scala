/*----------------------------------------------------------
 * Created on  : 18/03/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : LidoRouteModel.scala
 * Description : Scala file to keep case class files
 * ----------------------------------------------------------
 */
package com.emirates.helix.model

object LidoRouteModel {
  case class Row(cost_index:Int = 0, wind_comp:Int = 0, altitude:Int = 0, weight:Int = 0, mach_no: String = null, aircraft_type: String = null)
  case class LidoRouteArgs(in_path: String = null, out_path: String = null, compression: String = null, in_format: String = null, out_format: String = null, part: Int = 2,
                           onetimemaster_path: String = null, arinc_airport: String = null, arinc_runway: String = null, flight_master: String = null)
}
