/*----------------------------------------------------------
 * Created on  : 04/01/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : Model.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.emfg.model

/**
  * Model class with different case classes for supporting operations
  */

object Model {
  case class PlndFlgtArgs(fm_path: String = null, l_flight: String= null, l_route: String = null, l_weather: String = null,
                          out_incr: String = null, out_hist: Option[String] = None, compression: String = null,
                          init: Boolean = false, months: Int = 2, part: Int = 0)
}