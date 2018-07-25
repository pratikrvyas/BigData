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
  case class LidoWeatherDedupArgs(incr_path: String = null, hist_path: String= null, h_out_path: Option[String] = None, l_out_path: String = null,
                                  compression: String = null, init: Boolean = false, days: Int = 15, part: Int = 0)
}
