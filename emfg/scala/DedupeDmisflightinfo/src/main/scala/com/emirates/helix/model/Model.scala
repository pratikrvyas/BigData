/*----------------------------------------------------------------------------
 * Created on  : 01/23/2018
 * Author      : Manu Mukundan(S796466)
 * Email       : manu.mtitsr@gmail.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : Model.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.model

/**
  * Model class with different case classes for supporting operations
  */

object Model {

  case class DMISDedupArgs(incr_path: String = null, hist_path: String= null, h_out_path: Option[String] = None, l_out_path: String = null,
                           compression: String = null, init: Boolean = false, months: Int = 12, part: Int = 0)
}
