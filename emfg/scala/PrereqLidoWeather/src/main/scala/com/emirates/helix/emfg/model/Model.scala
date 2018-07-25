/*----------------------------------------------------------------------------
 * Created on  : 03/26/2018
 * Author      : Manu Mukundan(S796466)
 * Email       : manu.mtitsr@gmail.com
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

  case class LidoMetarPrereqArgs(in_path: String = null, out_path: String = null,compression: String = null, in_format: String = null,
                              out_format: String = null, part:Int = 4)

}
