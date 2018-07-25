/*----------------------------------------------------------------------------
 * Created on  : 01/03/2018
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
  case class SyncArgs(in_path: String = null,out_path: String = null, compression: String = null, in_format: String = null,
                  out_format: String = null, mapping_file: String = null )
}
