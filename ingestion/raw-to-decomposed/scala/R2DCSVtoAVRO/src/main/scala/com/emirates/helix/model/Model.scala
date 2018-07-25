/*----------------------------------------------------------
 * Created on  : 13/12/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
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
  case class Args(in_path: String = null, out_path: String = null, compression: String = null, delimiter: String = ",")
}
