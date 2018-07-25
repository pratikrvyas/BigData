/*----------------------------------------------------------
 * Created on  : 04/22/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : Model.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */

package com.emirates.helix.emfg.model

object Model {
  case class AltLdmArgs(in_path: String= null, out_path: String = null, rej_path: String = null, in_format: String = null,
                        out_format: String = null, compression: String = null, date_offset: Int = 1, part: Int = 0)
}
