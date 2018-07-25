/*----------------------------------------------------------------------------
 * Created on  : 01/03/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSReflineHistorySchemaSyncArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.agsreflineschemasyncarguments

/**
  * Model class with different case classes for supporting operations
  */

object AGSReflineHistorySchemaSyncArgs {
  case class Args(ags_src_refline_history_location: String = null
                 ,ags_tgt_refline_history_sync_location: String = null
                 ,coalesce_value : Int = 10
                 ,compression: String = null)
}
