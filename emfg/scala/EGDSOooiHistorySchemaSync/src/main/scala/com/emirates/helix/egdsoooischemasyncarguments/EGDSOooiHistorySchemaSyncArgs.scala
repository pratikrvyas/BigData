/*----------------------------------------------------------------------------
 * Created on  : 04/16/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EGDSOooiHistorySchemaSyncArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.egdsoooischemasyncarguments

/**
  * Model class with different case classes for supporting operations
  */

object EGDSOooiHistorySchemaSyncArgs {
  case class Args(egds_oooi_src_history_location: String = null
                 ,egds_oooi_tgt_history_sync_location: String = null
                 ,coalesce_value : Int = 10
                 ,compression: String = null)
}
