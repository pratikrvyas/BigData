/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSRouteDeDupeArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.agsroutededupearguments

/**
  * Model class with different case classes for supporting operations
  */

object AGSRouteDeDupeArgs {
  case class Args(ags_src_route_history_sync_location: String = null
                  ,ags_src_route_incremental_location: String = null
                  ,ags_tgt_route_dedupe_history_location: String = null
                  ,ags_tgt_route_dedupe_current_location: String = null
                  ,coalesce_value : Int = 10
                  ,lookback_months : Int = 3
                  ,compression: String = null)
}

