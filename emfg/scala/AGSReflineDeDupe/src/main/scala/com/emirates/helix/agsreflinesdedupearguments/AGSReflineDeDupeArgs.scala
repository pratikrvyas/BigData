/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSReflineHistorySchemaSyncArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.agsreflinesdedupearguments

/**
  * Model class with different case classes for supporting operations
  */

object AGSReflineDeDupeArgs {
  case class Args(ags_src_refline_history_sync_location: String = null
                  ,ags_src_refline_incremental_location: String = null
                  ,ags_tgt_refline_dedupe_history_location: String = null
                  ,ags_tgt_refline_dedupe_current_location: String = null
                  ,coalesce_value : Int = 10
                  ,lookback_months : Int = 14
                  ,compression: String = null)
}

