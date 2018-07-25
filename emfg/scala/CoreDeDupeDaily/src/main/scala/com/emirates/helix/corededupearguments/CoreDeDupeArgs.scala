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
package com.emirates.helix.corededupearguments

/**
  * Model class with different case classes for supporting operations
  */

object CoreDeDupeArgs {
  case class Args(core_dedupe_current_location: String = null
                  ,core_incremental_location: String = null
                  ,core_dedupe_tgt_current_location: String = null
                  ,coalesce_value : Int = 10
                  ,compression: String = null)
}

