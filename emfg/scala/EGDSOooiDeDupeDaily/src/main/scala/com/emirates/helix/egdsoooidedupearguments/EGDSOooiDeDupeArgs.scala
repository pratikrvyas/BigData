/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EGDSOooiDeDupeArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.egdsoooidedupearguments

/**
  * Model class with different case classes for supporting operations
  */

object EGDSOooiDeDupeArgs {
  case class Args(egds_oooi_src_dedupe_current_location: String = null
                  ,egds_oooi_src_incremental_location: String = null
                  ,egds_oooi_tgt_dedupe_current_location: String = null
                  ,coalesce_value : Int = 10
                  ,lookback_months : Int = 3
                  ,compression: String = null)
}

