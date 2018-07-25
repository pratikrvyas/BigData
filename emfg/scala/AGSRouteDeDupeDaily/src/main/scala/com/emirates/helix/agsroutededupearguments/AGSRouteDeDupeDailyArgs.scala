/*----------------------------------------------------------------------------
 * Created on  : 02/19/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSRouteDeDupeDailyArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.agsroutededupearguments

/**
  * Model class with different case classes for supporting operations
  */

object AGSRouteDeDupeArgs {
  case class Args(ags_src_route_dedupe_current_location: String = null
                  ,ags_src_route_incremental_location: String = null
                  ,ags_tgt_route_dedupe_current_location: String = null
                  ,coalesce_value : Int = 10
                  ,compression: String = null)
}

