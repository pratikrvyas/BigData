/*----------------------------------------------------------------------------
 * Created on  : 02/26/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : ActualFuelBurnArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.actualfuelburnarguments

/**
  * Model class with different case classes for supporting operations
  */

object ActualFuelBurnArgs {
  case class Args(actualfuelburn_src_ags_route_dedupe_location: String = null
                 ,flightmaster_current_location: String = null
                  ,actualfuelburn_tgt_history_location: String = null
                  ,actualfuelburn_tgt_current_location: String = null
                  ,actualfuelburn_tgt_ags_route_rejected_location: String = null
                  ,coalesce_value : Int = 10
                  ,lookback_months : Int = 3
                  ,compression: String = null)
}

