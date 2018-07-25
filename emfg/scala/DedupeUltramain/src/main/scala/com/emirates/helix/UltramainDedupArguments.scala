/*----------------------------------------------------------------------------
 * Created on  : 10/May/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UltramainDedupArguments.scala
 * Description : Class file for input parameter
 * ---------------------------------------------------------------------------*/

package com.emirates.helix

object UltramainDedupArguments {
  case class Args(ultramain_src_history_sector_location : String = null
                  ,ultramain_src_history_uvalue_location: String = null
                  ,ultramain_src_incr_sector_location : String = null
                  ,ultramain_src_incr_uvalue_location: String = null
                  ,ultramain_src_dedupe_current_location: String  = null
                  ,ultramain_tgt_dedupe_history_location: String  = null
                  ,ultramain_tgt_dedupe_current_location: String  = null
                  ,coalesce_value : Int = 10
                  ,lookback_months : Int = 3
                  ,data_load_mode:String=null
                  ,compression: String = null)
}
