/*----------------------------------------------------------------------------
 * Created on  : 06/Mar/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PayloadLoadsheetDedupArguments.scala
 * Description : Class file for input parameter
 * ---------------------------------------------------------------------------*/

package com.emirates.helix

object PayloadLoadsheetDedupArguments {

  case class Args(payload_src_history_sync_location: String = null
                  ,payload_src_loadsheet_incremental_location: String = null
                  ,payload_tgt_loadsheet_dedupe_history_location: String = null
                  ,payload_tgt_loadsheet_dedupe_current_location: String = null
                  ,coalesce_value : Int = 10
                  ,lookback_months : Int = 3
                   ,data_load_mode:String=null
                  ,compression: String = null)

}
