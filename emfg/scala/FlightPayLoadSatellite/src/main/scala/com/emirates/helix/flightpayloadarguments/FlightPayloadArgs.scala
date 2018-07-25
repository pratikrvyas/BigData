/*----------------------------------------------------------------------------
 * Created on  : 13/Mar/2018
 * Author      : Pratik Vyas (s795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlightPayloadArgs.scala
 * Description : Class file to hold supporting case classes
 * ---------------------------------------------------------------------------
 */



package com.emiraets.helix.flightpayloadarguments

object FlightPayloadArgs {

  case class Args(payload_src_loadsheet_dedupe_location: String = null
                  ,lidofsumflightlevel_src_dedupe_location: String = null
                  ,ldm_src_dedupe_location: String = null
                  ,flightmaster_current_location: String = null
                  ,payload_tgt_history_location: String = null
                  ,payload_tgt_current_location: String = null
                  ,payload_tgt_loadsheet_rejected_location: String = null
                  ,lidofsumflightlevel_tgt_rejected_location: String = null
                  ,ldm_tgt_rejected_location: String = null
                  ,data_load_mode:String=null
                  ,coalesce_value : Int = 10
                  ,lookback_months : Int = 3
                  ,compression: String = null)

}