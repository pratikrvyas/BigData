/*----------------------------------------------------------------------------
 * Created on  : 01/Mar/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PayoadLoadsheetSchemaSyncArguments.scala
 * Description : Class file for input arg
 * ----------------------------------------------------------
 */
package com.emirates.helix



/**
  * Model class with different case classes for supporting operations
  */




object PayoadLoadsheetSchemaSyncArguments {

  case class Args(history_src_clc_flight_dtls: String = null
                  ,history_src_clc_cabin_sec_info: String = null
                  ,history_src_clc_crew_figures: String = null
                  ,history_src_clc_leg_info: String = null
                  ,history_src_clc_pax_bag_total: String = null
                  ,history_src_clc_pax_figures: String = null
                  ,history_src_macs_load_sheet_data: String = null
                  ,history_src_macs_cargo_dstr_cmpt: String = null
                  ,tgt_payload_history_sync_location: String = null
                  ,coalesce_value : Int = 10
                  ,compression: String = null)

}
