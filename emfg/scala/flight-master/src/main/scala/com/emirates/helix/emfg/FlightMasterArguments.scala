/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Krishnaraj Rajagopal
 * Version     : 1.0
 * Project     : Helix-OpsEfficiency
 * Filename    : FlightMasterArguments.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.emfg

/**
  * Object which holds Flight Master's arguments
  */

object FlightMasterArguments {

  case class Args(core_input: String = null
                  , ags_input: String = null
                  , dmis_input: String = null
                  , lido_input: String = null
                  , compression: String = null
                  , coalesce_value: Int = 20
                  , broadcast_size: String = "1073741824"
                  , look_back_months: Int = 3
                  , output_history_location: String = null
                  , output_current_location: String = null
                  , aircraft_country_region_path: String = null
                  , flight_service_path: String = null
                  , flight_status_path: String = null
                  , flight_delay_path: String = null
                  , data_load_mode: String = null
                  , dmis_baydetail_input: String = null
                  , baychange_lookup: String = null
                  , concourse_lookup: String = null
                  , aircraft_details_lookup: String = null
                 )

}
