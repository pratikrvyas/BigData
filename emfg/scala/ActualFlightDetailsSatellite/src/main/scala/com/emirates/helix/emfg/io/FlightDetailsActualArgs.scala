/*----------------------------------------------------------
 * Created on  : 21/03/2018
 * Author      : Krishnaraj Rajagopal
 * Email       : krishnaraj.rajagopal@dnata.com
 * Version     : 1.1
 * Project     : Helix-OpsEfficnecy
 * Filename    : SparkUtil.scala
 * Description : Class file for Flatten Fuel burn Planned Argument
 * ----------------------------------------------------------
 */

package com.emirates.helix.emfg.io

/**
  * Model class with different case classes for supporting operations
  */

object FlightDetailsActualArgs {

  case class Args(flight_master_src_location: String = null,
                  fuelmon_dedupe_location: String = null,
                  ags_src_location: String = null,
                  coalesce_value: Int = 10,
                  compression: String = null,
                  ultramain_dedupe_location: String = null,
                  mfp_dedupe_location: String = null,
                  epic_dedupe_location: String = null,
                  master_epic_rejected_location: String = null,
                  look_back_months: Int = 3,
                  output_history_location: String = null,
                  output_current_location: String = null,
                  data_load_mode: String = null,
                  egds_dedupe_location:String = null)

}