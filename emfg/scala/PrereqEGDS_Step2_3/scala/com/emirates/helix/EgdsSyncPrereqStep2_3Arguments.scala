/*----------------------------------------------------------------------------
 * Created on  : 01/Mar/2018
 * Author      : Oleg Baydakov(S754344)
 * Email       : oleg.baydakov@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsSyncPrereqArgumentsArguments.scala
 * Description : Class file for input arg
 * ----------------------------------------------------------
 */
/**
  * Model class with different case classes for supporting operations
  */

package com.emirates.helix

object EgdsSyncPrereqArguments {

  case class Args( path_to_flight_master_current: String = null
                  ,path_to_edgs_prerequisite: String = null
                  ,path_to_taxi_fuel_flow: String = null
                  ,path_to_ags_current: String = null
                  ,path_to_altea_current: String = null
                  ,number_of_months:Int = 3
                  ,time_period:Integer=1800
                  ,coalesce_value : Int = 10
                  ,compression: String = null)

}
