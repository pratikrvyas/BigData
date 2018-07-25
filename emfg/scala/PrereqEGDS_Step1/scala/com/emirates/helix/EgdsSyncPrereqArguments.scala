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
package com.emirates.helix

/**
  * Model class with different case classes for supporting operations
  */

object EgdsSyncPrereqArguments {

  case class Args(path_to_egds_incremental: String = null
                  ,path_to_flight_master_current: String = null
                  ,path_to_edgs_prerequisite: String = null
                  ,path_to_edgs_rejected: String = null
                  ,date_for_folder: String = null
                  ,time_period:Integer=1800
                  ,coalesce_value : Int = 10
                  ,compression: String = null)

}
