/*----------------------------------------------------------------------------
 * Created on  : 02/14/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSRouteHistorySchemaSyncArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.agsrouteschemasyncarguments

/**
  * Model class with different case classes for supporting operations
  */

object AGSRouteHistorySchemaSyncArgs {
  case class Args(ags_src_route_history_location: String = null
                 ,ags_src_refline_history_location : String = null
                 ,ags_tgt_route_history_sync_location: String = null
                 ,coalesce_value : Int = 10
                 ,compression: String = null)
}
