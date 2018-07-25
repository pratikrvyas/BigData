/*----------------------------------------------------------------------------
 * Created on  : 03/14/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlattenPayloadArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */

package com.emirates.helix.flattenpayloadarguments

/**
  * Model class with different case classes for supporting operations
  */

object FlattenPayloadArgs {
  case class Args(payload_src_location: String = null
                   ,control_file: String=null
                  ,payload_tgt_serving_location: String = null
                  ,coalesce_value : Int = 10
                  ,compression: String = null)
}