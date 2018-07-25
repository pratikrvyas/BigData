/*----------------------------------------------------------------------------
 * Created on  : 03/05/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlattenActualFuelBurnArgs.scala
 * Description : Class file to hold supporting case classes
 * ----------------------------------------------------------
 */
package com.emirates.helix.flattenactualfuelburnarguments

/**
  * Model class with different case classes for supporting operations
  */

object FlattenActualFuelBurnArgs {
  case class Args(actualfuelburn_src_location: String = null
                   ,control_file: String=null
                  ,actualfuelburn_tgt_serving_location: String = null
                  ,coalesce_value : Int = 10
                  ,compression: String = null)
}

