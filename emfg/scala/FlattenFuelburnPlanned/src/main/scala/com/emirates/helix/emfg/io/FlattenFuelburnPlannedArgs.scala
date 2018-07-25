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

object FlattenFuelburnPlannedArgs {

  case class Args(fuelburnplanned_src_location: String = null
                  , control_file: String = null
                  , fuelburnplanned_tgt_serving_location: String = null
                  , coalesce_value: Int = 10
                  , compression: String = null)

}