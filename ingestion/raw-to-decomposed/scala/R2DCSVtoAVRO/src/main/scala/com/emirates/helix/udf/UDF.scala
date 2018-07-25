/*----------------------------------------------------------
 * Created on  : 13/12/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UDF.scala
 * Description : Class file for Spark UDF functions
 * ----------------------------------------------------------
 */

package com.emirates.helix.udf

import java.io.Serializable
import java.util.UUID
import org.apache.spark.sql.functions._

object UDF extends Serializable {
  /**
    * UDF function for generating unique ID
    * @return String Random uuid
    *
    */
  val generateUUID =  udf(() => UUID.randomUUID().toString)

  /**
    * UDF function for generating timestamp in seconds
    * @return String timestamp as string
    *
    */
  val generateTimestamp =  udf(() => (System.currentTimeMillis()/1000L).toString)
}
