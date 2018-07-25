/*----------------------------------------------------------
 * Created on  : 12/11/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UDF.scala
 * Description : Scala file to keep all UDF functions
 * ----------------------------------------------------------
 */
package com.emirates.helix.udf

import java.io.Serializable
import org.apache.spark.sql.functions.udf
import java.util.UUID

object UDF extends Serializable{

  /**
    * UDF function for generating unique ID
    *
    */
  val generateUUID =  udf(() => UUID.randomUUID().toString)

  /**
    * UDF function for generating timestamp in seconds
    *
    */
  val generateTimestamp =  udf(() => (System.currentTimeMillis()/1000L).toString)
}
