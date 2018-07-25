/*----------------------------------------------------------------------------
 * Created on  : 01/Mar/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PayoadLoadsheetSchemaSyncArguments.scala
 * Description : Class file for UDF
 * ----------------------------------------------------------
 */

package com.emirates.helix.udf

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

object UDF {

/*
  // Calculate  load for each compartment
  def compartmentLoadTot(text: String) = {
    var counts = 0
    var loads=0
    for (load <- text.trim.split(" ")) {
      if (counts > 0 && counts%2!=0) {
        loads = loads+load.toInt
      }
      counts = counts +1
    }
    loads
  }
  */

  def compartmentLoadTot(text: String) = {
    var counts = 0
    var loads=0
    for (load <- text.trim.split(" ")) {
      if (counts > 0 && counts%2!=0 && load.toString.trim.length > 0) {
        loads = loads+load.toInt
      }
      counts = counts +1
    }
    loads
  }

// Calculate total load in all compartments
  def compartmentLoad(text: String) = {
    var counts = 0
    var loads=""
    for (load <- text.trim.split(" ")) {
      if (counts > 0) {
        loads = loads
        if(counts%2==0) loads = loads+" "+load
        else loads=loads+"/"+load
      }
      else {loads = load}
      counts = counts +1
    }
    loads
  }

  val udf_compartmentLoad = udf(compartmentLoad(_:String))

  val udf_compartmentLoadTot = udf(compartmentLoadTot(_:String))


}
