/*----------------------------------------------------------------------------
 * Created on  : 01/04/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AGSProcessor.scala
 * Description : Class file for processing AGS data.
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix.emfg.process.ags

import com.emirates.helix.emfg.FlightMasterArguments.Args
import com.emirates.helix.emfg.common.FlightMasterUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Companion object for CoreProcessor class
  */
object AGSProcessor {

  def apply(args: Args): AGSProcessor = {
    val processor = new AGSProcessor()
    processor.args = args
    processor
  }
}

/**
  * AGS data processing for Flight Master
  */
class AGSProcessor extends FlightMasterUtils {

  private var args: Args = _

  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("AGSProcessor")

  /**
    *
    * @param ags_dedupe_df  Deduped AGS input data frame
    * @param sqlContext
    * @return  AGS processed data
    * @note This method is used to process AGS data for flight master
    */

  def generateAGSDataForFM(ags_dedupe_df: DataFrame)(implicit sqlContext: HiveContext): DataFrame = {

    log.info("Generating AGS data for Flight Master - Invoked AGSProcessor.generateAGSDataForFM method")

    ags_dedupe_df.registerTempTable("ags_dedupe_df_tab")

    val act_runway_details_temp1: DataFrame = sqlContext.sql("select fltdate, flightnumber, tail, origin, destination, rwy_to as act_dep_runway , rwy_ld as act_arr_runway from ags_dedupe_df_tab")

    val act_runway_details_temp2 = toMap(act_runway_details_temp1, COLUMNLIST_ACT_RUNWAY_DETAILS, "act_runway_details")

    return act_runway_details_temp2
  }
}

