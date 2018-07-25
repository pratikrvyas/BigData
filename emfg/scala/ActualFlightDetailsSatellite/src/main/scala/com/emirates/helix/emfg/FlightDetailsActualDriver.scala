/*----------------------------------------------------------
 * Created on  : 26/04/2018
 * Author      : Krishnaraj Rajagopal
 * Email       : krishnaraj.rajagopal@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlightDetailsActual.scala
 * Description : Class file for Flight details actual driver
 * ----------------------------------------------------------
 */

package com.emirates.helix.emfg

import com.emirates.helix.emfg.io.FlightDetailsActualArgs._
import com.emirates.helix.emfg.util.{SparkUtils, FightDetailsActualUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * FlightDetailsActualDriver class with main method for starting point for the app
  */
object FlightDetailsActualDriver extends SparkUtils with FightDetailsActualUtils {

  private lazy val logger: Logger = Logger.getLogger(FlightDetailsActualDriver.getClass)
  private val compress_rex = """^snappy$|^deflate$""".r

  /** Main method
    *
    * @param args  Command line arguments
    *              1. fuelburnplanned_src_location
    *              2. control_file
    *              3. fuelburnplanned_tgt_serving_location
    *              4. coalesce_value
    *              5. compression
    * @return Unit
    */
  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.emfg.FlightDetailsActualDriver " +
      " flightdetailsactual-<jar version>.jar") {
      head("FlightDetailsActualDriver")
      opt[String]('f', "flight_master_src_location")
        .required()
        .action((x, config) => config.copy(flight_master_src_location = x))
        .text("Required parameter : Flight master source location ")
      opt[String]('l', "fuelmon_dedupe_location")
        .required()
        .action((x, config) => config.copy(fuelmon_dedupe_location = x))
        .text("Required parameter : Fuelmon Source Location ")
      opt[String]('a', "ags_src_location")
        .required()
        .action((x, config) => config.copy(ags_src_location = x))
        .text("Required parameter : AGS source location")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : Coalesce value ")
      opt[String]('c', "compression")
        .required()
        .valueName("snappy OR deflate")
        .validate(x => {
          if (!(x.matches(compress_rex.toString()))) Left("Invalid compression specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(compression = x))
        .text("Required parameter : Output data compression format, snappy or deflate")
      opt[String]('u', "ultramain_dedupe_location")
        .required()
        .action((x, config) => config.copy(ultramain_dedupe_location = x))
        .text("Required parameter : Ultramain dedupe location")
      opt[String]('m', "mfp_dedupe_location")
        .required()
        .action((x, config) => config.copy(mfp_dedupe_location = x))
        .text("Required parameter : MFP dedupe source location")
      opt[String]('e', "epic_dedupe_location")
        .required()
        .action((x, config) => config.copy(epic_dedupe_location = x))
        .text("Required parameter : EPIC core source location")
      opt[String]('r', "master_epic_rejected_location")
        .required()
        .action((x, config) => config.copy(master_epic_rejected_location = x))
        .text("Required parameter : Master epic rejected location")
      opt[Int]('b', "look_back_months")
        .required()
        .action((x, config) => config.copy(look_back_months = x))
        .text("Required parameter : look_back_months ")
      opt[String]('h', "output_history_location")
        .required()
        .action((x, config) => config.copy(output_history_location = x))
        .text("Required parameter : Output history data location ")
      opt[String]('o', "output_current_location")
        .required()
        .action((x, config) => config.copy(output_current_location = x))
        .text("Required parameter : Output current data location ")
      opt[String]('d', "data_load_mode")
        .required()
        .action((x, config) => config.copy(data_load_mode = x))
        .text("Required parameter : Data load mode location ")
      opt[String]('g', "egds_dedupe_location")
        .required()
        .action((x, config) => config.copy(egds_dedupe_location = x))
        .text("Required parameter : EGDS dedupe location ")
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Args()) map { config =>

      val processor = FlightDetailsActualProcessor(config)

      implicit lazy val spark = processor.getSparkSession(config.compression, "FlightDetailsActual")

      val flight_master_df = readValueFromHDFS(config.flight_master_src_location, "parquet", spark)

      val ags_deduped_df = readValueFromHDFS(config.ags_src_location, "parquet", spark)

      val epic_deduped_df = readValueFromHDFS(config.epic_dedupe_location, "parquet", spark)

      val ultramain_deduped_df = readValueFromHDFS(config.ultramain_dedupe_location, "parquet", spark)

      val mfp_deduped_df = readValueFromHDFS(config.mfp_dedupe_location, "parquet", spark)

      val fuelmon_deduped_df = readValueFromHDFS(config.fuelmon_dedupe_location, "parquet", spark)

      val egds_deduped_df = readValueFromHDFS(config.egds_dedupe_location, "parquet", spark)

      val actual_flight_details_ags_core_df = processor.processAgsAndCore(ags_deduped_df, flight_master_df)

      val actual_flight_details_um_df = processor.processUltramain(ultramain_deduped_df)

      val actual_flight_details_mfp_df = processor.processMFP(mfp_deduped_df)

      val actual_flight_details_df = processor.generateActualFlightDetails(actual_flight_details_ags_core_df, epic_deduped_df, actual_flight_details_um_df,
        actual_flight_details_mfp_df, fuelmon_deduped_df, egds_deduped_df)

      logger.info("[INFO] SUCCESS ")

      if (config.data_load_mode == HISTORY) {
        writeOutput(actual_flight_details_df, config.look_back_months, config.coalesce_value, config.output_history_location, config.output_current_location, ACTUAL_FLIGHT_DETAILS_OUTPUT_TEMP_TABLE, FLIGHT_DATE_COLUMN_NAME, config.compression)
      }
      else {
        flight_master_df.write.format("parquet").option("spark.sql.parquet.compression.codec", config.compression).save(config.output_current_location)
      }

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
