/*----------------------------------------------------------------------------
 * Created on  : 13/Mar/2018
 * Author      : Pratik Vyas (s795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlightPayloadDriver.scala
 * Description : Scala application main class file which processes Payload loadsheet
 *               deduped, LidoFsum deduped data and populate the Flight Payload Satellite.
 * ---------------------------------------------------------------------------
 */
/*----------------------------------------------------------------------------
 * Modified on  : 23/Apr/2018
 * Author      : Fayaz Shaik (s796466)
 * Email       : fayaz.shaik@dnata.com
 * Description : Updated the arguments list and comments
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emiraets.helix.flightpayloadarguments.FlightPayloadArgs._
import org.apache.log4j.Logger


/**
  * FlightPayloadDriver class with main method for starting point for the app
  */
object FlightPayloadDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(FlightPayloadDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. payload_src_loadsheet_dedupe_location
    *              2. lidofsumflightlevel_src_dedupe_location
    *              3. ldm_src_dedupe_location
    *              4. flightmaster_current_location
    *              5. payload_tgt_history_location
    *              6. payload_tgt_current_location
    *              7. payload_tgt_loadsheet_rejected_location
    *              8. lidofsumflightlevel_tgt_rejected_location
    *              9. ldm_tgt_rejected_location
    *              10. data_load_mode
    *              11. coalesce_value
    *              12. lookback_months
    *              13. compression
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.FlightPayloadDriver " +
      " flightpayloadsatellite-<jar version>.jar") {
      head("FlightPayloadSatellite")
      opt[String]('i', "payload_src_loadsheet_dedupe_location")
        .required()
        .action((x, config) => config.copy(payload_src_loadsheet_dedupe_location = x))
        .text("Required parameter : Payload Deduped Location ")
      opt[String]('j', "lidofsumflightlevel_src_dedupe_location")
        .required()
        .action((x, config) => config.copy(lidofsumflightlevel_src_dedupe_location = x))
        .text("Required parameter : LidoFsum Deduped Location ")
      opt[String]('k', "ldm_src_dedupe_location")
        .required()
        .action((x, config) => config.copy(ldm_src_dedupe_location = x))
        .text("Required parameter : LDM Deduped Location ")
      opt[String]('x', "flightmaster_location")
        .required()
        .action((x, config) => config.copy(flightmaster_current_location = x))
        .text("Required parameter : FlightMaster Location ")
      opt[String]('y', "payload_tgt_history_location")
        .required()
        .action((x, config) => config.copy(payload_tgt_history_location = x))
        .text("Required parameter : FlightPayload Target History Location ")
      opt[String]('z', "payload_tgt_current_location")
        .required()
        .action((x, config) => config.copy(payload_tgt_current_location = x))
        .text("Required parameter : FlightPayload Target Current Location")
      opt[String]('a', "payload_tgt_loadsheet_rejected_location")
        .required()
        .action((x, config) => config.copy(payload_tgt_loadsheet_rejected_location = x))
        .text("Required parameter : FlightPayload Target Rejected Location")
      opt[String]('b', "lidofsumflightlevel_tgt_rejected_location")
        .required()
        .action((x, config) => config.copy(lidofsumflightlevel_tgt_rejected_location = x))
        .text("Required parameter : LidofsumFlightLevel Target Rejected Location")
      opt[String]('d', "ldm_tgt_rejected_location")
        .required()
        .action((x, config) => config.copy(ldm_tgt_rejected_location = x))
        .text("Required parameter : LDM Target Rejected Location")
      opt[String]('n', "data_load_mode")
        .optional()
        .action((x, config) => config.copy(data_load_mode = x))
        .text("Optional parameter : Payload Satellite check value for history or incremental load ")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : Coalesce value ")
      opt[Int]('m', "lookback_months")
        .optional()
        .action((x, config) => config.copy(lookback_months = x))
        .text("Optional parameter : Look back value for segregating history and current ")
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
      help("help") text ("Print this usage message")
    }

    parser.parse(args, Args()) map { config =>
      val processor = FlightPayloadProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "FlightPayloadSatellite")
      logger.info("Initiating the FlightPayloadSatellite Construction")
      processor.writeFlightPayload(
        processor.processFlightPayload(config.payload_src_loadsheet_dedupe_location,
          config.lidofsumflightlevel_src_dedupe_location, config.ldm_src_dedupe_location, config.flightmaster_current_location,
          config.payload_tgt_loadsheet_rejected_location, config.lidofsumflightlevel_tgt_rejected_location,
          config.ldm_tgt_rejected_location, config.coalesce_value),
        config.payload_tgt_history_location,config.payload_tgt_current_location,
        config.coalesce_value,config.lookback_months)
      logger.info("Completed the FlightPayloadSatellite Construction")

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }

  }

}