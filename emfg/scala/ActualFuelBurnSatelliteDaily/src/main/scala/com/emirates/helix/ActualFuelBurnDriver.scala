/*----------------------------------------------------------------------------
 * Created on  : 03/01/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : ActualFuelBurnDriver.scala
 * Description : Scala application main class file which processes AGS Route
 *               deduped data and populate the Actual Fuel Burn Satellite.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.actualfuelburnarguments.ActualFuelBurnArgs._
import org.apache.log4j.Logger

/**
  * ActualFuelBurnDriver class with main method for starting point for the app
  */
object ActualFuelBurnDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(ActualFuelBurnDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. actualfuelburn_src_ags_route_dedupe_location
    *              2. flightmaster_current_location
    *              3. actualfuelburn_tgt_current_location
    *              4. actualfuelburn_tgt_ags_route_rejected_location
    *              5. coalesce_value
    *              6. compression
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.ActualFuelBurnDriver " +
      " actualfuelburnsatellitedaily-<jar version>.jar") {
      head("ActualFuelBurnSatelliteDaily")
      opt[String]('i', "actualfuelburn_src_ags_route_dedupe_location")
        .required()
        .action((x, config) => config.copy(actualfuelburn_src_ags_route_dedupe_location = x))
        .text("Required parameter : Actual Fuel Burn Source or AGS route Deduped Location ")
      opt[String]('j', "flightmaster_current_location")
        .required()
        .action((x, config) => config.copy(flightmaster_current_location = x))
        .text("Required parameter : Flight Master current Location ")
      opt[String]('x', "actualfuelburn_tgt_current_location")
        .required()
        .action((x, config) => config.copy(actualfuelburn_tgt_current_location = x))
        .text("Required parameter : Actual Fuel Burn Current Target Location ")
      opt[String]('y', "actualfuelburn_tgt_ags_route_rejected_location")
        .required()
        .action((x, config) => config.copy(actualfuelburn_tgt_ags_route_rejected_location = x))
        .text("Required parameter : Actual Fuel Burn Rejected Records Location ")
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
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Args()) map { config =>
      val processor = ActualFuelBurnProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "ActualFuelBurnSatelliteDaily")

      processor.writeActualFuelBurn(processor.processActualFuelBurn(
        processor.prepareActualFuelBurnBase(config.actualfuelburn_tgt_ags_route_rejected_location,config.coalesce_value)),
        config.actualfuelburn_tgt_current_location, config.coalesce_value)
      logger.info("completed writeActualFuelBurn")

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
