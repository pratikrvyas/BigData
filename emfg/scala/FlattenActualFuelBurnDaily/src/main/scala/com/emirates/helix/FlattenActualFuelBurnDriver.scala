/*----------------------------------------------------------------------------
 * Created on  : 03/05/2018
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

import com.emirates.helix.flattenactualfuelburnarguments.FlattenActualFuelBurnArgs._
import org.apache.log4j.Logger

/**
  * ActualFuelBurnDriver class with main method for starting point for the app
  */
object FlattenActualFuelBurnDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(FlattenActualFuelBurnDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. actualfuelburn_src_location
    *              2. control_file
    *              3. actualfuelburn_tgt_serving_location
    *              4. coalesce_value
    *              5. compression
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.FlattenActualFuelBurnDriver " +
      " flattenactualfuelburn-<jar version>.jar") {
      head("FlattenActualFuelBurn")
      opt[String]('i', "actualfuelburn_src_location")
        .required()
        .action((x, config) => config.copy(actualfuelburn_src_location = x))
        .text("Required parameter : Actual Fuel Burn Satellite Source Location ")
      opt[String]('j', "control_file")
        .required()
        .action((x, config) => config.copy(control_file = x))
        .text("Required parameter : Control File Path ")
      opt[String]('x', "actualfuelburn_tgt_serving_location")
        .required()
        .action((x, config) => config.copy(actualfuelburn_tgt_serving_location = x))
        .text("Required parameter : Actual Fuel Burn Target Location of Serving Layer")
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
      val processor = FlattenActualFuelBurnProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "FlattenActualFuelBurn")

      processor.processFlattenActualFuelBurnBase(config.actualfuelburn_src_location,
        config.actualfuelburn_tgt_serving_location, config.control_file, config.coalesce_value)

      logger.info("[INFO] SUCCESS ")

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
