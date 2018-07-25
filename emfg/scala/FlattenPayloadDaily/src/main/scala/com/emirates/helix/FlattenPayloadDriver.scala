/*----------------------------------------------------------------------------
 * Created on  : 03/05/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : FlattenPayloadDriver.scala
 * Description : Scala application main class file which processes AGS Route
 *               deduped data and populate the Payload Satellite.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.flattenpayloadarguments.FlattenPayloadArgs._
import org.apache.log4j.Logger

/**
  * FlattenPayloadDriver class with main method for starting point for the app
  */
object FlattenPayloadDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(FlattenPayloadDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. payload_src_location
    *              2. control_file
    *              3. payload_tgt_serving_location
    *              4. coalesce_value
    *              5. compression
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.FlattenPayloadDriver " +
      " flattenpayload-<jar version>.jar") {
      head("FlattenPayload")
      opt[String]('i', "payload_src_location")
        .required()
        .action((x, config) => config.copy(payload_src_location = x))
        .text("Required parameter : Payload Satellite Source Location ")
      opt[String]('j', "control_file")
        .required()
        .action((x, config) => config.copy(control_file = x))
        .text("Required parameter : Control File Path ")
      opt[String]('x', "payload_tgt_serving_location")
        .required()
        .action((x, config) => config.copy(payload_tgt_serving_location = x))
        .text("Required parameter : Payload Target Location of Serving Layer")
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
      val processor = FlattenPayloadProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "FlattenPayload")

      processor.processFlattenPayloadBase(config.payload_src_location,
        config.payload_tgt_serving_location, config.control_file, config.coalesce_value)

      logger.info("[INFO] SUCCESS ")

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
