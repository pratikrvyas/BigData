/*----------------------------------------------------------------------------
 * Created on  : 02/19/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AgsRouteDeDupeDriver.scala
 * Description : Scala application main class file which processes AGS Route
 *               dedupe daily
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.agsroutededupearguments.AGSRouteDeDupeArgs._
import org.apache.log4j.Logger

/**
  * AgsReflineModeledDriver class with main method for starting point for the app
  */
object AgsRouteDeDupeDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(AgsRouteDeDupeDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. ags_src_route_dedupe_current_location
    *              2. ags_src_route_incremental_location
    *              3. ags_tgt_route_dedupe_current_location
    *              4. coalesce_value
    *              5. compression
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.AgsRouteDeDupeDriver " +
      " agsroutededupedaily-<jar version>.jar") {
      head("AgsRouteDeDupe")
      opt[String]('i', "ags_src_route_history_sync_location")
        .required()
        .action((x, config) => config.copy(ags_src_route_dedupe_current_location = x))
        .text("Required parameter : AGS route History Sync Location ")
      opt[String]('j', "ags_src_route_incremental_location")
        .required()
        .action((x, config) => config.copy(ags_src_route_incremental_location = x))
        .text("Required parameter : AGS route Incremental Data Location ")
      opt[String]('x', "ags_tgt_route_dedupe_current_location")
        .required()
        .action((x, config) => config.copy(ags_tgt_route_dedupe_current_location = x))
        .text("Required parameter : AGS route DeDupe Current Target Location ")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : AGS route DeDupe coalesce value ")
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
      val processor = AgsRouteDedupeProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "AgsRouteDeDupe")

      processor.writeAGSDeDupe(processor.deDupeAGSRoute(processor.readAGSRoute),
        config.coalesce_value, config.ags_tgt_route_dedupe_current_location)
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
