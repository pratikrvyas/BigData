/*----------------------------------------------------------
 * Created on  : 21/03/2018
 * Author      : Krishnaraj Rajagopal
 * Email       : krishnaraj.rajagopal@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : SparkUtil.scala
 * Description : Class file for Flatten Fuel burn Planned
 * ----------------------------------------------------------
 */

package com.emirates.helix.emfg

import com.emirates.helix.emfg.io.FlattenFuelburnPlannedArgs._
import org.apache.log4j.Logger

/**
  * FlattenFuelburnPlannedDriver class with main method for starting point for the app
  */
object FlattenFuelburnPlannedDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger: Logger = Logger.getLogger(FlattenFuelburnPlannedDriver.getClass)

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

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.FlattenFuelburnPlannedDriver " +
      " flattenfuelburnplanned-<jar version>.jar") {
      head("FlattenFuelburnPlanned")
      opt[String]('i', "fuelburnplanned_src_location")
        .required()
        .action((x, config) => config.copy(fuelburnplanned_src_location = x))
        .text("Required parameter : FuelburnPlanned Satellite Source Location ")
      opt[String]('j', "control_file")
        .required()
        .action((x, config) => config.copy(control_file = x))
        .text("Required parameter : Control File Path ")
      opt[String]('x', "fuelburnplanned_tgt_serving_location")
        .required()
        .action((x, config) => config.copy(fuelburnplanned_tgt_serving_location = x))
        .text("Required parameter : FuelburnPlanned Target Location of Serving Layer")
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
      val processor = FlattenFuelburnPlannedProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "FlattenFuelburnPlanned")

        processor.processFlattenFuelburnPlannedBase(config.fuelburnplanned_src_location,
          config.fuelburnplanned_tgt_serving_location, config.control_file, config.coalesce_value)

      logger.info("[INFO] SUCCESS ")

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
