/*----------------------------------------------------------------------------
 * Created on  : 10/May/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UltramainDedupDriver.scala
 * Description : Scala application main class file which dedupe Ultramain
 *
 * ---------------------------------------------------------------------------*/

package com.emirates.helix

import com.emirates.helix.UltramainDedupArguments._
import org.apache.log4j.Logger

object UltramainDedupDriver {


  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(UltramainDedupDriver.getClass)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.UltramainDedupDriver " +
      " ultramain_dedupe-<jar version>.jar") {
      head("UltramainDedup")
      opt[String]('i', "ultramain_src_history_sector_location")
        .required()
        .action((x, config) => config.copy(ultramain_src_history_sector_location = x))
        .text("Required parameter : Ultramain Sector History Location")
      opt[String]('j', "ultramain_src_history_uvalue_location")
        .required()
        .action((x, config) => config.copy(ultramain_src_history_uvalue_location = x))
        .text("Required parameter : Ultramain Uvalue History Location ")
      opt[String]('k', "ultramain_src_incr_sector_location")
        .required()
        .action((x, config) => config.copy(ultramain_src_incr_sector_location = x))
        .text("Required parameter : Ultramain Sector Incremental Location")
      opt[String]('l', "ultramain_src_incr_uvalue_location")
        .required()
        .action((x, config) => config.copy(ultramain_src_incr_uvalue_location = x))
        .text("Required parameter : Ultramain Uvalue Incremental Location ")
      opt[String]('l', "ultramain_src_dedupe_current_location")
        .required()
        .action((x, config) => config.copy(ultramain_src_dedupe_current_location = x))
        .text("Required parameter : Ultramain  Dedupe current  Location ")
      opt[String]('x', "ultramain_tgt_dedupe_history_location")
        .required()
        .action((x, config) => config.copy(ultramain_tgt_dedupe_history_location = x))
        .text("Required parameter : Ultramain Target Dedup History Location ")
      opt[String]('y', "ultramain_tgt_dedupe_current_location")
        .required()
        .action((x, config) => config.copy(ultramain_tgt_dedupe_current_location = x))
        .text("Required parameter : Ultramain Target Dedup Current Location ")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : Ultramain Dedup coalesce value ")
      opt[Int]('m', "lookback_months")
        .optional()
        .action((x, config) => config.copy(lookback_months = x))
        .text("Optional parameter : Ultramain Dedup look back value for segregating history and current ")
      opt[String]('n', "data_load_mode")
        .optional()
        .action((x, config) => config.copy(data_load_mode = x))
        .text("Optional parameter : Ultramain Dedup check value for history or incremental load ")
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
      val processor = UltramainDedupProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "UltramainDedup")


      processor.writeUltramainDeDupe(processor.deDupUltramain(processor.processUltramain),config.lookback_months,config.coalesce_value)

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }

  }
}
