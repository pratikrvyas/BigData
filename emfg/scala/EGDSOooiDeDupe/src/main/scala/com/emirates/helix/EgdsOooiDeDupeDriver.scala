/*----------------------------------------------------------------------------
 * Created on  : 04/16/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsOooiDeDupeDriver.scala
 * Description : Scala application main class file which processes EGDS Oooi
 *               Dedupe.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.egdsoooidedupearguments.EGDSOooiDeDupeArgs._
import org.apache.log4j.Logger

/**
  * EgdsOooiDeDupeDriver class with main method for starting point for the app
  */
object EgdsOooiDeDupeDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(EgdsOooiDeDupeDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. egds_oooi_src_history_location
    *              2. egds_oooi_src_incremental_location
    *              3. egds_oooi_tgt_dedupe_history_location
    *              4. egds_oooi_tgt_dedupe_current_location
    *              5. coalesce_value
    *              6. lookback_months
    *              7. compression
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.EgdsOooiDeDupeDriver " +
      " egdsoooidedupe-<jar version>.jar") {
      head("EgdsOooiDeDupe")
      opt[String]('i', "egds_oooi_src_history_sync_location")
        .required()
        .action((x, config) => config.copy(egds_oooi_src_history_sync_location = x))
        .text("Required parameter : EGDS Oooi History Sync Location ")
      opt[String]('j', "egds_oooi_src_incremental_location")
        .required()
        .action((x, config) => config.copy(egds_oooi_src_incremental_location = x))
        .text("Required parameter : EGDS Oooi Incremental Data Location ")
      opt[String]('x', "egds_oooi_tgt_dedupe_history_location")
        .required()
        .action((x, config) => config.copy(egds_oooi_tgt_dedupe_history_location = x))
        .text("Required parameter : EGDS Oooi DeDupe History Target Location ")
      opt[String]('y', "egds_oooi_tgt_dedupe_current_location")
        .required()
        .action((x, config) => config.copy(egds_oooi_tgt_dedupe_current_location = x))
        .text("Required parameter : EGDS Oooi DeDupe Current Target Location ")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : EGDS Oooi DeDupe coalesce value ")
      opt[Int]('m', "lookback_months")
        .optional()
        .action((x, config) => config.copy(lookback_months = x))
        .text("Optional parameter : EGDS Oooi DeDupe look back value for segregating history and current ")
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
      val processor = EgdsOooiDedupeProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "EgdsOooiDeDupe")

      processor.writeEGDSOooiDeDupe(processor.deDupeEGDSOooi(processor.readEGDSOooi),config.lookback_months,
        config.coalesce_value, config.egds_oooi_tgt_dedupe_history_location, config.egds_oooi_tgt_dedupe_current_location)
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
