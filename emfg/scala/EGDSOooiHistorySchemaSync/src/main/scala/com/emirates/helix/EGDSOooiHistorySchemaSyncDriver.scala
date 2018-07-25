/*----------------------------------------------------------------------------
 * Created on  : 04/16/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EGDSOooiHistorySchemaSyncDriver.scala
 * Description : Scala application main class file which process EGDS Oooi
 *               history and incremental data for schema sync.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.egdsoooischemasyncarguments.EGDSOooiHistorySchemaSyncArgs._
import org.apache.log4j.Logger

/**
  * EGDSOooiHistorySchemaSyncDriver class with main method for starting point for the app
  */
object EGDSOooiHistorySchemaSyncDriver {

  private val compress_rex = """^snappy$|^deflate$""".r
  private lazy val logger:Logger = Logger.getLogger(EGDSOooiHistorySchemaSyncDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. egds_oooi_src_history_location
    *              2. egds_oooi_tgt_history_sync_location
    *              3. coalesce_value
    *              4. compression
    *
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.EGDSOooiHistorySchemaSyncDriver " +
      " egdsoooihistoryschemasync-<jar version>.jar") {
      head("EGDSOooiHistorySchemaSync")
      opt[String]('i', "egds_oooi_src_history_location")
        .required()
        .action((x, config) => config.copy(egds_oooi_src_history_location = x))
        .text("Required parameter : EGDS Oooi History Location from Decomposed ")
      opt[String]('o', "egds_oooi_tgt_history_sync_location")
        .required()
        .action((x, config) => config.copy(egds_oooi_tgt_history_sync_location = x))
        .text("Required parameter : EGDS Oooi History Location for Schema Sync - Modeled ")
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
      val processor = EgdsOooiHistorySchemaSyncProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "EgdsOooiHistorySchemaSync")
      processor.writeEGDSOooiHistorySync(processor.readEGDSOooiHistory,config.coalesce_value)
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
