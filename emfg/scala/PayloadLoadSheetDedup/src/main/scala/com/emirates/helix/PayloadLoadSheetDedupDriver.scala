/*----------------------------------------------------------------------------
 * Created on  : 06/Mar/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PayloadLoadSheetDedupDriver.scala
 * Description : Scala application main class file which dedupe Payload loadsheet
 *
 * ---------------------------------------------------------------------------*/

package com.emirates.helix

import com.emirates.helix.PayloadLoadsheetDedupArguments._
import org.apache.log4j.Logger


object PayloadLoadSheetDedupDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(PayloadLoadSheetDedupDriver.getClass)

  def main(args:Array[String]) : Unit = {

  val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.PayloadLoadSheetDedupDriver " +
    " payload_loadsheet_dedup-<jar version>.jar") {
    head("PayloadLoadsheetDedup")
    opt[String]('i', "payload_src_history_sync_location")
      .required()
      .action((x, config) => config.copy(payload_src_history_sync_location = x))
      .text("Required parameter : Payload Loadsheet Dedupe History Location")
    opt[String]('j', "payload_src_loadsheet_incremental_location")
      .required()
      .action((x, config) => config.copy(payload_src_loadsheet_incremental_location = x))
      .text("Required parameter : Payload Loadsheet Dedup Incremental Location ")
    opt[String]('x', "payload_tgt_loadsheet_dedupe_history_location")
      .required()
      .action((x, config) => config.copy(payload_tgt_loadsheet_dedupe_history_location = x))
      .text("Required parameter : Payload Loadsheet Dedup Target Location ")
    opt[String]('y', "payload_tgt_loadsheet_dedupe_current_location")
      .required()
      .action((x, config) => config.copy(payload_tgt_loadsheet_dedupe_current_location = x))
      .text("Required parameter : Payload Loadsheet Dedup Current Target Location ")
    opt[Int]('p', "coalesce_value")
      .optional()
      .action((x, config) => config.copy(coalesce_value = x))
      .text("Optional parameter : Payload Loadsheet Dedup coalesce value ")
    opt[Int]('m', "lookback_months")
      .optional()
      .action((x, config) => config.copy(lookback_months = x))
      .text("Optional parameter : Payload Loadsheet Dedup look back value for segregating history and current ")
    opt[String]('n', "data_load_mode")
      .optional()
      .action((x, config) => config.copy(data_load_mode = x))
      .text("Optional parameter : Payload Loadsheet Dedup check value for history or incremental load ")
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
      val processor = PayloadLoadsheetDedupProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "PayloadLoadsheetDedup")


      processor.writePayloadLoadsheetDeDupe(processor.deDupPayloadLoadsheet(processor.readPayloadLoadsheetHistoryIncrement),config.lookback_months,config.coalesce_value)

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }

}
}
