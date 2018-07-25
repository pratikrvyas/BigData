

package com.emirates.helix


import com.emirates.helix.PayoadLoadsheetSchemaSyncArguments._
//import org.apache.log4j.

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger


object PayloadLoadsheetSchemaSyncDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(PayloadLoadsheetSchemaSyncDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *
    *              3. coalesce_value
    *              4. compression
    *
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.PayloadLoadsheetSchemaSyncDriver " +
      " payloadLoadsheetSchemaSync-<jar version>.jar") {
      head("PayloadLoadsheetHistorySchemaSync")
      opt[String]('a', "history_src_clc_flight_dtls")
      .required()
        .action((x, config) => config.copy(history_src_clc_flight_dtls = x))
        .text("Required parameter : clc_flight_dtls History Location from Decomposed ")
      opt[String]('b', "history_src_clc_cabin_sec_info")
      .required()
        .action((x, config) => config.copy(history_src_clc_cabin_sec_info = x))
        .text("Required parameter : clc_cabin_sec_info History Location from Decomposed ")
      opt[String]('c', "history_src_clc_crew_figures")
      .required()
        .action((x, config) => config.copy(history_src_clc_crew_figures = x))
        .text("Required parameter : clc_crew_figures History Location from Decomposed ")
      opt[String]('d', "history_src_clc_leg_info")
      .required()
        .action((x, config) => config.copy(history_src_clc_leg_info = x))
        .text("Required parameter : clc_leg_info History Location from Decomposed ")
      opt[String]('e', "history_src_clc_pax_bag_total")
      .required()
        .action((x, config) => config.copy(history_src_clc_pax_bag_total = x))
        .text("Required parameter : clc_pax_bag_total History Location from Decomposed ")
      opt[String]('f', "history_src_clc_pax_figures")
      .required()
        .action((x, config) => config.copy(history_src_clc_pax_figures = x))
        .text("Required parameter : clc_pax_figures History Location from Decomposed ")
      opt[String]('g', "history_src_macs_load_sheet_data")
      .required()
        .action((x, config) => config.copy(history_src_macs_load_sheet_data = x))
        .text("Required parameter : macs_load_sheet_data History Location from Decomposed ")
      opt[String]('h', "history_src_macs_cargo_dstr_cmpt")
      .required()
        .action((x, config) => config.copy(history_src_macs_cargo_dstr_cmpt = x))
        .text("Required parameter : macs_cargo_dstr_cmpt History Location from Decomposed ")
      opt[String]('o', "tgt_payload_history_sync_location")
        .required()
        .action((x, config) => config.copy(tgt_payload_history_sync_location = x))
        .text("Required parameter : Payload History Location for Schema Sync - Modeled ")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : coalesce value ")
      opt[String]('q', "compression")
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
      val processor = PayoadLoadsheetSchemaSyncProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "Payload")
      val temp_df  = processor.syncSchema(config.history_src_clc_flight_dtls,config.history_src_clc_cabin_sec_info,config.history_src_clc_crew_figures,config.history_src_clc_leg_info,config.history_src_clc_pax_bag_total,config.history_src_clc_pax_figures,config.history_src_macs_load_sheet_data,config.history_src_macs_cargo_dstr_cmpt)
      if(temp_df.count > 0){
        processor.writePayloadHistorySync(temp_df,config.coalesce_value,config.tgt_payload_history_sync_location)
      }
      else
        processor.writePayloadHistorySync(temp_df,config.coalesce_value,config.tgt_payload_history_sync_location)

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }

}
