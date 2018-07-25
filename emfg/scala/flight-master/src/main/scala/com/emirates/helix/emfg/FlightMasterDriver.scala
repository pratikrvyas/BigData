/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Krishnaraj Rajagopal
 * Version     : 1.0
 * Project     : Helix-OpsEfficiency
 * Filename    : FlightMasterDriver.scala
 * Description : Driver class for Flight Master
 * ----------------------------------------------------------
 */
package com.emirates.helix.emfg

import com.databricks.spark.avro._
import com.emirates.helix.emfg.FlightMasterArguments.Args
import com.emirates.helix.emfg.common.{FlightMasterUtils, SparkUtils}
import com.emirates.helix.emfg.process._
import com.emirates.helix.emfg.process.ags.AGSProcessor
import com.emirates.helix.emfg.process.core.{CoreProcessor, DivertedFlightProcessor}
import com.emirates.helix.emfg.process.dmis.DMISProcessor
import com.emirates.helix.emfg.process.lido.LidoFSumProcessor
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext


/**
  * CORE data population to Flight Master
  */
object FlightMasterDriver extends SparkUtils with FlightMasterUtils {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("FlightMasterDriver")
  private val compress_rex = """^snappy$|^deflate$""".r

  /**
    *
    * @param args List of arguments
    */
  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.CoreMasterDriver " +
      " CoreMaster-<jar version>.jar") {
      head("CoreMasterDriver")
      opt[String]('c', "core_input")
        .required()
        .action((x, config) => config.copy(core_input = x))
        .text("Required parameter : CORE input data location ")
      opt[String]('a', "ags_input")
        .required()
        .action((x, config) => config.copy(ags_input = x))
        .text("Required parameter : AGS input data location ")
      opt[String]('d', "dmis_input")
        .required()
        .action((x, config) => config.copy(dmis_input = x))
        .text("Required parameter : DMIS input data location ")
      opt[String]('l', "lido_input")
        .required()
        .action((x, config) => config.copy(lido_input = x))
        .text("Required parameter : LIDO input data location ")

      opt[String]('q', "aircraft_country_region_path")
        .required()
        .action((x, config) => config.copy(aircraft_country_region_path = x))
        .text("Required parameter : Aircraft_country_region_path input data location ")
      opt[String]('t', "flight_service_path")
        .required()
        .action((x, config) => config.copy(flight_service_path = x))
        .text("Required parameter : Flight_service_path input data location ")
      opt[String]('f', "flight_status_path")
        .required()
        .action((x, config) => config.copy(flight_status_path = x))
        .text("Required parameter : Flight_status_path input data location ")
      opt[String]('u', "flight_delay_path")
        .required()
        .action((x, config) => config.copy(flight_delay_path = x))
        .text("Required parameter : Flight_delay_path input data location ")
      opt[Int]('m', "look_back_months")
        .required()
        .action((x, config) => config.copy(look_back_months = x))
        .text("Required parameter : look_back_months ")
      opt[String]('h', "output_history_location")
        .required()
        .action((x, config) => config.copy(output_history_location = x))
        .text("Required parameter : Output history data location ")
      opt[String]('o', "output_current_location")
        .required()
        .action((x, config) => config.copy(output_current_location = x))
        .text("Required parameter : Output current data location ")
      opt[String]('s', "compression")
        .required()
        .valueName("snappy OR deflate")
        .validate(x => {
          if (!x.matches(compress_rex.toString())) Left("Invalid compression specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(compression = x))
        .text("Required parameter : Output data compression format, snappy or deflate")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Required parameter : Coalesce value ")
      opt[String]('b', "broadcast_size")
        .required()
        .action((x, config) => config.copy(broadcast_size = x))
        .text("Required parameter : Auto Broadcast Join Threshold ")
      opt[String]('r', "data_load_mode")
        .required()
        .action((x, config) => config.copy(data_load_mode = x))
        .text("Required parameter : Data load mode location ")
      opt[String]('x', "dmis_baydetail_input")
        .required()
        .action((x, config) => config.copy(dmis_baydetail_input = x))
        .text("Required parameter : DMIS bay detail input ")
      opt[String]('y', "baychange_lookup")
        .required()
        .action((x, config) => config.copy(baychange_lookup = x))
        .text("Required parameter : Bay change lookup ")
      opt[String]('z', "concourse_lookup")
        .required()
        .action((x, config) => config.copy(concourse_lookup = x))
        .text("Required parameter : Concourse lookup ")
      opt[String]('k', "aircraft_details_lookup")
        .required()
        .action((x, config) => config.copy(aircraft_details_lookup = x))
        .text("Required parameter : Aircraft details lookup")
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Args()) map { config =>

      val divertedFlightProcessor = DivertedFlightProcessor(config)

      implicit lazy val sc = new SparkContext(sparkConfig("Flight Master").set("spark.sql.avro.compression.codec", config.compression).set("spark.sql.parquet.compression.codec", config.compression).set("spark.sql.autoBroadcastJoinThreshold", config.broadcast_size))

      sc.setLogLevel("INFO")

      implicit lazy val sqlContext: HiveContext = new HiveContext(sc)

      //CORE Processing

      val core_input_df = sqlContext.read.format("parquet").load(config.core_input)

      val core_diverted_df = divertedFlightProcessor.processDivertedFlight(core_input_df)

      val numberOfPartitions = config.coalesce_value

      // load the master tables

      val aircraft_country_region_df = sqlContext.read.avro(config.aircraft_country_region_path)

      val flight_service_df = sqlContext.read.avro(config.flight_service_path)

      val flight_status_master_df = sqlContext.read.avro(config.flight_status_path)

      val flight_delay_master_df = sqlContext.read.avro(config.flight_delay_path)

      val aircraft_details_master_df = sqlContext.read.avro(config.aircraft_details_lookup)

      val coreProcessor = CoreProcessor(config)

      val core_fm_df = coreProcessor.generateCOREDataForFM(core_diverted_df, aircraft_country_region_df, flight_service_df, flight_status_master_df, flight_delay_master_df, aircraft_details_master_df, numberOfPartitions)


      //AGS Processing

      val ags_dedupe_df = sqlContext.read.parquet(config.ags_input)

      val agsProcessor = AGSProcessor(config)

      val ags_fm_df = agsProcessor.generateAGSDataForFM(ags_dedupe_df)


      //DMIS Processing

      val dmisProcessor = DMISProcessor(config)

      val core_dmis_fm_df = dmisProcessor.generateDMISDataForFM(config, core_fm_df)

      //LIDO Processing

      val lido_dedupe_df = sqlContext.read.parquet(config.lido_input)

      val lidoFSumProcessor = LidoFSumProcessor(config)

      val lido_fm_df = lidoFSumProcessor.generateLIDODataForFM(lido_dedupe_df)

      //Generate Flight Master Data

      val flightMasterProcessor = FlightMasterProcessor(config)

      val flight_master_df = flightMasterProcessor.generateFlightMaster(core_dmis_fm_df, ags_fm_df, lido_fm_df, numberOfPartitions)

      if (config.data_load_mode == HISTORY) {
        writeOutput(flight_master_df, config.look_back_months, config.coalesce_value, config.output_history_location, config.output_current_location, FLIGHT_MASTER_OUTPUT_TEMP_TABLE, FLIGHT_DATE_COLUMN_NAME, config.compression)
      }
      else {
        flight_master_df.write.format("parquet").option("spark.sql.parquet.compression.codec", config.compression).save(config.output_current_location)
      }

    } getOrElse {
      log.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
