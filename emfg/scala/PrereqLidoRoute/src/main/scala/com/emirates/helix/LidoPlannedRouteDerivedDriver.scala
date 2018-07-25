/*----------------------------------------------------------
 * Created on  : 18/03/2018
 * Author      : Manu Mukundan(S795217), Ravindra Kumar(S794645)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : LidoPlannedRouteDerivedDriver.scala
 * Description : Primary class files for Lido planned route derived parameter definition
 * ----------------------------------------------------------
 */

package com.emirates.helix

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import com.emirates.helix.model.LidoRouteModel._

/**
  * LidoPlannedRouteDerivedDriver class with main method for starting point for the app
  */
object LidoPlannedRouteDerivedDriver {

  private val compress_rex = """^snappy$|^deflate$""".r
  private val in_fmt_rex = """^avro$|^parquet$|^csv$|^sequence$""".r
  private val out_fmt_rex = """^avro$|^parquet$"""
  private lazy val logger: Logger = Logger.getLogger(LidoPlannedRouteDerivedDriver.getClass)

  /**
    * Main method
    *
    * @param args command line arguments
    * @return Unit
    */

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[LidoRouteArgs]("spark-submit <spark-options> --class com.emirates.helix.LidoPlannedRouteDerivedDriver" +
      " lido-route-derived-<jar version>.jar") {
      head("LidoRouteDerived")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input file path for incremental data")
      opt[String]('m', "onetimemaster_path")
        .required()
        .action((x, config) => config.copy(onetimemaster_path = x))
        .text("Required parameter : Input file path onetimemaster data")
      opt[String]('a', "arinc_airport")
        .required()
        .action((x, config) => config.copy(arinc_airport = x))
        .text("Required parameter : Input file path arinc airport data")
      opt[String]('k', "arinc_runway")
        .required()
        .action((x, config) => config.copy(arinc_runway = x))
        .text("Required parameter : Input file path arinc runway data")
      opt[String]('f', "flight_master")
        .required()
        .action((x, config) => config.copy(flight_master = x))
        .text("Required parameter : Input file path for flight master parquet file")
      opt[String]('o', "out_path")
        .required()
        .action((x, config) => config.copy(out_path = x))
        .text("Required parameter : Output file path")
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
      opt[String]('s', "in_fmt")
        .required()
        .valueName("avro, parquet OR csv")
        .validate(x => {
          if (!(x.matches(in_fmt_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(in_format = x))
        .text("Required parameter : Input data format, avro, parquet, csv or sequence")
      opt[String]('p', "out_fmt")
        .required()
        .valueName("avro OR parquet")
        .validate(x => {
          if (!(x.matches(out_fmt_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })

        .action((x, config) => config.copy(out_format = x))
        .text("Required parameter : Input data format, avro or parquet")
      opt[Int]('r', "part")
        .required()
        .action((x, config) => config.copy(part = x))
        .text("Required parameter : Number of partitions for coalesce")
      help("help") text ("Print this usage message")
    }

    parser.parse(args, LidoRouteArgs()) map { config =>
      val processor = LidoPlannedRouteDerivedProcessor(config)
      implicit lazy val sc = new SparkContext(processor.sparkConfig())
      implicit lazy val hiveContext = processor.getHiveContext(sc, config.compression)
      val in_df = processor.readValueFromHDFS(config.in_path, config.in_format)
      val master_df = processor.readValueFromHDFS(config.onetimemaster_path, config.in_format)
      val arinc_airport = processor.readValueFromHDFS(config.arinc_airport, config.in_format)
      val arinc_rnway = processor.readValueFromHDFS(config.arinc_runway, config.in_format)
      val flight_master = processor.readValueFromHDFS(config.flight_master, "parquet")

      processor.writeToHDFS(processor.processData(in_df,master_df,arinc_rnway,arinc_airport,flight_master),
        config.out_path, config.out_format, Some("overwrite"), Some(config.part))
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
