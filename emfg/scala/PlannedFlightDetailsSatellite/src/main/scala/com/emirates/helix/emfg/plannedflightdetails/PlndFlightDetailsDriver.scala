/*----------------------------------------------------------
 * Created on  : 04/01/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PlndFlightDetailsDriver.scala
 * Description : Scala application main class file for Planned Flight Details satellite construction
 * ----------------------------------------------------------
 */

package com.emirates.helix.emfg.plannedflightdetails

import com.emirates.helix.emfg.model.Model
import org.apache.log4j.Logger

/**
  * PlndFlightDetailsDriver class with main method for starting point for the app
  */

object PlndFlightDetailsDriver {
  private val compress_rex = """^snappy$|^deflate$""".r
  private val format_rex = """^avro$|^parquet$|^sequence$""".r
  private lazy val logger:Logger = Logger.getLogger(PlndFlightDetailsDriver.getClass)
  /** Main method
    *
    *  @param args command line arguments
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {
    val parser = new scopt.OptionParser[Model.PlndFlgtArgs]("spark-submit <spark-options> --class com.emirates.helix.emfg.PlndFlightDetailsDriver" +
      " plan-flightdtls-sat-<jar version>.jar") {
      head("PlannedFlightDetailsConstruction")
      opt[String]('f', "flightmstr")
        .required()
        .action((x, config) => config.copy(fm_path = x))
        .text("Required parameter : Flight master input path")
      opt[String]('l', "lidoflight")
        .required()
        .action((x, config) => config.copy(l_flight = x))
        .text("Required parameter : Lido flight level input path")
      opt[String]('r', "lidoroute")
        .required()
        .action((x, config) => config.copy(l_route = x))
        .text("Required parameter : Lido route level input path")
      opt[String]('w', "lidoweather")
        .required()
        .action((x, config) => config.copy(l_weather = x))
        .text("Required parameter : Lido weather input path")
      opt[String]('i', "incr")
        .required()
        .action((x, config) => config.copy(out_incr = x))
        .text("Required parameter : Output file path for latest/current data store")
      opt[String]('h', "hist")
        .optional()
        .action((x, config) => config.copy(out_hist = Some(x)))
        .text("Optional parameter : Output file path for history data store. Needed only for the first day run")
      opt[Int]('m', "months")
        .optional()
        .action((x, config) => config.copy(months = x))
        .text("Optional parameter : History and current data split month value for initial run. Default : 2 months")
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
      opt[Int]('p', "part")
        .required()
        .action((x, config) => config.copy(part = x))
        .text("Required parameter : Number of partitions for coalesce")
      opt[Unit]('t', "init") action {
        (x,conf) => conf.copy(init = true)
      } text("Use this option if it is the first time to load both history and incremental")
      help("help") text ("Print this usage message")
    }

    parser.parse(args, Model.PlndFlgtArgs()) map { config =>
      val processor = PlndFlightDetailsProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "PlndFlightDetails")
      spark.sparkContext.setCheckpointDir("/tmp/emfg_planed_flight_details/")
      (config.init, config.out_hist) match {
        case (true, None) => {
          logger.error("Output path to store history data must be provided for initial run. Killing application")
          spark.stop()
          System.exit(1)
        }
        case (true,Some(_)) => {
          val fm = processor.readValueFromHDFS(config.fm_path,"parquet")
          val lf = processor.readValueFromHDFS(config.l_flight,"parquet")
          val lr = processor.readValueFromHDFS(config.l_route,"parquet")
          val lw = processor.readValueFromHDFS(config.l_weather,"parquet")

          val (cur, hist) = processor.processData(fm, lf, lr, lw)
          processor.writeToHDFS(cur,config.out_incr,"parquet")
          processor.writeToHDFS(hist.getOrElse(null), config.out_hist.getOrElse(""),"parquet")
        }
        case (false,_) => {
          val fm = processor.readValueFromHDFS(config.fm_path,"parquet")
          val lf = processor.readValueFromHDFS(config.l_flight,"parquet")
          val lr = processor.readValueFromHDFS(config.l_route,"parquet")
          val lw = processor.readValueFromHDFS(config.l_weather,"parquet")
          val cur = processor.processData(fm, lf, lr, lw)._1
          processor.writeToHDFS(cur,config.out_incr,"parquet")
        }
      }
      spark.stop()
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
