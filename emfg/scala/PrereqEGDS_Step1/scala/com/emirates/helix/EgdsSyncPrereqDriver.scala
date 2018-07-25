/*----------------------------------------------------------------------------
 * Created on  : 01/Mar/2018
 * Author      : Oleg Baydakov(S754344)
 * Email       : oleg.baydakov@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsSyncPrereqArgumentsArguments.scala
 * Description : Driver for EDGS data
 * ----------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.EgdsSyncPrereqArguments._
//import org.apache.log4j.

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import org.joda.time._
import org.joda.time.{DateTime, Period}
import org.joda.time.format.{DateTimeFormat}
import org.joda.time.format.{DateTimeFormatter}

import com.typesafe.config._


object EgdsSyncPrereqDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(EgdsSyncPrereqDriver.getClass)

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

    // val defaultConfig:Config = ConfigFactory.parseResources("defaults.conf").resolve();
    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.EgdsSyncPrereqDriver " +
      " egds-sync-prereq-<jar version>.jar") {
      head("EgdsSyncPrerequisite")
      opt[String]('a', "path_to_egds_incremental")
        .required()
        .action((x, config) => config.copy(path_to_egds_incremental = x))
        .text("Required parameter : EDGS data Location from Incremental ")
      opt[String]('b', "path_to_flight_master_current")
        .required()
        .action((x, config) => config.copy(path_to_flight_master_current = x))
        .text("Required parameter : path_to_flight_master_current Flight Master Location from Current ")
      opt[String]('c', "path_to_edgs_prerequisite")
        .required()
        .action((x, config) => config.copy(path_to_edgs_prerequisite = x))
        .text("Required parameter : path_to_edgs_prerequisite EGDS Location in Prerequisite ")
      opt[String]('d', "path_to_edgs_rejected")
        .required()
        .action((x, config) => config.copy(path_to_edgs_rejected = x))
        .text("Required parameter : path_to_edgs_rejected EGDS Location in Rejected")
      opt[Int]('e', "time_period")
        .required()
        .action((x, config) => config.copy(time_period = x))
        .text("Required parameter : Time period to identify rejected records (sec)")
      opt[Int]('f', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : coalesce value ")
      opt[String]('g', "date for folder name")
        .optional()
        .action((x, config) => config.copy(date_for_folder = x))
        .text("Required parameter : date for folder name ")
      opt[String]('h', "compression")
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
      val processor = EgdsSyncPrereqProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "EGDS")
      val final_df  = processor.readAndTransform(config.path_to_egds_incremental,
                                                                config.path_to_flight_master_current,
                                                                config.time_period,config.date_for_folder)

      if(final_df.count > 0){
        //val path_to_folder_yesterday   =  DateTime.now.minusDays(1).toString("yyyy-MM-dd")
        // CHANGE DATE
        processor.writeEDGSprerequisite(final_df,config.path_to_edgs_prerequisite,config.coalesce_value, config.date_for_folder, config.compression)
      }
      //val path_to_folder_today   =  DateTime.now.toString("yyyy-MM-dd")
      // DATE FROM INPUT PARAMETERS
      //val next_day_date:DateTime = DateTime.parse(config.date_for_folder, DateTimeFormat.forPattern("yyyy-MM-dd")).plusDays(1)
      //val path_to_folder_today=next_day_date.toString("YYYY-MM-dd")
      //if(tomorrow_df.count > 0){
        // COMMENTED OLEG 29.05
        //processor.writeEDGSprerequisite_tomorrow(tomorrow_df,config.path_to_edgs_prerequisite,config.coalesce_value,path_to_folder_today, config.compression)
      //}

      // rejected records
      val df_rejected= processor.regectedRecords()

      if(df_rejected.count > 0) {
        //val path_to_folder_yesterday   =  DateTime.now.minusDays(1).toString("yyyy-MM-dd")
        // CHANGE DATE
        //val path_to_folder_yesterday="2018-04-21"
        processor.writeRejected(df_rejected,config.path_to_edgs_rejected,config.coalesce_value,config.date_for_folder,config.compression)
      }


     } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }

}
