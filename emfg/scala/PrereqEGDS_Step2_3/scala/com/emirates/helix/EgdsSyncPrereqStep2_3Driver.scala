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

import org.apache.log4j.Logger


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

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.EgdsSyncPrereqDriver " +
      " egds-sync-prereq-<jar version>.jar") {
      head("EgdsSyncPrerequisite")
      opt[String]('a', "path_to_flight_master_current")
        .required()
        .action((x, config) => config.copy(path_to_flight_master_current = x))
        .text("Required parameter : path_to_flight_master_current Flight Master Location from Current ")
      opt[String]('b', "path_to_edgs_prerequisite")
        .required()
        .action((x, config) => config.copy(path_to_edgs_prerequisite = x))
        .text("Required parameter : path_to_edgs_prerequisite EGDS Location in Prerequisite ")
      opt[String]('c', "path_to_taxi_fuel_flow")
        .required()
        .action((x, config) => config.copy(path_to_taxi_fuel_flow = x))
        .text("Required parameter : path_to_taxi_fuel_flow Location for Taxi Fuel Dataset(file)")
      opt[String]('e', "path_to_ags")
        .required()
        .action((x, config) => config.copy(path_to_ags_current = x))
        .text("Required parameter : path_to_ags  Location for current AGS")
      opt[String]('f', "path_to_altea")
        .required()
        .action((x, config) => config.copy(path_to_altea_current = x))
        .text("Required parameter : path_to_ags  Location for current ALTEA")
      opt[Int]('g', "time_period")
        .required()
        .action((x, config) => config.copy(time_period = x))
        .text("Required parameter : Time period to identify rejected records (sec)")
      opt[Int]('h', "number_of_months")
        .required()
        .action((x, config) => config.copy(number_of_months = x))
        .text("Required parameter : Number of months for EGDS history")
      opt[Int]('i', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : coalesce value ")
      opt[String]('j', "compression")
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
      implicit lazy val spark = processor.getSparkSession(config.compression, "EGDS Step 2 and 3")
      val (final_df,consol_df)  = processor.readAndTransform(
        config.path_to_edgs_prerequisite,
        config.path_to_flight_master_current,
        config.path_to_taxi_fuel_flow,
        config.path_to_ags_current,
        config.path_to_altea_current,
        config.coalesce_value,
        config.time_period,config.number_of_months)
      if(final_df.count > 0){
        println("HELIX write final_df BEGIN")
        processor.writeEGDS_Step3(final_df,config.path_to_edgs_prerequisite,config.coalesce_value,config.compression)
        println("HELIX write final_df END")
      }
      if(consol_df.count > 0){
        println("HELIX write consol_df BEGIN")
        processor.writeEGDS_Step2(consol_df,config.path_to_edgs_prerequisite,config.coalesce_value,config.compression)
        println("HELIX write consol_df END")
      }

      } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }

}
