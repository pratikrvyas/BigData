/*----------------------------------------------------------------------------
 * Created on  : 01/31/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : CoreDeDupeDriverDaily.scala
 * Description : Scala application main class file which processes Core
 *               dedupe current data and incremental data.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import org.apache.spark.{SparkConf, SparkContext}
import com.emirates.helix.corededupearguments.CoreDeDupeArgs._
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext

/**
  * CoreDeDupeDriver class with main method for starting point for the app
  */
object CoreDeDupeDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(CoreDeDupeDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. core_dedupe_current_location
    *              2. core_incremental_location
    *              3. coalesce_value
    *              4. compression
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.CoreDeDupeDriver " +
      " corededupedaily-<jar version>.jar") {
      head("CoreDeDupeDaily")
      opt[String]('i', "core_dedupe_current_location")
        .required()
        .action((x, config) => config.copy(core_dedupe_current_location = x))
        .text("Required parameter : Core DeDupe Current Location ")
      opt[String]('j', "core_incremental_location")
        .required()
        .action((x, config) => config.copy(core_incremental_location = x))
        .text("Required parameter : Core Incremental Data Location ")
      opt[String]('x', "core_dedupe_tgt_current_location")
        .required()
        .action((x, config) => config.copy(core_dedupe_tgt_current_location = x))
        .text("Required parameter : Core DeDupe Target Current Location ")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : Core DeDupe coalesce value ")
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
      val processor = CoreDedupeProcessor(config)
      implicit lazy val sc = new SparkContext(new SparkConf().set("spark.sql.parquet.compression.codec", config.compression))
      implicit lazy val sqlContext:HiveContext = new HiveContext(sc)
      // implicit lazy val hiveContext = new HiveContext(sc)
      processor.writeCoreDeDupe(processor.deDupeCore(processor.readCore),
        config.coalesce_value,config.core_dedupe_tgt_current_location)
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
