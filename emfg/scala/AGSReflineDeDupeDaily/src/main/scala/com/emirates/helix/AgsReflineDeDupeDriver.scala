/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AgsReflineDeDupeDriver.scala
 * Description : Scala application main class file which processes AGS Refline
 *               history and sync the schema to incremental.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import org.apache.spark.{SparkConf, SparkContext}
import com.emirates.helix.agsreflinesdedupearguments.AGSReflineDeDupeArgs._
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext

/**
  * AgsReflineModeledDriver class with main method for starting point for the app
  */
object AgsReflineDeDupeDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(AgsReflineDeDupeDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. ags_refline_dedupe_current_location
    *              2. ags_src_refline_incremental_location
    *              3. ags_tgt_refline_dedupe_current_location
    *              4. coalesce_value
    *              5. compression
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.AgsReflineDeDupeDriver " +
      " agsreflinededupedaily-<jar version>.jar") {
      head("AgsReflineDeDupeDaily")
      opt[String]('i', "ags_refline_dedupe_current_location")
        .required()
        .action((x, config) => config.copy(ags_refline_dedupe_current_location = x))
        .text("Required parameter : AGS Refline History Sync Location ")
      opt[String]('j', "ags_src_refline_incremental_location")
        .required()
        .action((x, config) => config.copy(ags_src_refline_incremental_location = x))
        .text("Required parameter : AGS Refline Incremental Data Location ")
      opt[String]('x', "ags_tgt_refline_dedupe_current_location")
        .required()
        .action((x, config) => config.copy(ags_tgt_refline_dedupe_current_location = x))
        .text("Required parameter : AGS Refline DeDupe Target Current Location ")
      opt[Int]('p', "coalesce_value")
        .optional()
        .action((x, config) => config.copy(coalesce_value = x))
        .text("Optional parameter : AGS Refline DeDupe coalesce value ")
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
      val processor = AgsReflineDedupeProcessor(config)
      implicit lazy val sc = new SparkContext(new SparkConf().set("spark.sql.parquet.compression.codec", config.compression))
      implicit lazy val sqlContext:HiveContext = new HiveContext(sc)
      // implicit lazy val hiveContext = new HiveContext(sc)
      processor.writeAGSDeDupe(processor.deDupeAGSRefline(processor.readAGSRefline),
        config.coalesce_value, config.ags_tgt_refline_dedupe_current_location)
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
