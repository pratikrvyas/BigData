/*----------------------------------------------------------------------------
 * Created on  : 01/04/2018
 * Author      : Fayaz Shaik(S796466)
 * Email       : fayazbasha.shaik@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AgsReflineModeledDriver.scala
 * Description : Scala application main class file which process AGS Refline
 *               history and incremental data for schema sync.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.emirates.helix.agsreflineschemasyncarguments.AGSReflineHistorySchemaSyncArgs._
import org.apache.log4j.Logger

/**
  * AgsReflineModeledDriver class with main method for starting point for the app
  */
object AgsReflineHistorySchemaSyncDriver {

  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(AgsReflineHistorySchemaSyncDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              1. ags_src_refline_history_location
    *              2. ags_tgt_refline_history_sync_location
    *              3. coalesce_value
    *              4. compression
    *
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Args]("spark-submit <spark-options> --class com.emirates.helix.AgsReflineHistorySchemaSyncDriver " +
      " agsreflinehistoryschemasync-<jar version>.jar") {
      head("AgsReflineHistorySchemaSync")
      opt[String]('i', "ags_src_refline_history_location")
        .required()
        .action((x, config) => config.copy(ags_src_refline_history_location = x))
        .text("Required parameter : AGS Refline History Location from Decomposed ")
      opt[String]('o', "ags_tgt_refline_history_sync_location")
        .required()
        .action((x, config) => config.copy(ags_tgt_refline_history_sync_location = x))
        .text("Required parameter : AGS Refline History Location for Schema Sync - Modeled ")
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
      val processor = AgsReflineHistorySchemaSyncProcessor(config)
      implicit lazy val sc = new SparkContext(new SparkConf().set("spark.sql.avro.compression.codec", config.compression))
      implicit lazy val sqlContext = new SQLContext(sc)
      // import sqlContext.implicits._
      processor.writeAGSHistorySync(processor.syncAGSRefline(processor.readAGSReflineHistory),config.coalesce_value)
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
