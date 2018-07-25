/*----------------------------------------------------------------------------
 * Created on  : 03/26/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : LidoMetarPrereqDriver.scala
 * Description : Scala application main class file for formatting Lido Metar incremental data.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix.emfg
import com.emirates.helix.emfg.model._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

/**
  * LidoMetarPrereqDriver class with main method for starting point for the app
  */
object LidoMetarPrereqDriver {
  private val compress_rex = """^snappy$|^deflate$""".r
  private val format_rex = """^avro$|^parquet$|^csv$|^sequence$""".r

  private lazy val logger:Logger = Logger.getLogger(LidoMetarPrereqDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              input path, output path, compression, input format, output format
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {
    val parser = new scopt.OptionParser[Model.LidoMetarPrereqArgs]("spark-submit <spark-options> --class com.emirates.helix.emfg.LidoMetarPrereqDriver" +
      " lidometar-sync-prereq-<jar version>.jar") {
      head("LidoMetarSyncPrereq")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input data path")
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
        .valueName("avro, parquet, csv OR sequence")
        .validate(x => {
          if (!(x.matches(format_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(in_format = x))
        .text("Required parameter : Input data format, avro, parquet, csv OR sequence")
      opt[String]('p', "out_fmt")
        .required()
        .valueName("avro OR parquet")
        .validate(x => {
          if (!(x.matches(format_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(out_format = x))
        .text("Required parameter : Output data format, avro or parquet")
      opt[Int]('r', "part")
        .optional()
        .action((x, config) => config.copy(part = x))
        .text("Optional parameter : Output partitions to coalesce")
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Model.LidoMetarPrereqArgs()) map { config =>
      val processor = LidoMetarPrereqProcessor(config)
      implicit lazy val sc = new SparkContext(processor.sparkConfig())
      implicit lazy val sqlContext = processor.getSQLContext(sc,config.compression)
      processor.writeToHDFS(processor.processData(processor.readValueFromHDFS(config.in_path,config.in_format)),
        config.out_path, config.out_format, Some("overwrite"), Some(config.part))

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
