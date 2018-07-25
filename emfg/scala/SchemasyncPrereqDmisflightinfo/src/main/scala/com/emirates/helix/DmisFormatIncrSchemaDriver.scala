/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DmisFormatIncrSchemaDriver.scala
 * Description : Scala application main class file for formatting DMIS incremental data.
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.model._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

/**
  * DmisFormatIncrSchemaDriver class with main method for starting point for the app
  */
object DmisFormatIncrSchemaDriver {
  private val compress_rex = """^snappy$|^deflate$""".r
  private val format_rex = """^avro$|^parquet$""".r

  private lazy val logger:Logger = Logger.getLogger(DmisFormatIncrSchemaDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              input path, output path, compression, input format, output format
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Model.DMISIncrFrmtArgs]("spark-submit <spark-options> --class com.emirates.helix.DmisFormatIncrSchemaDriver" +
      " dmis-sync-prereq-<jar version>.jar") {
      head("DMISSchemaSyncPrereq")
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
        .valueName("avro OR parquet")
        .validate(x => {
          if (!(x.matches(format_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(in_format = x))
        .text("Required parameter : Input data format, avro or parquet")
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
        .text("Required parameter : Input data format, avro or parquet")
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Model.DMISIncrFrmtArgs()) map { config =>
      val processor = DmisFormatIncrSchemaProcessor(config)
      implicit lazy val sc = new SparkContext(processor.sparkConfig())
      implicit lazy val sqlContext = processor.getSQLContext(sc,config.compression)
      processor.writeData(processor.processData(processor.readValueFromHDFS(config.in_path,config.in_format,sqlContext)))

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
