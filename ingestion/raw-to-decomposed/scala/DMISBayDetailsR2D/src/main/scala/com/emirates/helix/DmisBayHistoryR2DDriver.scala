/*----------------------------------------------------------
 * Created on  : 12/11/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DmisBayHistoryR2DDriver.scala
 * Description : Scala application main class file which process Dmis bay details feed R2D
 * ----------------------------------------------------------
 */
package com.emirates.helix

import org.apache.log4j.Logger
import com.emirates.helix.model._
import org.apache.spark.SparkContext

/**
  * DmisBayHistoryR2DDriver class with main method for starting point for the app
  */
object DmisBayHistoryR2DDriver {
  private val compress_rex = """^snappy$|^deflate$""".r
  private val informat_rex = """^avro$|^parquet$|^sequence$""".r
  private val outformat_rex = """^avro$|^parquet$""".r

  private lazy val logger:Logger = Logger.getLogger(DmisBayHistoryR2DDriver.getClass)
  /** Main method
    *
    *  @param args Command line arguments
    *              input path, output path, compression, input format, output format, partitions
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Model.XmlR2DArgs]("spark-submit <spark-options> --class com.emirates.helix.DmisBayHistoryR2DDriver" +
      " dmis-bayhistory-r2d-<jar version>.jar") {
      head("DmisBayHistoryR2D")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input file path")
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
      opt[Int]('p', "part")
        .required()
        .action((x, config) => config.copy(part = x))
        .text("Required parameter : Number of partitions for coalesce")
      opt[String]('r', "in_format")
        .required()
        .valueName("avro/parquet/sequence")
        .validate(x => {
          if (!(x.matches(informat_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(in_format = x))
        .text("Required parameter : Input data format, avro or parquet or sequence")
      opt[String]('s', "out_format")
        .required()
        .valueName("avro OR parquet")
        .validate(x => {
          if (!(x.matches(outformat_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(out_format = x))
        .text("Required parameter : Output data format, avro or parquet")
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Model.XmlR2DArgs()) map { config =>
      val processor = DmisBayHistoryR2DProcessor(config)
      implicit lazy val sc = new SparkContext(processor.sparkConfig())
      implicit lazy val sqlContext = processor.getSQLContext(sc, config.compression)
      processor.writeToHDFS(processor.processData(processor.readValueFromHDFS(config.in_path, config.in_format)),
        config.out_path, config.out_format)
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
