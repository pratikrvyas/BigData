/*----------------------------------------------------------
 * Created on  : 19/11/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.1
 * Project     : Helix-OpsEfficnecy
 * Filename    : MultiSourceR2DDriver.scala
 * Description : Scala application main class file for moving csv/avro/parquet data to decomposed layer
 * ----------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.util.SparkUtils
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import com.emirates.helix.model.Model._

/**
  * MultiSourceR2DDriver class with main method for starting point for the app
  */
object MultiSourceR2DDriver extends SparkUtils {
  private val compress_rex = """^snappy$|^deflate$""".r
  private val in_fmt_rex = """^avro$|^parquet$|^csv$""".r
  private val out_fmt_rex = """^avro$|^parquet$"""
  private lazy val logger:Logger = Logger.getLogger(MultiSourceR2DDriver.getClass)

  /**
   * Main method
   *
   *  @param args command line arguments
   *  @return Unit
   */

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[R2DArgs]("spark-submit <spark-options> --class com.emirates.helix.MultiSourceR2DDriver" +
      " multisource-r2d-<jar version>.jar") {
      head("MultiSourceR2D")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input file path for raw data")
      opt[String]('o', "out_path")
        .required()
        .action((x, config) => config.copy(out_path = x))
        .text("Required parameter : Output file path for decomposed data")
      opt[String]('d', "drop")
        .optional()
        .action((x, config) => config.copy(drop_col = x))
        .text("Optional parameter : If a column from input source has to be removed. Specify the column name")
      opt[String]('c', "compression")
        .required()
        .valueName("snappy OR deflate")
        .validate(x => { if (!(x.matches(compress_rex.toString()))) Left("Invalid compression specified : " + x) else Right(true) } match {
          case Left(l)  => failure(l)
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

    parser.parse(args, R2DArgs()) map { config =>
      val processor = MultiSourceR2DProcessor(config)
      implicit lazy val sc = new SparkContext(processor.sparkConfig())
      implicit lazy val sqlContext = processor.getSQLContext(sc,config.compression)
      processor.writeToHDFS(processor.processData(processor.readValueFromHDFS(config.in_path, config.in_format)),
        config.out_path, config.out_format, Some("error"), Some(config.part))
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}