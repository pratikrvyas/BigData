/*----------------------------------------------------------
 * Created on  : 13/12/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : GenericCSVDriver.scala
 * Description : Scala application main class file for processing csv data
 * ----------------------------------------------------------
 */
package com.emirates.helix

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext,SparkConf}
import com.emirates.helix.model._

/**
  * GenericCSVDriver class with main method for starting point for the app
  */
object RawCSVDriver {

  private val compress_rex = """^snappy$|^deflate$""".r
  private lazy val logger: Logger = Logger.getLogger(RawCSVDriver.getClass)

  /** Main method
    *
    *  @param args command line arguments
    *  @return Unit
    */
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Model.Args]("spark-submit <spark-options> --class com.emirates.helix.RawCSVDriver " +
      "generic-csvtoavro-r2d-<jar versoin>.jar") {
      head("GenericCSVParser")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input file path for raw data")
      opt[String]('o', "out_path")
        .required()
        .action((x, config) => config.copy(out_path = x))
        .text("Required parameter : Output file path for decomposed data")
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
      opt[String]('d', "delimiter")
        .optional()
        .action((x, config) => config.copy(delimiter = x))
        .text("Optional parameter : Delimiter value to parse csv. By default it is comma(,)")
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Model.Args()) map { config =>
      val processor = RawCSVProcessor(config)
      implicit lazy val sc = new SparkContext(new SparkConf().set("spark.sql.avro.compression.codec", config.compression))
      implicit lazy val sqlContext = new SQLContext(sc)
      processor.writeData(processor.processData(processor.readData))
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
