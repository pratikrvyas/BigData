/*----------------------------------------------------------
 * Created on  : 12/11/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsCsvToAvro.scala
 * Description : Scala application main class file which process EGDS OOOI raw data
 *               stored as sequence file and loads to decomposed layer
 * ----------------------------------------------------------
 */

package com.emirates.helix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.avro.Schema
import com.emirates.helix.model._
import org.apache.log4j.Logger

/**
  * EgdsR2DDriver class with main method for starting point for the app
  */
object EgdsR2DDriver {
  private val compress_rex = """^snappy$|^deflate$""".r

  private lazy val logger:Logger = Logger.getLogger(EgdsR2DDriver.getClass)

  /** Main method
    *
    *  @param args command line arguments
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Model.Args]("spark-submit <spark-options> --class com.emirates.helix.EgdsR2DDriver" +
      " egds-oooi-r2d-<jar version>.jar") {
      head("EgdsOOOIR2D")
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
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Model.Args()) map { config =>
      val processor = EgdsR2DProcessor(config)
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
