/*----------------------------------------------------------
 * Created on  : 04/22/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : AltealdmPrereqDriver.scala
 * Description : Scala application main class file which process ALTEA LDM message
 *               to calculate load distribution
 * ----------------------------------------------------------
 */
package com.emirates.helix.emfg

import com.emirates.helix.emfg.model.Model
import org.apache.log4j.Logger

/**
  * AltealdmPrereqDriver class with main method for starting point for the app
  */
object AltealdmPrereqDriver {
  private val compress_rex = """^snappy$|^deflate$""".r
  private val format_rex = """^avro$|^parquet$|^sequence$""".r
  private lazy val logger:Logger = Logger.getLogger(AltealdmPrereqDriver.getClass)
  /** Main method
    *
    *  @param args command line arguments
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {
    val parser = new scopt.OptionParser[Model.AltLdmArgs]("spark-submit <spark-options> --class com.emirates.helix.emfg.AltealdmPrereqDriver" +
      " altea-ldm-prereq-<jar version>.jar") {
      head("AlteaLdmPrereq")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input data path to read data from HDFS")
      opt[String]('o', "out_path")
        .required()
        .action((x, config) => config.copy(out_path = x))
        .text("Required parameter : Output path to save data to HDFS")
      opt[String]('v', "rej_path")
        .required()
        .action((x, config) => config.copy(rej_path = x))
        .text("Required parameter : path to save rejected data to HDFS")
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
      opt[String]('p', "in_format")
        .required()
        .valueName("avro OR parquet OR sequence")
        .validate(x => {
          if (!(x.matches(format_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(in_format = x))
        .text("Required parameter : Input data format, avro, parquet or sequence file")
      opt[String]('r', "out_format")
        .required()
        .valueName("avro OR parquet")
        .validate(x => {
          if (!(x.matches(format_rex.toString()))) Left("Invalid format specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(out_format = x))
        .text("Required parameter : Input data format, avro, parquet or sequence file")
      opt[Int]('t', "part")
        .required()
        .action((x, config) => config.copy(part = x))
        .text("Required parameter : Number of partitions for coalesce")
      opt[Int]('d', "offset")
        .required()
        .action((x, config) => config.copy(date_offset = x))
        .text("Required parameter : Date offset from current date for flagging records processed completely")
      help("help") text ("Print this usage message")
    }

    parser.parse(args, Model.AltLdmArgs()) map { config =>
      val processor = AltealdmPrereqProcessor(config)
      implicit lazy val spark = processor.getSparkSession(config.compression, "AlteaLdmR2DD")
      processor.writeToHDFS(processor.processData(processor.readValueFromHDFS(config.in_path, config.in_format),config.rej_path,config.out_format,config.part),
        config.out_path, config.out_format, Some("overwrite"), Some(config.part))
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}