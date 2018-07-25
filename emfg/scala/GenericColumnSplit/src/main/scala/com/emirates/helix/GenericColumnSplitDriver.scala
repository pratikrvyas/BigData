/*----------------------------------------------------------
 * Created on  : 02/01/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : GenericColumnSplitDriver.scala
 * Description : Scala application main class file which for reading and splitting the columns
 *               flight level info
 * ----------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.model._
import com.emirates.helix.util.SparkUtil._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

/**
  * GenericColumnSplitDriver class with main method for starting point for the app
  */
object GenericColumnSplitDriver {

  private val compress_rex = """^snappy$|^deflate$""".r
  private val format_rex = """^avro$|^parquet$""".r
  private lazy val logger:Logger = Logger.getLogger(GenericColumnSplitDriver.getClass)
  /**
    * Main method
    *
    *  @param args command line arguments
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Model.Args]("spark-submit <spark-options> --class com.emirates.helix.GenericColumnSplitDriver" +
      " generic-columnsplit-<jar version>.jar") {
      head("GenericColumnSplitDriver")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input file path")
      opt[String]('o', "out_path")
        .required()
        .action((x, config) => config.copy(out_path = x))
        .text("Required parameter : Output file path")
      opt[String]('x', "conf_file")
        .required()
        .action((x, config) => config.copy(conf_file = x))
        .text("Required parameter : Config parameter file for the column names to select")
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

    parser.parse(args, Model.Args()) map { config =>
      val processor = GenericColumnSplitProcessor(config)
      implicit lazy val sc = new SparkContext()
      implicit lazy val sqlContext = setSqlContext(config.compression)
      import sqlContext.implicits._

      val columns: Seq[String] = sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true").option("inferSchema", "true").load(config.conf_file)
        .select($"COLUMN_NAME")
        .map(r => r.getString(0)).collect().toSeq

      processor.writeData(processor.processData(processor.readData,columns))

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }

}
