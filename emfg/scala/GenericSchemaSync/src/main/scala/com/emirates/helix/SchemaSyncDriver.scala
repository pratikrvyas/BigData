/*----------------------------------------------------------------------------
 * Created on  : 01/07/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : SchemaSyncDriver.scala
 * Description : Scala application main class file for doing schema sync historic and
 *               incremental data with simple types
 * ----------------------------------------------------------------------------
 */

package com.emirates.helix

import org.apache.spark.SparkContext
import com.emirates.helix.model._
import com.emirates.helix.util.SparkUtil.setSqlContext
import org.apache.log4j.Logger
/**
  * SchemaSyncDriver class with main method for starting point for the app
  */
object SchemaSyncDriver {

  private val compress_rex = """^snappy$|^deflate$""".r
  private val format_rex = """^avro$|^parquet$""".r

  private lazy val logger:Logger = Logger.getLogger(SchemaSyncDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *              input path, output path, compression, input format, output format, schema mapping file
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Model.SyncArgs]("spark-submit <spark-options> --class com.emirates.helix.SchemaSyncDriver" +
      " schema-sync-<jar version>.jar") {
      head("SchemaSync")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input file path")
      opt[String]('o', "out_path")
        .required()
        .action((x, config) => config.copy(out_path = x))
        .text("Required parameter : Output file path")
      opt[String]('x', "mapping_file")
        .required()
        .action((x, config) => config.copy(mapping_file = x))
        .text("Required parameter : Mapping file with historic and incremental column mapping")
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
    parser.parse(args, Model.SyncArgs()) map { config =>
      val processor = SchemaSyncProcessor(config)
      implicit lazy val sc = new SparkContext()
      implicit lazy val sqlContext = setSqlContext(config.compression)
      import sqlContext.implicits._

      val sync_col:Seq[(String,String)] = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true").load(config.mapping_file)
      .select($"HISTORY",$"INCREMENTAL")
      .map(r => (r.getString(0),r.getString(1))).collect().toSeq

      processor.writeData(processor.processData(processor.readData,sync_col))

    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
