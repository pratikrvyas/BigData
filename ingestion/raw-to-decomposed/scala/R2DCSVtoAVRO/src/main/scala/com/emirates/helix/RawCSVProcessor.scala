/*----------------------------------------------------------
 * Created on  : 13/12/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : GenericCSVProcessor.scala
 * Description : Secondary class file for invoking processing of generic csv file
 * ----------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.model._
import com.emirates.helix.udf.UDF._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Companion object for GenericCSVProcessor class
  */
object RawCSVProcessor {

  /**
    * GenericCSVProcessor instance creation
    * @param args user parameter instance
    * @return GenericCSVProcessor instance
    */
  def apply(args: Model.Args): RawCSVProcessor = {
    var processor = new RawCSVProcessor()
    processor.args = args
    processor
  }
}

/**
  * GenericCSVProcessor class with methods to read, process and write the data
  */
class RawCSVProcessor {
  private var args: Model.Args = _
  private lazy val log: Logger = Logger.getLogger(RawCSVProcessor.getClass)

  /**  Run method which actually initializes spark and does the processing
    *
    *  @param in_df RDD representing raw data
    *  @return DataFrame Spark dataframe for processed R2D data
    */
  def processData(in_df: DataFrame): DataFrame = {
    in_df.withColumn("HELIX_UUID",generateUUID()).withColumn("HELIX_TIMESTAMP",cast)
  }

  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeData(out_df: DataFrame) : Unit = {
    out_df.write.format("com.databricks.spark.avro").save(args.out_path)
  }

  /**  Method to read raw data from HDFS
    *
    *  @return RDD[String] RDD of string representing input raw data
    */
  def readData(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter", args.delimiter).load(args.in_path)
  }
}
