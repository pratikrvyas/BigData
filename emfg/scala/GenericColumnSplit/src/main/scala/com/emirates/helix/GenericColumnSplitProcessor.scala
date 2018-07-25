/*----------------------------------------------------------
 * Created on  : 02/01/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : GenericColumnSplitProcessor.scala
 * Description : Secondary class file for invoking processing of column split
 * ----------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.model._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
/**
  * Companion object for GenericColumnSplitProcessor class
  */
object GenericColumnSplitProcessor {

  /**
    * GenericColumnSplitProcessor instance creation
    * @param args user parameter instance
    * @return GenericColumnSplitProcessor instance
    */
  def apply(args: Model.Args): GenericColumnSplitProcessor = {
    var processor = new GenericColumnSplitProcessor()
    processor.args = args
    processor
  }
}

/**
  * GenericColumnSplitProcessor class with methods to read, process and write the data
  */
class GenericColumnSplitProcessor {
  private var args: Model.Args = _

  /** Run method which actually initializes spark and does the processing
    *
    * @param in_df DataFrame representing input data
    * @return DataFrame Spark dataframe with filtered data
    */
  def processData(in_df: DataFrame,columns:Seq[String])(implicit sqlContext: SQLContext): DataFrame = {
    in_df.select(columns.map(name => col(name)):_*)
  }

  /**  Method to read raw data from HDFS
    *
    *  @return DataFrame for processed data
    */
  def readData(implicit sqlContext: SQLContext): DataFrame = {
    args.in_format match {
      case "avro" => sqlContext.read.format("com.databricks.spark.avro").load(args.in_path)
      case "parquet" => sqlContext.read.parquet(args.in_path)
    }
  }

  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeData(out_df: DataFrame): Unit = {
    args.out_format match {
      case "avro" => out_df.write.format("com.databricks.spark.avro").save(args.out_path)
      case "parquet" => out_df.write.parquet(args.out_path)
    }
  }
}