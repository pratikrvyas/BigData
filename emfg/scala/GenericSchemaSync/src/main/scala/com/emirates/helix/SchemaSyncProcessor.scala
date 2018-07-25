/*----------------------------------------------------------------------------
 * Created on  : 01/09/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : SchemaSyncProcessor.scala
 * Description : Secondary class file for syncing historic data with incremental data.
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.model.Model._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.collection.mutable.ListBuffer

/**
  * Companion object for SchemaSyncProcessor class
  */
object SchemaSyncProcessor {

  /**
    * AgsReflineProcessor instance creation
    * @param args user parameter instance
    * @return SchemaSyncProcessor instance
    */
  def apply(args: SyncArgs): SchemaSyncProcessor = {
    var processor = new SchemaSyncProcessor()
    processor.args = args
    processor
  }
}

/**
  * SchemaSyncProcessor class with methods to
  * read history data and perform schema sync.
  */
class SchemaSyncProcessor {
  private var args: SyncArgs = _

   /**  Method performing schema sync
    *
    *  @return DataFrame
    */
   def processData(in_df: DataFrame,col_mapping:Seq[(String,String)])(implicit sqlContext: SQLContext): DataFrame = {
     var new_col = new ListBuffer[String]()
     col_mapping.map { elem => { elem match {
       case ("",y) => new_col = new_col += y
       case _ => } } }
     val out_df = in_df.select(col_mapping.filter(_._1 != "").map {
       elem => { elem match {
           case (x,y) => if (x == y) col(x) else col(x).alias(y) } } }: _*)
     new_col.foldLeft(out_df)((out_df,column) => out_df.withColumn(column,lit(null).cast(StringType)))
   }

  /**  Method to read input historic data from HDFS
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
