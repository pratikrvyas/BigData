/*----------------------------------------------------------
 * Created on  : 21/12/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : EgdsR2DProcessor.scala
 * Description : Secondary class file for invoking processing of TIBCO message in json format
 * ----------------------------------------------------------
 */
package com.emirates.helix

import com.emirates.helix.model._
import com.emirates.helix.udf.UDF._
import com.emirates.helix.util.SparkUtil._
import org.apache.avro.Schema
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Companion object for JsonToAvroProcessor class
  */
object EgdsR2DProcessor {

  /**
    * EgdsR2DProcessor instance creation
    * @param args user parameter instance
    * @return EgdsR2DProcessor instance
    */
  def apply(args: Model.Args): EgdsR2DProcessor = {
    var processor = new EgdsR2DProcessor()
    processor.args = args
    processor
  }
}

/**
  * EgdsR2DProcessor class with methods to read, process and write the data
  */
class EgdsR2DProcessor {
  private var args: Model.Args = _

  /**  Run method which actually initializes spark and does the processing
    *
    *  @param json_rdd RDD of Strings representing raw message
    *  @return DataFrame Spark dataframe for processed R2D data
    */
  def processData(json_rdd: RDD[String])(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._

    val message_data = sqlContext.read.json(json_rdd)
      .select($"metadata.messageTime".as("tibco_messageTime"),
      getFormattedMessage($"message").as("message_list"))

    message_data.select($"tibco_messageTime",
      $"message_list"(0).as("flightstate"),
      $"message_list"(1).as("acreg_no"),
      $"message_list"(2).as("flight_no"),
      $"message_list"(3).as("flight_date"),
      $"message_list"(4).as("message_date"),
      $"message_list"(5).as("time_of_message_received"),
      $"message_list"(6).as("dep_stn"),
      $"message_list"(7).as("arr_stn"),
      $"message_list"(8).as("time_of_message_delivered"),
      $"message_list"(9).as("fob"),
      $"message_list"(10).as("zfw"))
      .withColumn("HELIX_UUID",generateUUID())
      .withColumn("HELIX_TIMESTAMP",generateTimestamp())
  }

  /**  Method to read raw data from HDFS
    *
    *  @return RDD[String] RDD of string representing input raw data
    */
  def readData(implicit sc: SparkContext): RDD[String] = {
    sc.sequenceFile[LongWritable, BytesWritable](args.in_path).map(x => new String(x._2.copyBytes(), "utf-8"))
  }

  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeData(out_df: DataFrame) : Unit = {
    out_df.write.format("com.databricks.spark.avro").save(args.out_path)
  }
}
