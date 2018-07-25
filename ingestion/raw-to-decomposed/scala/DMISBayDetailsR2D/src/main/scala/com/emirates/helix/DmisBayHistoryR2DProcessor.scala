/*----------------------------------------------------------
 * Created on  : 14/02/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DmisBayHistoryR2DProcessor.scala
 * Description : Secondary class file for processing Dmis bay details feed
 * ----------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.model.Model
import com.emirates.helix.util.SparkUtils
import com.emirates.helix.udf.UDF._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{regexp_extract,when}

/**
  * Companion object for JsonToAvroProcessor class
  */
object DmisBayHistoryR2DProcessor {

  /**
    * DmisBayHistoryR2DProcessor instance creation
    * @param args user parameter instance
    * @return DmisBayHistoryR2DProcessor instance
    */
  def apply(args: Model.XmlR2DArgs): DmisBayHistoryR2DProcessor = {
    var processor = new DmisBayHistoryR2DProcessor()
    processor.args = args
    processor
  }
}

/**
  * DmisBayHistoryR2DProcessor class with methods to read, process and write the data
  */
class DmisBayHistoryR2DProcessor extends SparkUtils {
  private var args: Model.XmlR2DArgs = _
  private lazy val log: Logger = Logger.getLogger(DmisBayHistoryR2DProcessor.getClass)

  /**  Run method which actually initializes spark and does the processing
    *
    *  @param in_df Dataframe representing raw message
    *  @return DataFrame Spark dataframe for processed R2D data
    */
  def processData(in_df: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val out_df = in_df.select($"metadata.messageTime".as("tibco_messageTime"), $"message")
      .withColumn("airlineCode", regexp_extract($"message","<ns0:airlineCode>(\\w+)<\\/ns0:airlineCode>", 1))
      .withColumn("arrivalDepartureFlag", regexp_extract($"message","<ns0:arrivalDepartureFlag>(\\w+)<\\/ns0:arrivalDepartureFlag>", 1))
      .withColumn("aircraftRegistrationNumber", regexp_extract($"message","<ns0:aircraftRegistrationNumber>(\\w+)<\\/ns0:aircraftRegistrationNumber>", 1))
      .withColumn("route", regexp_extract($"message","<ns0:route>(\\S+?)<\\/ns0:route>", 1))
      .withColumn("bchtFlightNumber", regexp_extract($"message","<ns0:bchtFlightNumber>(\\w+)<\\/ns0:bchtFlightNumber>", 1))
      .withColumn("bchtStaStd", regexp_extract($"message","<ns0:bchtStaStd>(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})<\\/ns0:bchtStaStd>", 1))
      .withColumn("bchtReasonId", regexp_extract($"message","<ns0:bchtReasonId>(\\w+)<\\/ns0:bchtReasonId>", 1).as("bchtReasonId"))
      .withColumn("bchtOldBayNo", regexp_extract($"message","<ns0:bchtOldBayNo>(\\w+)<\\/ns0:bchtOldBayNo>", 1))
      .withColumn("bchtNewBayNo", regexp_extract($"message","<ns0:bchtNewBayNo>(\\w+)<\\/ns0:bchtNewBayNo>", 1))
      .withColumn("srcCreateDate",regexp_extract($"message","<ns0:srcCreateDate>(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})<\\/ns0:srcCreateDate>", 1) )
      .withColumn("srcUpdateDate",regexp_extract($"message","<ns0:srcUpdateDate>(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})<\\/ns0:srcUpdateDate>", 1) )
      .withColumn("airlineCode", when ($"airlineCode" === "", null).otherwise($"airlineCode"))
      .withColumn("arrivalDepartureFlag", when ($"arrivalDepartureFlag" === "", null).otherwise($"arrivalDepartureFlag"))
      .withColumn("aircraftRegistrationNumber", when ($"aircraftRegistrationNumber" === "", null).otherwise($"aircraftRegistrationNumber"))
      .withColumn("route", when ($"route" === "", null).otherwise($"route"))
      .withColumn("bchtFlightNumber", when ($"bchtFlightNumber" === "", null).otherwise($"bchtFlightNumber"))
      .withColumn("bchtStaStd", when ($"bchtStaStd" === "", null).otherwise($"bchtStaStd"))
      .withColumn("bchtReasonId", when ($"bchtReasonId" === "", null).otherwise($"bchtReasonId"))
      .withColumn("bchtOldBayNo", when ($"bchtOldBayNo" === "", null).otherwise($"bchtOldBayNo"))
      .withColumn("bchtNewBayNo", when ($"bchtNewBayNo" === "", null).otherwise($"bchtNewBayNo"))
      .withColumn("srcCreateDate", when ($"srcCreateDate" === "", null).otherwise($"srcCreateDate"))
      .withColumn("srcUpdateDate", when ($"srcUpdateDate" === "", null).otherwise($"srcUpdateDate"))
      .drop($"message")
      .withColumn("HELIX_UUID",generateUUID())
      .withColumn("HELIX_TIMESTAMP",generateTimestamp())

    log.info("Completed parsing Dmis Bay history data")
    out_df
  }

  /**
    * overridden method of super class
    */
  override def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String] = Some("error"),
                           coalesceValue: Option[Int] = Some(args.part)): Unit =
    super.writeToHDFS(output, hdfsPath, outputFormat,saveMode, coalesceValue)
    log.info("Finished writing R2D data")
}
