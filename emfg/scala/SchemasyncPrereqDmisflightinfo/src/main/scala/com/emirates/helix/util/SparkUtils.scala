/*----------------------------------------------------------
 * Created on  : 03/01/2018
 * Author      : Krishnaraj Rajagopal
 * Email       : krishnaraj.rajagopal@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : SparkUtil.scala
 * Description : Class file for utility functions
 * ----------------------------------------------------------
 */
package com.emirates.helix.util


import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * @author Krishnaraj Rajagopal
  *
  * A utility class that contains common code for writing Spark applications
  */
trait SparkUtils {

  /**
    * Reads data from HDFS
    *
    *  @param sqlContext   SQLContext object
    *  @param hdfsPath     The path on HDFS where the results are to be stored
    *  @param inputFormat  The underlying file format. Currently Avro and Parquet are supported
    *  @return DataFrame object
    *
    */
  def readValueFromHDFS(hdfsPath: String, inputFormat: String, sqlContext: SQLContext): DataFrame = {

    import com.databricks.spark.avro._
    import org.apache.spark.sql.DataFrameReader

    val dfReader:DataFrameReader = sqlContext.read

    inputFormat.toLowerCase() match {
      case "parquet" ⇒ {
        Try { dfReader.parquet(hdfsPath) } match {
          case Failure(df)   ⇒ { throw new Exception(df.getCause) }
          case Success(succ) ⇒ succ
        }
      }
      case "avro" ⇒ {
        Try { dfReader.avro(hdfsPath) } match {
          case Failure(df)   ⇒ { throw new Exception(df.getCause) }
          case Success(succ) ⇒ succ
        }
      }
      case _ ⇒ throw new UnsupportedOperationException(s"The output format $inputFormat is not supported")
    }
  }

  /**
    * Writes out the result to HDFS
    *
    *  @param output  DataFrame:      The result of a computation
    *  @param hdfsPath      The path on HDFS where the results are to be stored
    *  @param outputFormat  The underlying file format. Currently Avro and Parquet are supported
    *  @param saveMode      The Save mode append or overwrite
    *  @param coalesceValue The coalesce value to use
    */
  def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String], coalesceValue: Option[Int]): Unit = {

    import com.databricks.spark.avro._
    import org.apache.spark.sql.DataFrameWriter
    val mode = saveMode.getOrElse("overwrite")
    var dfWriter: Option[DataFrameWriter] = None

    if (coalesceValue.isEmpty) {
      dfWriter = Some(output.write.mode(mode))
    } else {
      dfWriter = Some(output.coalesce(coalesceValue.get.toInt).write.mode(mode))
    }
    val result = dfWriter.getOrElse(throw new RuntimeException("Something went wrong while trying to coalesce final result"))
    outputFormat.toLowerCase() match {
      case "parquet" ⇒ result.parquet(hdfsPath)
      case "avro"    ⇒ result.avro(hdfsPath)
      case _         ⇒ throw new UnsupportedOperationException(s"The output format $outputFormat is not supported")
    }
  }

  /**
    * Creates a Spark Configuration object
    *
    * @param appName The name of the spark Application
    */
  def sparkConfig(appName: String = null): SparkConf = {
    val conf = new SparkConf()
    if (null != appName) conf.setAppName(appName)
      .set("spark.hadoop.mapreduce.output.fileoutputformat.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.shuffle.service.enabled","true")
      .set("spark.rdd.compress", "true")
      .set("spark.core.connection.ack.wait.timeout", "600")
      //.set("spark.akka.frameSize", "512")
      //.set("spark.akka.threads", "10")
      //.set("spark.eventLog.enabled", "true")
      .set("spark.io.compression.codec", "lzf")
      .set("spark.speculation", "true")
    conf
  }

  /**
    * Setting sqlcontext configurations
    * @param sc SparkContext to configure
    * @return SQLContext object
    */
  def getSQLContext(sc: SparkContext,compression: String = "avro"): SQLContext = {

    val sqlContext = new SQLContext(sc);
    setSqlContextConfig(sqlContext,compression)
    sqlContext
  }

  /**
    * Setting hivecontext configurations
    * @param sc SparkContext to configure
    * @return HiveContext object
    */
  def getHiveContext(sc: SparkContext, compression: String = "avro"): HiveContext = {

    val hiveContext = new HiveContext(sc);
    setSqlContextConfig(hiveContext,compression)
    hiveContext
  }

  /**
    * Decorates a Hive/SQLContext with parameters
    */
  def setSqlContextConfig(sqlContext: SQLContext, compression: String) {

    // Turn on parquet filter push-down,stats filtering, and dictionary filtering
    sqlContext.setConf("parquet.filter.statistics.enabled", "true")
    sqlContext.setConf("parquet.filter.dictionary.enabled", "true")
    sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")

    // Use non-hive read path
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "true")

    // Turn off schema merging
    sqlContext.setConf("spark.sql.parquet.mergeSchema", "false")
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet.mergeSchema", "false")

    // Set parquet compression
    sqlContext.setConf("spark.sql.parquet.compression.codec", compression)

    // Configure avro parameters
    sqlContext.setConf("spark.sql.avro.compression.codec", compression)
  }

}