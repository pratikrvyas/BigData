/*----------------------------------------------------------------------------
 * Created on  : 20/01/2018
 * Author      : Krishnaraj Rajagopal(s746216)
 * Email       : krishnaraj.rajagopal@emirates.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : SparkUtils.scala
 * Description : Common Util class for Operation Efficiency.
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix.emfg.common

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

trait SparkUtils {

  /**
    *
    * @param hdfsPath    The path on HDFS where the results are to be stored
    * @param inputFormat The underlying file format. Currently Avro and Parquet are supported
    * @param sqlContext  SqlContext
    * @return
    */
  def readValueFromHDFS(hdfsPath: String, inputFormat: String, sqlContext: SQLContext): DataFrame = {

    val dfReader = sqlContext.read

    inputFormat.toLowerCase() match {
      case "parquet" ⇒
        Try {
          dfReader.parquet(hdfsPath)
        } match {
          case Failure(df) ⇒
            throw new Exception(df.getCause)

          case Success(succ) ⇒ succ
        }

      case "avro" ⇒
        Try {
          dfReader.format("com.databricks.spark.avro").load(hdfsPath)
        } match {
          case Failure(df) ⇒
            throw new Exception(df.getCause)

          case Success(succ) ⇒ succ
        }

      case _ ⇒ throw new UnsupportedOperationException(s"The output format $inputFormat is not supported")
    }
  }

  /**
    *
    * @param output        DataFrame:      The result of a computation
    * @param hdfsPath      The path on HDFS where the results are to be stored
    * @param outputFormat  The underlying file format. Currently Avro and Parquet are supported
    * @param saveMode      The Save mode append or overwrite
    * @param coalesceValue The coalesce value to use
    */
  def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String], coalesceValue: Option[Int]): Unit = {

    import com.databricks.spark.avro._
    import org.apache.spark.sql.DataFrameWriter
    val mode = saveMode.getOrElse("overwrite")
    var dfWriter: Option[DataFrameWriter] = None

    if (coalesceValue.isEmpty) {
      dfWriter = Some(output.write.mode(mode))
    } else {
      dfWriter = Some(output.coalesce(coalesceValue.get).write.mode(mode))
    }
    val result = dfWriter.getOrElse(throw new RuntimeException("Something went wrong while trying to coalesce final result"))
    outputFormat.toLowerCase() match {
      case "parquet" ⇒ result.parquet(hdfsPath)
      case "avro" ⇒ result.avro(hdfsPath)
      case _ ⇒ throw new UnsupportedOperationException(s"The output format $outputFormat is not supported")
    }
  }

  /**
    * Creates a Spark Configuration object
    *
    * @param appName The name of the spark Application
    */
  def sparkConfig(appName: String): SparkConf = {
    val conf = new SparkConf().setAppName(appName)
      .set("spark.hadoop.mapreduce.output.fileoutputformat.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.broadcast.compress", "true")
      //.set("spark.core.connection.ack.wait.timeout", "600")
      //.set("spark.sql.autoBroadcastJoinThreshold", "1073741824")
      //.set("spark.akka.frameSize", "512")
      //.set("spark.akka.threads", "10")
      //.set("spark.eventLog.enabled", "true")
      //.set("spark.io.compression.codec", "lzf")
    //.set("spark.speculation", "true")
    conf
  }

  /**
    *
    * @param sc The SparkContext to configure
    * @return
    */
  def getSQLContext(sc: SparkContext): SQLContext = {

    val sqlContext = new SQLContext(sc)
    setSqlContextConfig(sqlContext)
    sqlContext
  }

  /**
    * Decorates a SQLContext with parameters
    */
  def setSqlContextConfig(sqlContext: SQLContext): Unit = {

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
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    // Configure avro parameters
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
  }

}