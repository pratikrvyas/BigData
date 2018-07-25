/*----------------------------------------------------------------------------
 * Created on  : 01/Mar/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PayoadLoadsheetSchemaSyncArguments.scala
 * Description : Utility Class file
 * ----------------------------------------------------------
 */

package com.emirates.helix.util

import org.apache.spark.sql.SparkSession

trait SparkUtils {
  /**
    * Get spark session with the config parameters
    * @param compression output compression
    * @return spark session object
    */
  def getSparkSession(compression: String, appName: String = ""): SparkSession = {
    SparkSession.builder()
      .config("parquet.filter.statistics.enabled", "true") // Turn on parquet filter push-down,stats filtering, and dictionary filtering
      .config("parquet.filter.dictionary.enabled", "true")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.sql.hive.convertMetastoreParquet", "true") // Use non-hive read path
      .config("spark.sql.parquet.mergeSchema", "false")  // Turn off schema merging
      .config("spark.sql.hive.convertMetastoreParquet.mergeSchema", "false")
      .config("spark.sql.parquet.compression.codec", compression) // Set parquet compression
      .config("spark.sql.avro.compression.codec", compression) // Configure avro parameters
      .config("spark.hadoop.mapreduce.output.fileoutputformat.compress", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.rdd.compress", "true")
      .config("spark.core.connection.ack.wait.timeout", "600")
      //.set("spark.akka.frameSize", "512")
      //.set("spark.akka.threads", "10")
      //.set("spark.eventLog.enabled", "true")
      .config("spark.io.compression.codec", "lzf")
      .config("spark.speculation", "true")
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
  }
}

