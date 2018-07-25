/*----------------------------------------------------------
 * Created on  : 03/01/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : SparkUtil.scala
 * Description : Class file for utility functions
 * ----------------------------------------------------------
 */
package com.emirates.helix.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Spark utility class with utility methods
  */
object SparkUtil {

  /** Utility function to setup the SQLContext object
    *
    *  @param compression compression format
    *  @param sc SparkContext
    *  @return  SQLContext object
    */
  def setSqlContext(compression: String) (implicit sc: SparkContext): SQLContext = {

    val sqlContext = new SQLContext(sc)
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
    sqlContext
  }
}
