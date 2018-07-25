package com.emirates.helix.emfg.util


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, lit, udf}

trait FightDetailsActualUtils {

  //val COLUMN_LIST_FLIGHT_ACT_EPIC_DETAILS = List("HOLD_ENTRY", "HOLD_EXIT", "HOLD_AREA", "EVENT_TYPE", "NEAREST_STAR", "HOLD_TIME")

  val HISTORY: String = "History"

  val FLIGHT_DATE_COLUMN_NAME: String = "flight_date"

  val ACTUAL_FLIGHT_DETAILS_OUTPUT_TEMP_TABLE: String = "actual_flight_details_output_tab"

  /**
    * To convert a set of columns as Map <String,String>
    *
    */
  val asMap = udf((keys: Seq[String], values: Seq[String]) => keys.zip(values).toMap)

  /**
    * Method to convert struct to Map
    */

  def toMap(df: DataFrame, colList: List[String], newColName: String): DataFrame = {

    val keys = array(colList.map(lit): _*)
    val values = array(colList.map(col): _*)
    val mapDF = df.withColumn(newColName, asMap(keys, values))
    mapDF
  }

  /** Method to write processed data to HDFS
    *
    * @param out_df Dataframe to be written to HDFS
    */
  def writeOutput(out_df: DataFrame, look_back_months: Int, coalesce_value: Int,
                  output_history_location: String,
                  output_current_location: String, tempTableName: String, filterColumnName: String, compressionFormat: String)(implicit spark: SparkSession): Unit = {
    out_df.createOrReplaceTempView(tempTableName)

    val max_flight_date = spark.sql("select max(trim(" + filterColumnName + ")) " +
      s" from ${tempTableName}").first().mkString

    val incr_sql = "select * from " + tempTableName + " where to_date(trim(" + filterColumnName + ")) " +
      " >= add_months(to_date('" + max_flight_date + "')," + -1 * look_back_months + ")"

    val hist_sql = "select * from " + tempTableName + " where to_date(trim(" + filterColumnName + ")) " +
      " < add_months(to_date('" + max_flight_date + "')," + -1 * look_back_months + ")"

    val output_df_last_incr = spark.sql(incr_sql).coalesce(coalesce_value)
    val output_df_last_hist = spark.sql(hist_sql).coalesce(coalesce_value)

    output_df_last_hist.write.format("parquet").option("spark.sql.parquet.compression.codec", compressionFormat).save(output_history_location)
    output_df_last_incr.write.format("parquet").option("spark.sql.parquet.compression.codec", compressionFormat).save(output_current_location)

  }
}
