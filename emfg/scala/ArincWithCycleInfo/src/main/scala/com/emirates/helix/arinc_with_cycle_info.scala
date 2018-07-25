/*
*=====================================================================================================================================
* Created on     :   10/12/2017
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   arinc_with_cycle_info.scala
* Description    :   This is spark application to add cycle start date and end date to arinc decomposed data
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.emirates.helix.arinc_with_cycle_info" /home/ops_eff/ek_arinc/arinc_scripts/arinc_with_cycle_info-1.0.jar -i /data/helix/decomposed/arinc/airport/incremental/1.0/2017-11-06/09-36/*.avro -o /user/ops_eff/arinc/airport/incremental/1.0/2017-11-06/09-39/ -s /apps/helix/controlfiles/arinc/cycle_date/ -t 10 -c snappy

package com.emirates.helix

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import org.joda.time._
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

object arinc_with_cycle_info {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("arinc_with_cycle_info")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("arinc_with_cycle_info")


  def getStartDate(ctrlfile_path : String) : String ={
    val ctrlfile_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true") .option("inferSchema", "true") .load(ctrlfile_path)
    val ctrlfile_df_persist = ctrlfile_df.persist()
    ctrlfile_df_persist.registerTempTable("ctrlfile_tbl")
    val max_row_df = sqlContext.sql("select * from ctrlfile_tbl order by to_date(start_date) desc limit 1")
    val cycle_days = (max_row_df.select("no_of_days").map(r => r.getInt(0)).collect.toList)(0)
    val end_row_df =  max_row_df.select(date_add($"start_date",-(cycle_days+1)).as("start_date"),$"no_of_days")
    val new_cycle_date = ((end_row_df.select("start_date").map(r => r.getDate(0)).collect.toList)(0)).toString
    log.info("[INFO] Start Cycle Date from the Control file is : "+new_cycle_date)

    return new_cycle_date
  }

  //logic to Calculate Next Cycle Date
  def nextCycleDate(ctrlfile_path : String) = {
    log.info("[INFO] Calculating Next Cycle Date ")
    val ctrlfile_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true") .option("inferSchema", "true") .load(ctrlfile_path)
    val ctrlfile_df_persist = ctrlfile_df.persist()
    ctrlfile_df_persist.registerTempTable("ctrlfile_tbl")
    val max_row_df = sqlContext.sql("select * from ctrlfile_tbl order by to_date(start_date) desc limit 1")
    val cycle_days = (max_row_df.select("no_of_days").map(r => r.getInt(0)).collect.toList)(0)
    val cycle_date = (max_row_df.select("start_date").map(r => r.getString(0)).collect.toList)(0)
    val end_row_df =  max_row_df.select(date_add($"start_date",cycle_days+1).as("start_date"),$"no_of_days")
    val new_cycle_date = ((end_row_df.select("start_date").map(r => r.getDate(0)).collect.toList)(0)).toString
    val new_ctrlfile_df = ctrlfile_df_persist.unionAll(end_row_df)
    log.info("[INFO] Current Cycle Date 	: "+ cycle_date)
    log.info("[INFO] No of Cycle Days 	: "+ cycle_days)
    log.info("[INFO] New Cycle Date 		: "+ new_cycle_date)

    (cycle_date,new_cycle_date,new_ctrlfile_df)
  }

  //logic to Calculate Previous Cycle Date
  def prevCycleDate(ctrlfile_path : String) = {
    log.info("[INFO] Calculating Previous Cycle Date ")
    val ctrlfile_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true") .option("inferSchema", "true") .load(ctrlfile_path)
    ctrlfile_df.registerTempTable("ctrlfile_tbl")
    val max_row_df = sqlContext.sql("select * from ctrlfile_tbl order by to_date(start_date) desc limit 1")
    val cycle_days = (max_row_df.select("no_of_days").map(r => r.getInt(0)).collect.toList)(0)
    val cycle_date = (max_row_df.select("start_date").map(r => r.getString(0)).collect.toList)(0)
    val end_row_df =  max_row_df.select(date_add($"start_date",-(cycle_days+1)).as("start_date"),$"no_of_days")
    val new_cycle_date = ((end_row_df.select("start_date").map(r => r.getDate(0)).collect.toList)(0)).toString
    log.info("[INFO] Current Cycle Date 	: "+ cycle_date)
    log.info("[INFO] No of Cycle Days 	: "+ cycle_days)
    log.info("[INFO] New Cycle Date 		: "+ new_cycle_date)

    (cycle_date,new_cycle_date)
  }

  //logic to update control file with Next Cycle Date
  def updateCtrlfile(new_ctrlfile_df :  DataFrame, ctrlfile_path : String) = {
    log.info("[INFO] Updating Control file with new cycle date")
    new_ctrlfile_df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").mode("overwrite").save(ctrlfile_path)
    log.info("[INFO] Updated Control file with new cycle date successfully")
  }

  //logic to add new cycle date to arinc data
  def addNewColumns(control_file_path : String, threshold_date : String, hdfs_input_path : String, hdfs_output_path : String) = {

    val start_date = new DateTime(getStartDate(control_file_path))
    val d = Days.daysBetween(start_date, DateTime.now)
    val days = d.getDays()

    log.info("[INFO] No of day between start date and now is :" + days)

    if(days >= threshold_date.trim.toInt) {
      log.info("[INFO] Processing the data with new cycle dates")
      log.info("[INFO] Adding New Columns (new_cycle_date,cycle_date) to data")
      val (cycle_date,new_cycle_date,new_ctrlfile_df) = nextCycleDate(control_file_path)
      val arinc_input_df = sqlContext.read.avro(hdfs_input_path).withColumn("cycle_start_date", lit(cycle_date)).withColumn("cycle_end_date", lit(new_cycle_date))
      //WRITING DATA TO DECOMPOSED IN AVRO FORMAT WITH SNAPPY COMPRESSION
      sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
      arinc_input_df.write.avro(hdfs_output_path)
      log.info("[INFO] Added New Columns (cycle_date, new_cycle_date) to data successfully")
      updateCtrlfile(new_ctrlfile_df,control_file_path)
    }
    else if(days < threshold_date.trim.toInt){
      log.info("[INFO] Reprocessing the data with current cycle dates")
      log.info("[INFO] Adding New Columns (new_cycle_date,cycle_date) to data")
      val (cycle_date,new_cycle_date) = prevCycleDate(control_file_path)
      val arinc_input_df = sqlContext.read.avro(hdfs_input_path).withColumn("cycle_start_date", lit(new_cycle_date)).withColumn("cycle_end_date", lit(cycle_date))
      //WRITING DATA TO DECOMPOSED IN AVRO FORMAT WITH SNAPPY COMPRESSION
      sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
      arinc_input_df.write.avro(hdfs_output_path)
      log.info("[INFO] Added New Columns (new_cycle_date,cycle_date) to data successfully")
    }
  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path: String = null, control_file: String = null, threshold_date : String = null, compression: String = null)

  def main(args:Array[String]) : Unit = {
    val parser = new scopt.OptionParser[Config]("arinc_with_cycle_info") {
      head("arinc_with_cycle_info")
      opt[String]('i', "hdfs_input_path")
        .required()
        .action((x, config) => config.copy(hdfs_input_path = x))
        .text("Required parameter : Input file path for data")
      opt[String]('o', "hdfs_output_path")
        .required()
        .action((x, config) => config.copy(hdfs_output_path = x))
        .text("Required parameter : Output file path for data")
      opt[String]('s', "control_file")
        .required()
        .action((x, config) => config.copy(control_file = x))
        .text("Required parameter : controlfile path for cycle info")
      opt[String]('t', "threshold_date")
        .required()
        .action((x, config) => config.copy(threshold_date = x))
        .text("Required parameter : threshold date for cycle info")
      opt[String]('c', "compression")
        .required()
        .valueName("snappy OR deflate")
        .validate(x => {
          if (!(x.matches(compress_rex.toString()))) Left("Invalid compression specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(compression = x))
        .text("Required parameter : Output data compression format, snappy or deflate")
    }

    parser.parse(args, Config()) map { config =>
      //ctrlfile = "/apps/helix/controlfiles/arinc/cycle_date/"
      //input = "/data/helix/decomposed/arinc/airport/incremental/1.0/2017-11-06/09-36/*.avro"
      //output = "/user/ops_eff/arinc/airport/incremental/1.0/2017-11-06/09-36/"
      addNewColumns(config.control_file,config.threshold_date,config.hdfs_input_path,config.hdfs_output_path)

    }

  }


}
