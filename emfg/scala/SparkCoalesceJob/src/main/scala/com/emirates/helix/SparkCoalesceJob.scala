package com.emirates.helix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import com.databricks.spark.avro._


object SparkCoalesceJob {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("Dedup_Lido_Fsum_Flight_Level_Hist_Incr")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Dedup_Lido_Fsum_Flight_Level_Hist_Incr")

  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path: String = null, compression: String = null, spark_partitions : Int = 0)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("SparkCoalesceJob") {
      head("SparkCoalesceJob")
      opt[String]('i', "hdfs_input_path")
        .required()
        .action((x, config) => config.copy(hdfs_input_path = x))
        .text("Required parameter : Input file path for data")
      opt[String]('o', "hdfs_output_path")
        .required()
        .action((x, config) => config.copy(hdfs_output_path = x))
        .text("Required parameter : Output file path for data")
      opt[Int]('p', "spark_partitions")
        .required()
        .action((x, config) => config.copy(spark_partitions = x))
        .text("Required parameter : Output file path for data")
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

      //READING DATA FORM PREREQUISITE
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_input_path + "...")
      val hist_input_df = hiveContext.read.avro(config.hdfs_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_input_path + "is successfull")

      log.info("[INFO] PERFORMING COALESCE OPERATION WITH " + config.spark_partitions)
      val output_df = hist_input_df.coalesce(config.spark_partitions)
      log.info("[INFO] PERFORMING COALESCE OPERATION WITH " + config.spark_partitions + "successfully")

      log.info("[INFO] PERFORMING COALESCE OPERATION WITH " + config.spark_partitions)
      output_df.write.avro(config.hdfs_output_path)
      log.info("[INFO] PERFORMING COALESCE OPERATION WITH " + config.spark_partitions + "successfully")

    }
  }


}
