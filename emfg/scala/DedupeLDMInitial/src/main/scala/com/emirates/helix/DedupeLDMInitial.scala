/*
*=====================================================================================================================================
* Created on     :   17/01/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   Dedupe_LDM_Initial
* Description    :   This is spark application is used to dedup ldm fsum flight level (HIST+INCR)
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.helix.emirates.DedupeLDMDaily" /home/ops_eff/ek_ldm/Dedupe_LDM_Initial-1.0-SNAPSHOT.jar -h /data/helix/modelled/emfg/sync/altea/ldm/2018-04-29/00-00 -i /data/helix/modelled/emfg/prerequisite/altea/ldm/incremental/2018-04-24/ -o /data/helix/modelled/emfg/dedupe/altea/ldm/current -v /data/helix/modelled/emfg/dedupe/altea/ldm/history -r 15 -c snappy -p 2

package com.emirates.helix

import com.databricks.spark.avro._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive._
import org.apache.spark.sql.Column
import java.text.SimpleDateFormat

object DedupeLDMInitial {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("Dedup_ldm_Fsum_Flight_Level_Hist_Incr")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Dedup_ldm_Fsum_Flight_Level_Hist_Incr")

  def dedup(hist_input_df : DataFrame , incr_input_df : DataFrame, spark_partitions : Int) : DataFrame = {

    //val ip = sqlContext.read.parquet("/data/helix/modelled/emfg/dedupe/altea/ldm/current")
    def replaceNullOrEmptyWithZero(c: Column): Column = {
      import org.apache.spark.sql.functions._;
      when(c.isNull, lit("0")).when(c.isNaN, lit("0")).when(c === "", lit("0")).otherwise(c)
    }

    //TRIM SPACES IN ALL COLS
    def trimAllColumns(df: DataFrame): DataFrame = {
      import org.apache.spark.sql.functions._;
      df.columns.foldLeft(df) { (memoDF, colName) =>
        memoDF.withColumn(colName, trim(col(colName)))
      }
    }

    //FORMATTING FLIGHT DATE TIME - YYYY-MM-DD HH:MM:SS
    val dateformatter = hiveContext.udf.register("dateformatter", (datetime : String) =>
      (
        //2018-03-20T15:35:23.92+04:00
        if(datetime == null)
          null
        else if(datetime.trim().matches("""(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d).(\d)"""))
          datetime.trim().substring(0,19)
        else if(datetime.trim().matches("""(\d\d)([a-z A-Z][a-z A-Z][a-z A-Z])(\d\d\d\d)""")){
          val inputFormat1 = new SimpleDateFormat("ddMMMyyy")
          val outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          outputFormat.format(inputFormat1.parse(datetime.trim())).toString
        }
        else if(datetime.trim().matches("""(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)"""))
          datetime.trim()
        else if (datetime.trim().matches("""(\d\d\d\d)-(\d\d)-(\d\d)([a-z A-Z])(\d\d):(\d\d):(\d\d)\.(\d\d\d)\+(\d\d):(\d\d)"""))
          datetime.trim().replace("T"," ").substring(0,19)
        else if (datetime.trim().contains("T"))
          datetime.trim().replace("T"," ").substring(0,19)
        else if(datetime.trim().matches("""(\d\d\d\d)-(\d\d)-(\d\d)""")){
          val inputFormat1 = new SimpleDateFormat("yyyy-MM-dd")
          val outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          outputFormat.format(inputFormat1.parse(datetime.trim())).toString
        }
        else
          datetime.trim()
        )
    )

    //FORMATTING TAIL NUMBER - REMOVE "-"
    val tailnumformatter = hiveContext.udf.register("tailnumformatter", (tailnum : String) =>
      (
        if( tailnum != null && !tailnum.equals(null) && !tailnum.isEmpty ) {tailnum.trim().replace("-","").replace(" ","")}
        else{null}
        )
    )

    //UNION ALL - HISTORY + INCREMENTAL
    def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
      val columns = a.columns.toSet.intersect(b.columns.toSet).map(col).toSeq
      println(columns)
      a.select(columns: _*).unionAll(b.select(columns: _*))
    }

    //FILTER RECORDS WITH STATUS ! = y
    log.info("[INFO] FILTERING RECORDS WITH STATUS NOT EQUAL TO Y")
    val incr_input_df_Y = incr_input_df.where($"latest_status" === "Y")
    log.info("[INFO] FILTERING RECORDS WITH STATUS NOT EQUAL Y COMPLETED SUCCESSFULLY")

    val union_all_df = unionByName(hist_input_df.drop($"rowno"), incr_input_df_Y.drop($"rowno"))

    //ELIMINATE DUPLICATES AT RECORD LEVEL
    val drop_dup_df = union_all_df.repartition(spark_partitions * 100).withColumn("tibco_messageTime",dateformatter($"tibco_messageTime")).withColumn("aircraft_reg",tailnumformatter($"aircraft_reg")).dropDuplicates().cache()

    //FLIGHT DATE & TRIM SPACES
    val fdts = trimAllColumns(drop_dup_df).withColumn("flight_date",dateformatter($"flight_date"))

    //ELIMATE DUPLICATES IF ANY AT FLIGHT IDENTIFIER AND SELECT LATEST
    val dup_cnt = fdts.select("flight_no","flight_date","aircraft_reg","dep_station","arr_station").groupBy($"flight_no",$"flight_date",$"aircraft_reg",$"dep_station",$"arr_station").count.where($"count" > 1).count()

    log.info("[INFO] DUPLICATES WITH RESPECT TO FLIGHT IDENTIFIERS IS :" + dup_cnt)

    if(dup_cnt == 0)
    {
      return trimAllColumns(fdts).withColumn("cargo_weight",$"cargo_weight".cast("Int")).withColumn("mail_weight",$"mail_weight".cast("Int")).withColumn("transit_weight",$"transit_weight".cast("Int")).withColumn("baggage_weight",$"baggage_weight".cast("Int")).withColumn("miscl_weight",$"miscl_weight".cast("Int"))
    }
    else
    {
      fdts.registerTempTable("drop_dup_tbl")

      val rank1_df = hiveContext.sql("select *,row_number() OVER (PARTITION BY  flight_no,flight_date,aircraft_reg,dep_station,arr_station order by cast(tibco_messageTime as timestamp)) as rowno from drop_dup_tbl").where($"rowno" === 1)

      return trimAllColumns(rank1_df).withColumn("cargo_weight",$"cargo_weight".cast("Int")).withColumn("mail_weight",$"mail_weight".cast("Int")).withColumn("transit_weight",$"transit_weight".cast("Int")).withColumn("baggage_weight",$"baggage_weight".cast("Int")).withColumn("miscl_weight",$"miscl_weight".cast("Int"))
    }

  }

  def writetohdfs(output_df : DataFrame,months : Int,spark_partitions : Int,hdfs_output_path_incr : String, hdfs_output_path_hist : String) = {

    hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    output_df.registerTempTable("ldm_dedup_temp_tbl")

    val max_flight_date = hiveContext.sql("select max(flight_date) from ldm_dedup_temp_tbl").first().mkString

    log.info("[INFO] MAX FLIGHT DATE IS "+ max_flight_date +"...")

    val incr_sql = "select * from ldm_dedup_temp_tbl where to_date(flight_date) >= add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"
    val hist_sql = "select * from ldm_dedup_temp_tbl where to_date(flight_date) < add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"

    log.info("[INFO] HISTORY SQL QUERY "+ hist_sql +"...")
    log.info("[INFO] INCREMENTAL SQL QUERY "+ incr_sql +"...")

    val output_df_last_incr = hiveContext.sql(incr_sql)
    val output_df_last_hist = hiveContext.sql(hist_sql)

    val output_df_last_incr_coalesece = output_df_last_incr.coalesce(spark_partitions)
    val output_df_last_hist_coalesece = output_df_last_hist.coalesce(spark_partitions)

    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_incr +"...")
    output_df_last_incr_coalesece.write.mode("overwrite").parquet(hdfs_output_path_incr)
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_incr +" successfully")

    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_hist +"...")
    output_df_last_hist_coalesece.write.mode("overwrite").parquet(hdfs_output_path_hist)
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_hist +" successfully")
  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_hist_input_path : String = null,hdfs_incr_input_path : String = null,hdfs_output_path_incr: String = null, compression: String = null, spark_partitions : Int = 0, no_of_months : Int = 0,hdfs_output_path_hist : String = null)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("Dedup_Lido_Fsum_Route_Level_Hist_Incr") {
      head("Dedup_Lido_Fsum_Route_Level_Hist_Incr")
      opt[String]('h', "hdfs_hist_input_path")
        .required()
        .action((x, config) => config.copy(hdfs_hist_input_path = x))
        .text("Required parameter : Input file path for data")
      opt[String]('i', "hdfs_incr_input_path")
        .required()
        .action((x, config) => config.copy(hdfs_incr_input_path = x))
        .text("Required parameter : Input file path for data")
      opt[String]('o', "hdfs_output_path_incr")
        .required()
        .action((x, config) => config.copy(hdfs_output_path_incr = x))
        .text("Required parameter : Output file path for data")
      opt[String]('v', "hdfs_output_path_hist")
        .required()
        .action((x, config) => config.copy(hdfs_output_path_hist = x))
        .text("Required parameter : Output file path for data")
      opt[Int]('r', "no_of_months")
        .required()
        .action((x, config) => config.copy(no_of_months = x))
        .text("Required parameter : no of months to segregate the data")
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

      //READING DATA
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_hist_input_path + "...")
      val hist_input_df = hiveContext.read.avro(config.hdfs_hist_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_hist_input_path + "is successfull")

      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_incr_input_path + "...")
      val incr_input_df = hiveContext.read.avro(config.hdfs_incr_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_incr_input_path + "is successfull")

      //CALLING dedupe
      log.info("[INFO] PERFORMING DEDUP (UNION,DUPLICATES,LATEST) ...")
      val output_df = dedup(hist_input_df,incr_input_df,config.spark_partitions)
      log.info("[INFO] PERFORMING DEDUP (UNION,DUPLICATES,LATEST) IS COMPLETED SUCCESSFULLY")

      //WRITING DATA TO PREREQUISITE IN AVRO FORMAT WITH SNAPPY COMPRESSION
      writetohdfs(output_df,config.no_of_months,config.spark_partitions,config.hdfs_output_path_incr,config.hdfs_output_path_hist)

    }
  }

}
