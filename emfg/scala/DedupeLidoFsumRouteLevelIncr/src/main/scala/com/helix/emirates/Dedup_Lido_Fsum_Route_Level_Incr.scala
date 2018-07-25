/*
*=====================================================================================================================================
* Created on     :   20/03/2018
* Author         :   Ravindra Chellubani, Manu Mukundan
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   Dedup_Lido_Fsum_Route_Level_Incr
* Description    :   This is spark application is used to dedup lido fsum route level (HIST+INCR)
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.helix.emirates.Dedup_Lido_Fsum_Flight_Level_Incr" /home/ops_eff/ek_lido/Dedup_Lido_Fsum_Flight_Level_Incr-1.0-SNAPSHOT.jar -h /data/helix/modelled/flight_genome/dedup/lido/lido_fsum_flight_level/current/1.0/*/* -i /data/helix/modelled/flight_genome/prerequisite/lido_fsum_flight_level_with_drvd_cols/incremental/1.0/2018-01-08/07-30 -o /data/helix/modelled/flight_genome/dedup/lido/lido_fsum_flight_level/current/1.0/18-01-2018/00-99/ -c snappy -p 2

package com.helix.emirates

import com.databricks.spark.avro._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import java.text.SimpleDateFormat
import org.apache.spark.sql.Column

import org.apache.spark.sql.hive._


object Dedup_Lido_Fsum_Route_Level_Incr {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("Dedup_Lido_Fsum_Route_Level_Incr")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Dedup_Lido_Fsum_Flight_Level_Incr")

  def dedup(hist_input_df : DataFrame , incr_input_df : DataFrame) : DataFrame = {

    def replaceNullOrEmptyWithZero(c: Column): Column = {
      import org.apache.spark.sql.functions._;
      when(c.isNull,lit("0")).when(c.isNaN,lit("0")).when(c === "",lit("0")).otherwise(c)
    }

    //TRIM SPACES IN ALL COLS
    def trimAllColumns(df: DataFrame): DataFrame = {
      import org.apache.spark.sql.functions._;
      df.columns.foldLeft(df) { (memoDF, colName) =>
        memoDF.withColumn(colName, trim(col(colName)))
      }
    }

    //UNION ALL - HISTORY + INCREMENTAL
    def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
      val columns = a.columns.toSet.intersect(b.columns.toSet).map(col).toSeq
      println(columns)
      a.select(columns: _*).unionAll(b.select(columns: _*))
    }
    val union_all_df = unionByName(hist_input_df,incr_input_df)

    //ELIMINATE DUPLICATES AT RECORD LEVEL
    val drop_dup_df = union_all_df.dropDuplicates

    //ELIMINATE DUPLICATES AT FLIGHT LEVEL
    drop_dup_df.registerTempTable("drop_dup_tbl")

    val rank_df = hiveContext.sql("select *,row_number() OVER (PARTITION BY cast(trim(FliDupNo) as int),trim(FliNum),trim(DatOfOriUtc),trim(DepAirIat),trim(DesAirIat),trim(RegOrAirFinNum),NoOfWayOfMaiRouDepDes,FliDupNo,altitude,waypoint_seq_no,TrackWindSegment_drvd,mora_or_mea_drvd,waypointName ,latitude_drvd,longitude_drvd,dstToNxtWaypnt_drvd,estFltTimeToNxtWaypnt_drvd,accum_distance_drvd,remaining_fuel_drvd,altitude_drvd_upd,isa_dev_drvd,sr_tdev_drvd,segWndCmptCrs_drvd,wind_drvd,machno_drvd,tas_drvd,trop_drvd,cas_drvd,estFuelOnBrdOvrWaypnt,estTotFuelBurnOvrWaypnt,outbndTrueTrk,estElpTimeOvrWaypnt,SegTemp ,airdist_drvd,acc_airdist_drvd  ORDER BY trim(tibco_message_time) DESC) as rank from drop_dup_tbl")

    rank_df.registerTempTable("rank_tbl")

    val latest_df = hiveContext.sql("select * from rank_tbl where rank = 1")

    //FORMATTING FLIGHT DATE TIME - YYYY-MM-DD HH:MM:SS
    val dateformatter = hiveContext.udf.register("dateformatter", (datetime : String) =>
      (
        //2018-03-20T15:35:23.92+04:00
        if(datetime.trim().matches("""(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d).(\d)"""))
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
        else
          datetime.trim()
        )
    )

    val tibco_date_df = latest_df.withColumn("tibco_message_time",dateformatter($"tibco_message_time"))
    val date_df = tibco_date_df.withColumn("DatOfOriUtc",dateformatter($"DatOfOriUtc"))

    //FORMATTING FLIGHT NUMBER TIME - 4 DIGITS
    val flightnumformatter = hiveContext.udf.register("flightnumformatter", (flinum : String) =>
      (
        if(flinum.trim().matches("""(\d\d\d)"""))
          "0".concat(flinum.trim())
        else if(flinum.trim().matches("""(\d\d\d\d)"""))
          flinum.trim()
        else if(flinum.trim().matches("""(\d\d)"""))
          "00".concat(flinum.trim())
        else if(flinum.trim().matches(("""(\d)""")))
          "000".concat(flinum.trim())
        else if(flinum.trim().matches(("""([0-9 a-z A-Z][0-9 a-z A-Z][0-9 a-z A-Z][0-9 a-z A-Z])""")))
          flinum.trim()
        else if(flinum.trim().matches(("""([0-9 a-z A-Z][0-9 a-z A-Z][0-9 a-z A-Z])""")))
          "0".concat(flinum.trim())
        else if(flinum.trim().matches(("""([0-9 a-z A-Z][0-9 a-z A-Z])""")))
          "00".concat(flinum.trim())
        else if(flinum.trim().matches(("""([0-9 a-z A-Z])""")))
          "000".concat(flinum.trim())
        else
          flinum.trim()
        )
    )

    val flinum_df = date_df.withColumn("FliNum",flightnumformatter($"FliNum"))

    //FORMATTING TAIL NUMBER - REMOVE "-"
    val tailnumformatter = hiveContext.udf.register("tailnumformatter", (tailnum : String) =>
      (
        tailnum.trim().replace("-","").replace(" ","")
        )
    )

    val tailnum_df = flinum_df.withColumn("RegOrAirFinNum",tailnumformatter($"RegOrAirFinNum"))

    //FORMATTING STATION IATA - 3 CHARACTERS
    val stationformatter = hiveContext.udf.register("stationformatter", (stat : String) =>
      (
        if(stat.trim().matches("""([a-z A-Z][a-z A-Z][a-z A-Z])"""))
          stat.trim()
        else
          stat.trim()
        )
    )

    val depstat_df = tailnum_df.withColumn("DepAirIat",stationformatter($"DepAirIat"))
    val arrstat_df = depstat_df.withColumn("DesAirIat",stationformatter($"DesAirIat"))

    //FILTERING BAD RECORDS IF ANY
    arrstat_df.registerTempTable("arrstat_tbl")
    val bad_df = hiveContext.sql("select * from arrstat_tbl where substring(trim(FliNum),length(FliNum)-4) = '-bad' or substring(trim(DepAirIat),length(DepAirIat)-4) = '-bad' or substring(trim(DatOfOriUtc),length(DatOfOriUtc)-4) = '-bad' or substring(trim(DesAirIat),length(DesAirIat)-4) = '-bad'")

    //GETTING DEDUP DATA
    val dedup_df = hiveContext.sql("select * from arrstat_tbl where substring(trim(FliNum),length(FliNum)-4) != '-bad' or substring(trim(DepAirIat),length(DepAirIat)-4) != '-bad' or substring(trim(DatOfOriUtc),length(DatOfOriUtc)-4) != '-bad' or substring(trim(DesAirIat),length(DesAirIat)-4) != '-bad'")

    //ELIMINATE DUPLICATES AT RECORD LEVEL
    val drop_dup_dedup_df = dedup_df.dropDuplicates



//    //UNIT TEST
//    union_all_df.registerTempTable("union_all_tbl")
//    val distinct_df = hiveContext.sql("select distinct FliNum,DepAirIat,DatOfOriUtc,DesAirIat,RegOrAirFinNum,FliDupNo from  union_all_tbl")
//
//    val trg_cnt = dedup_df.count
//    val src_cnt = distinct_df.count
//
//    val msg = if(trg_cnt == src_cnt) "Unit Test Passed Counts Matched Source count :"+ src_cnt +" Target Count "+ trg_cnt else "Unit Test Failed Counts UnMatched Source count :"+ src_cnt +" Target Count "+ trg_cnt
//    log.info("[INFO] " + msg)


    return  drop_dup_dedup_df
  }

  def writetohdfs(output_df : DataFrame,spark_partitions : Int,hdfs_output_path_incr : String) = {

    val output_df_last_incr_coalesece = output_df.coalesce(spark_partitions)
    hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_incr +"...")
    output_df_last_incr_coalesece.write.mode("overwrite").parquet(hdfs_output_path_incr)
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_incr +" successfully")

  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_hist_input_path : String = null,hdfs_incr_input_path : String = null,hdfs_output_path_incr: String = null, compression: String = null, spark_partitions : Int = 0)

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
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_hist_input_path + "...")
      val hist_input_df = hiveContext.read.parquet(config.hdfs_hist_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_hist_input_path + "is successfull")

      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_incr_input_path + "...")
      val incr_input_df = hiveContext.read.avro(config.hdfs_incr_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_incr_input_path + "is successfull")

      //CALLING HHMMTOMins Function
      log.info("[INFO] PERFORMING DEDUP (UNION,DUPLICATES,LATEST) ...")
      val output_df = dedup(hist_input_df,incr_input_df)
      log.info("[INFO] PERFORMING DEDUP (UNION,DUPLICATES,LATEST) IS COMPLETED SUCCESSFULLY")

      //WRITING DATA TO PREREQUISITE IN AVRO FORMAT WITH SNAPPY COMPRESSION
      writetohdfs(output_df,config.spark_partitions,config.hdfs_output_path_incr)

    }
  }


}
