/*
*=====================================================================================================================================
* Created on     :   17/01/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   Dedupe_EPIC_Initial
* Description    :   This is spark application is used to dedup epic flight level (HIST+INCR)
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.emirates.helix.DedupeEpicInitial" /home/ops_eff/ek_epic/DedupeEpicInitial-1.0-SNAPSHOT.jar  --epic_t_int_core-hist /data/helix/raw/epic/epic_t_int_core/full/1.0/2018-04-01/00-00 --epic_calc_eta-hist /data/helix/raw/epic/epic_calc_eta/full/1.0/2018-04-01/00-00 --epic_hold_snapshot-hist /data/helix/raw/epic/epic_hold_snapshot/full/1.0/2018-04-01/00-00 --epic_t_int_core-incr /data/helix/raw/epic/epic_t_int_core/incremental/1.0/2018-05-03/05-00 --epic_calc_eta-incr /data/helix/raw/epic/epic_calc_eta/incremental/1.0/2018-04-29/00-00 --epic_hold_snapshot-incr /data/helix/raw/epic/epic_hold_snapshot/incremental/1.0/2018-04-29/00-00 --hist-output /data/helix/modelled/emfg/dedupe/epic/history --incr-output /data/helix/modelled/emfg/dedupe/epic/history --lookback-months 15  --compression snappy --spark-partitions 2

package com.emirates.helix
import org.kohsuke.args4j.{CmdLineException, CmdLineParser}

import com.databricks.spark.avro._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive._
import org.apache.spark.sql.Column
import java.text.SimpleDateFormat

object DedupeEpicInitial {

  val conf = new SparkConf().setAppName("DedupeEpicInitial")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("DedupeEpicInitial")


  //UNION ALL - HISTORY + INCREMENTAL
  def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
    val columns = a.columns.toSet.intersect(b.columns.toSet).map(col).toSeq
    println(columns)
    a.select(columns: _*).unionAll(b.select(columns: _*))
  }


  def dedupe_hold(epic_hold_snapshot_hist: String,epic_hold_snapshot_incr: String): DataFrame = {

    log.info("[INFO] READING INPUT DATA :" + epic_hold_snapshot_hist)
    //val epic_hold_snapshot_hist = "	/data/helix/decomposed/epic/epic_hold_snapshot/full/1.0/2018-04-01/00-00"
    val hold_hist = hiveContext.read.avro(epic_hold_snapshot_hist).drop($"UUID").drop($"TIMESTAMP")
    val epic_hold_snapshot_hist_drop_dup = hold_hist.dropDuplicates()

    log.info("[INFO] READING INPUT DATA :" + epic_hold_snapshot_incr)
    //val epic_hold_snapshot_incr =  "/data/helix/decomposed/epic/epic_hold_snapshot/incremental/1.0/2018-04-29/01-30"
    val hold_incr = hiveContext.read.avro(epic_hold_snapshot_incr).drop($"UUID").drop($"TIMESTAMP")
    val epic_hold_snapshot_incr_drop_dup = hold_incr.dropDuplicates()

    log.info("[INFO] UNION OF HISTORY AND INCREMENT DATA")
    val hold_union_all_df = unionByName(epic_hold_snapshot_hist_drop_dup, epic_hold_snapshot_incr_drop_dup)

    val dup_cnt = hold_union_all_df.groupBy($"ETA_CALC_ID").count.where($"count" > 1).count()

    log.info("[INFO] DUPLICATES COUNT "+ dup_cnt)

    if(dup_cnt > 0){

      hold_union_all_df.registerTempTable("hold_tbl")
      val rank1_df = hiveContext.sql("select *,row_number() OVER (PARTITION BY ETA_CALC_ID order by HOLD_ID) as rowno from hold_tbl").where($"rowno" === 1)

      return rank1_df.dropDuplicates()
    }
    else{

      return hold_union_all_df.dropDuplicates()

    }
  }

  def dedupe_calc(epic_calc_eta_hist: String, epic_calc_eta_incr: String) : DataFrame = {

    log.info("[INFO] READING INPUT DATA :" + epic_calc_eta_hist)
    //val epic_calc_eta_hist = "/data/helix/raw/epic/epic_calc_eta/full/1.0/2018-04-01/00-00"
    val calc_hist = hiveContext.read.avro(epic_calc_eta_hist).drop($"UUID").drop($"TIMESTAMP")
    //select core_id, count(1) from (select * from epic_calc_eta where HOLD_TIME > 0  and  core_id in
    //(select CORE_ID from epic_calc_eta where CORE_ID <> 0 and EVENT_TYPE <> 'GHOST_FLIGHT' group by CORE_ID having Count(*) > 1 ) ) src
    //group by core_id having count(1) > 1
    val epic_calc_eta_hist_drop_dup = calc_hist.dropDuplicates().where($"core_id" !== 0).where($"event_type" !== "GHOST_FLIGHT").where($"HOLD_TIME" > 0 )

    log.info("[INFO] READING INPUT DATA :" + epic_calc_eta_incr)
    //val epic_calc_eta_incr =  "/data/helix/raw/epic/epic_calc_eta/incremental/1.0/2018-04-29/00-00"
    val calc_incr = hiveContext.read.avro(epic_calc_eta_incr).drop($"UUID").drop($"TIMESTAMP")
    val epic_calc_eta_incr_drop_dup = calc_incr.dropDuplicates().where($"core_id" !== 0).where($"event_type" !== "GHOST_FLIGHT").where($"HOLD_TIME" > 0 )

    log.info("[INFO] UNION OF HISTORY AND INCREMENT DATA")
    val calc_union_all_df = unionByName(epic_calc_eta_hist_drop_dup, epic_calc_eta_incr_drop_dup)

    return calc_union_all_df.dropDuplicates()
  }

  def dedupe_init(epic_t_int_core_hist: String, epic_t_int_core_incr: String) : DataFrame = {

    log.info("[INFO] READING INPUT DATA :" + epic_t_int_core_hist)
    //val epic_t_int_core_hist = "/data/helix/decomposed/epic/epic_t_int_core/full/1.0/2018-05-03/12-00"
    val init_hist = hiveContext.read.avro(epic_t_int_core_hist).drop($"HELIX_UUID").drop($"HELIX_TIMESTAMP")
    val epic_t_int_core_hist_drop_dup = init_hist.dropDuplicates()

    log.info("[INFO] READING INPUT DATA :" + epic_t_int_core_incr)
    //val epic_t_int_core_incr =  "/data/helix/raw/epic/epic_calc_eta/incremental/1.0/2018-04-29/00-00"
    val init_incr = hiveContext.read.avro(epic_t_int_core_incr).drop($"HELIX_UUID").drop($"HELIX_TIMESTAMP")
    val epic_t_int_core_incr_drop_dup = init_incr.dropDuplicates()

    log.info("[INFO] UNION OF HISTORY AND INCREMENT DATA")
    val init_union_all_df = unionByName(epic_t_int_core_hist_drop_dup, epic_t_int_core_incr_drop_dup)

    val init_dup_cnt = init_union_all_df.groupBy($"core_id").count.where($"count" > 1).count()

    log.info("[INFO] DUPLICATES COUNT "+ init_dup_cnt)

    if(init_dup_cnt > 0){

      init_union_all_df.registerTempTable("init_tbl")
      val init_rank_df = hiveContext.sql("select *,row_number() OVER (PARTITION BY core_id order by LU_DATE) as rowno from init_tbl").where($"rowno" === 1)

      return init_rank_df.dropDuplicates()
    }
    else{

      return init_union_all_df.dropDuplicates()

    }
  }

  def writetohdfs(output_df : DataFrame,months : Int,spark_partitions : Int,hdfs_output_path_incr : String, hdfs_output_path_hist : String, compression : String) = {

    hiveContext.setConf("spark.sql.parquet.compression.codec", compression)

    output_df.registerTempTable("temp_tbl")

    val max_flight_date = hiveContext.sql("select max(FLIGHT_DATE_ID) from temp_tbl").first().mkString

    log.info("[INFO] MAX FLIGHT DATE IS "+ max_flight_date +"...")

    val incr_sql = "select * from temp_tbl where to_date(FLIGHT_DATE_ID) >= add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"
    val hist_sql = "select * from temp_tbl where to_date(FLIGHT_DATE_ID) < add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"

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


  def main(args:Array[String]) : Unit = {
    val parser = new CmdLineParser(CliArgs)
    try {
      parser.parseArgument(args:_*)
    } catch {
      case e: CmdLineException =>
        print(s"[ERROR] Parameters Missing or Wrong \n:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }

   // println(CliArgs.epic_t_int_core_hist)
   // println(CliArgs.epic_calc_eta_hist)
   // println(CliArgs.epic_hold_snapshot_hist)
   // println(CliArgs.epic_t_int_core_incr)
   // println(CliArgs.epic_calc_eta_incr)
   // println(CliArgs.epic_hold_snapshot_incr)
   // println(CliArgs.hist_output_path)
   // println(CliArgs.incr_output_path)
   // print(CliArgs.lookback_months)
   // println(CliArgs.compression)
   // println(CliArgs.spark_partitions)

    val dedupe_hold_df = dedupe_hold(CliArgs.epic_hold_snapshot_hist,CliArgs.epic_hold_snapshot_incr)
    val dedupe_calc_df = dedupe_calc(CliArgs.epic_calc_eta_hist,CliArgs.epic_calc_eta_incr)
    val dedupe_init_df = dedupe_init(CliArgs.epic_t_int_core_hist,CliArgs.epic_t_int_core_incr)

    val join_calc_hold =  dedupe_calc_df.join(dedupe_hold_df.select("ETA_CALC_ID","HOLD_AREA"),dedupe_calc_df.col("CALC_ETA_ID") === dedupe_hold_df.col("ETA_CALC_ID"),"left_outer")

    //join_calc_hold.write.mode("overwrite").parquet("/user/ops_eff/epic_inter")
    //dedupe_hold_df.write.mode("overwrite").parquet("/user/ops_eff/epic_hold")
    //dedupe_calc_df.write.mode("overwrite").parquet("/user/ops_eff/epic_calc")
    //dedupe_init_df.write.mode("overwrite").parquet("/user/ops_eff/epic_init")

    val join_init_calchold = dedupe_init_df.join(join_calc_hold.drop($"FLIGHT_NO"),Seq("CORE_ID"),"left_outer").drop($"rowno")

    writetohdfs(join_init_calchold,CliArgs.lookback_months,CliArgs.spark_partitions,CliArgs.incr_output_path,CliArgs.hist_output_path,CliArgs.compression)

  }

}
