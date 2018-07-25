/*
*=====================================================================================================================================
* Created on     :   17/01/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   Dedupe_EPIC_Daily
* Description    :   This is spark application is used to dedup epic flight level (INCR)
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.emirates.helix.DedupeEpicDaily" /home/ops_eff/ek_epic/DedupeEpicDaily-1.0-SNAPSHOT.jar  --epic_dedupe_current 	/data/helix/uat/modelled/emfg/dedupe/epic/current --epic_t_int_core-incr /data/helix/raw/epic/epic_t_int_core/incremental/1.0/2018-05-03/05-00 --epic_calc_eta-incr /data/helix/raw/epic/epic_calc_eta/incremental/1.0/2018-04-29/00-00 --epic_hold_snapshot-incr /data/helix/raw/epic/epic_hold_snapshot/incremental/1.0/2018-04-29/00-00 --incr-output /data/helix/modelled/emfg/dedupe/epic/history --compression snappy --spark-partitions 2

package com.emirates.helix
import org.kohsuke.args4j.{CmdLineException, CmdLineParser}

import com.databricks.spark.avro._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive._



object DedupeEpicDaily {

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


  def dedupe_hold(epic_hold_snapshot_incr: String): DataFrame = {

    log.info("[INFO] READING INPUT DATA :" + epic_hold_snapshot_incr)
    //val epic_hold_snapshot_incr =  "/data/helix/decomposed/epic/epic_hold_snapshot/incremental/1.0/2018-04-29/01-30"
    val hold_incr = hiveContext.read.avro(epic_hold_snapshot_incr).drop($"UUID").drop($"TIMESTAMP")
    val epic_hold_snapshot_incr_drop_dup = hold_incr.dropDuplicates()

    val dup_cnt = epic_hold_snapshot_incr_drop_dup.groupBy($"ETA_CALC_ID").count.where($"count" > 1).count()

    log.info("[INFO] DUPLICATES COUNT "+ dup_cnt)

    if(dup_cnt > 0){

      epic_hold_snapshot_incr_drop_dup.registerTempTable("hold_tbl")
      val rank1_df = hiveContext.sql("select *,row_number() OVER (PARTITION BY ETA_CALC_ID order by HOLD_ID) as rowno from hold_tbl").where($"rowno" === 1)

      return rank1_df.dropDuplicates()
    }
    else{

      return epic_hold_snapshot_incr_drop_dup.dropDuplicates()

    }
  }

  def dedupe_calc(epic_calc_eta_incr: String) : DataFrame = {


    log.info("[INFO] READING INPUT DATA :" + epic_calc_eta_incr)
    //val epic_calc_eta_incr =  "/data/helix/raw/epic/epic_calc_eta/incremental/1.0/2018-04-29/00-00"
    val calc_incr = hiveContext.read.avro(epic_calc_eta_incr).drop($"UUID").drop($"TIMESTAMP")
    val epic_calc_eta_incr_drop_dup = calc_incr.dropDuplicates().where($"core_id" !== 0).where($"event_type" !== "GHOST_FLIGHT").where($"HOLD_TIME" > 0 )


    return epic_calc_eta_incr_drop_dup.dropDuplicates()
  }

  def dedupe_init(epic_t_int_core_incr: String) : DataFrame = {

    log.info("[INFO] READING INPUT DATA :" + epic_t_int_core_incr)
    //val epic_t_int_core_incr =  "/data/helix/raw/epic/epic_calc_eta/incremental/1.0/2018-04-29/00-00"
    val init_incr = hiveContext.read.avro(epic_t_int_core_incr).drop($"HELIX_UUID").drop($"HELIX_TIMESTAMP")
    val epic_t_int_core_incr_drop_dup = init_incr.dropDuplicates()

    val init_dup_cnt = epic_t_int_core_incr_drop_dup.groupBy($"core_id").count.where($"count" > 1).count()

    log.info("[INFO] DUPLICATES COUNT "+ init_dup_cnt)

    if(init_dup_cnt > 0){

      epic_t_int_core_incr_drop_dup.registerTempTable("init_tbl")
      val init_rank_df = hiveContext.sql("select *,row_number() OVER (PARTITION BY core_id order by LU_DATE) as rowno from init_tbl").where($"rowno" === 1)

      return init_rank_df.dropDuplicates()
    }
    else{

      return epic_t_int_core_incr_drop_dup.dropDuplicates()

    }

  }

  def dedupeUnion(df : DataFrame) : DataFrame = {

    val init_dup_cnt = df.groupBy($"core_id").count.where($"count" > 1).count()

    log.info("[INFO] DUPLICATES COUNT "+ init_dup_cnt)

    if(init_dup_cnt > 0){

      df.registerTempTable("init_tbl")
      val init_rank_df = hiveContext.sql("select *,row_number() OVER (PARTITION BY core_id order by LU_DATE) as rowno from init_tbl").where($"rowno" === 1)

      return init_rank_df.dropDuplicates()
    }
    else{

      return df.dropDuplicates()

    }

  }

  def writetohdfs(output_df : DataFrame,spark_partitions : Int,hdfs_output_path_incr : String, compression : String) = {

    hiveContext.setConf("spark.sql.parquet.compression.codec", compression)

    val output_df_last_incr_coalesece = output_df.coalesce(spark_partitions)

    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_incr +"...")
    output_df_last_incr_coalesece.write.mode("overwrite").parquet(hdfs_output_path_incr)
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_incr +" successfully")

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

   // println(CliArgs.epic_dedupe_current)
   // println(CliArgs.epic_t_int_core_incr)
   // println(CliArgs.epic_calc_eta_incr)
   // println(CliArgs.epic_hold_snapshot_incr)
   // println(CliArgs.incr_output_path)
   // println(CliArgs.compression)
   // println(CliArgs.spark_partitions)

    val dedupe_hold_df = dedupe_hold(CliArgs.epic_hold_snapshot_incr)
    val dedupe_calc_df = dedupe_calc(CliArgs.epic_calc_eta_incr)
    val dedupe_init_df = dedupe_init(CliArgs.epic_t_int_core_incr)

    val join_calc_hold =  dedupe_calc_df.join(dedupe_hold_df.select("ETA_CALC_ID","HOLD_AREA"),dedupe_calc_df.col("CALC_ETA_ID") === dedupe_hold_df.col("ETA_CALC_ID"),"left_outer")

    val join_init_calchold = dedupe_init_df.join(join_calc_hold.drop($"FLIGHT_NO"),Seq("CORE_ID"),"left_outer").drop($"rowno")

    val dedupe_current = hiveContext.read.parquet(CliArgs.epic_dedupe_current).drop($"rowno")

    val union_all_init = unionByName(join_init_calchold,dedupe_current).dropDuplicates()

    val final_df =  dedupeUnion(union_all_init).drop($"rowno")

    writetohdfs(final_df,CliArgs.spark_partitions,CliArgs.incr_output_path,CliArgs.compression)

  }

}

