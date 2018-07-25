
/*----------------------------------------------------------------------------
 * Created on  : 10/May/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UltramainDedupProcessor.scala
 * Description : Dedupe Ultramain processor.
 * ---------------------------------------------------------------------------*/

package com.emirates.helix


import com.emirates.helix.util._
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.TimestampType


object UltramainDedupProcessor {



  /**
    * UltramainDedupProcessor instance creation
    * @param args user parameter instance
    * @return Processor instance
    */
  def apply(args:  UltramainDedupArguments.Args): UltramainDedupProcessor = {
    var processor = new UltramainDedupProcessor()
    processor.args = args
    processor
  }


  class UltramainDedupProcessor extends SparkUtils {
    var args: UltramainDedupArguments.Args = _




    /** Method to process incremental data
      *
      * output union dataset
      */
    def process_incremental_UltramainSectorUvalue(implicit spark: SparkSession): DataFrame = {


      // Load incremental sector and uvalue
      val df_incr_sector= spark.read.avro(args.ultramain_src_incr_sector_location).drop("HELIX_UUID").drop("HELIX_TIMESTAMP").drop("CHANGED_BY").drop("PROGRESS_RECID").drop("VERSION")
      val df_incr_uvalue= spark.read.avro(args.ultramain_src_incr_uvalue_location).drop("HELIX_UUID").drop("HELIX_TIMESTAMP").drop("CHANGED_BY").drop("PROGRESS_RECID").drop("VERSION")


      // join incremental sector and uvalue
      val joinType="inner"
      val df_incr_sector_uvalue=df_incr_uvalue.join(df_incr_sector,(df_incr_uvalue.col("REL_OBJ_ID") === df_incr_sector.col("OBJ_ID")),joinType)

      // Load current Ultramain dedupe
      val df_current_ultrmain = spark.read.parquet(args.ultramain_src_dedupe_current_location)

      // union current with dedupe
      val df_sector_uvalue=df_current_ultrmain.union(df_incr_sector_uvalue)

      return df_sector_uvalue

    }

    /** Method to process History data
      *
      * output union dataset
      */
    def process_history_UltramainSectorUvalue(implicit spark: SparkSession): DataFrame = {


     // Load history/full
      val df_hist_sector= spark.read.avro(args.ultramain_src_history_sector_location).drop("HELIX_UUID").drop("HELIX_TIMESTAMP").drop("CHANGED_BY").drop("PROGRESS_RECID").drop("VERSION")
      val df_hist_uvalue= spark.read.avro(args.ultramain_src_history_uvalue_location).drop("HELIX_UUID").drop("HELIX_TIMESTAMP").drop("CHANGED_BY").drop("PROGRESS_RECID").drop("VERSION")

      // Load incremental
      val df_incr_sector= spark.read.avro(args.ultramain_src_incr_sector_location).drop("HELIX_UUID").drop("HELIX_TIMESTAMP").drop("CHANGED_BY").drop("PROGRESS_RECID").drop("VERSION")
      val df_incr_uvalue= spark.read.avro(args.ultramain_src_incr_uvalue_location).drop("HELIX_UUID").drop("HELIX_TIMESTAMP").drop("CHANGED_BY").drop("PROGRESS_RECID").drop("VERSION")

      // union sector : history + incremental
      val df_sector=df_hist_sector.union(df_incr_sector)

      // union uvalue :  history + incremental
      val df_uvalue=df_hist_uvalue.union(df_incr_uvalue)

      // join sector and uvalue
      val joinType="inner"
      val df_sector_uvalue=df_uvalue.join(df_sector,(df_uvalue.col("REL_OBJ_ID") === df_sector.col("OBJ_ID")),joinType)


      return df_sector_uvalue

    }


    /** Method to process ultramain data
      *
      * output ultramain dataset
      */
    def processUltramain()(implicit spark: SparkSession): DataFrame = {


      if (args.data_load_mode.toUpperCase().equals("HISTORY"))
      {
         return process_history_UltramainSectorUvalue
      }
      else
      {
        return process_incremental_UltramainSectorUvalue
      }

    }

    /** Method to dedup ultramain sector and uvalue data
      *
      * output dedup dataset
      */
    def deDupUltramain(sector_uvalue_df: DataFrame)(implicit spark: SparkSession): DataFrame = {

      import org.apache.spark.sql.types.LongType
      import org.apache.spark.sql.types.DoubleType


      val deduped_df = sector_uvalue_df.dropDuplicates()


      val ultramainGrp = Window.partitionBy("REL_OBJ_ID","U__FIELD_ID")
      val dedup_grp_df = deduped_df.withColumn("MAX_ADD_DATE_TIME", max(col("ADD_DATE_TIME").cast(DoubleType)).over(ultramainGrp))
      val dedup_max_df=  dedup_grp_df.filter(dedup_grp_df("MAX_ADD_DATE_TIME").cast(DoubleType) === (dedup_grp_df("ADD_DATE_TIME").cast(DoubleType))).drop("MAX_ADD_DATE_TIME")



      /// dedupe on CHG_DATE_TIME
      val dedup_changed_time_grp_df = dedup_max_df.withColumn("MAX_CHG_DATE_TIME", max(col("CHG_DATE_TIME").cast(DoubleType)).over(ultramainGrp))
      val dedup_max_changed_time=  dedup_changed_time_grp_df.filter(dedup_changed_time_grp_df("MAX_CHG_DATE_TIME").cast(DoubleType) === (dedup_changed_time_grp_df("CHANGED_TIME").cast(DoubleType))).drop("MAX_CHG_DATE_TIME")


      return dedup_max_changed_time

    }




    /** Method to write processed data to HDFS
      *
      * @param out_df Dataframe to be written to HDFS
      */
    def writeUltramainDeDupe(out_df: DataFrame, lookback_months : Int, coalesce_value : Int) (implicit spark: SparkSession): Unit = {


      if (args.data_load_mode.toUpperCase().equals("HISTORY")) {
        out_df.createOrReplaceTempView("ultramain_temptab")

      // uncomment after testing
       val max_SCH_DEP_DATE = spark.sql("select max(SCH_DEP_DATE) from ultramain_temptab").first().mkString

        // delete after testing
        //val max_SCH_DEP_DATE ="2017-11-25 00:00:00.0"

        val incr_sql = "select * from ultramain_temptab where to_date(SCH_DEP_DATE) >= " +
          "add_months(to_date('" + max_SCH_DEP_DATE + "')," + -1 * lookback_months + ")"
        val hist_sql = "select * from ultramain_temptab where to_date(SCH_DEP_DATE) < " +
          "add_months(to_date('" + max_SCH_DEP_DATE + "')," + -1 * lookback_months + ")"

        val output_df_last_incr = spark.sql(incr_sql).coalesce(coalesce_value)
        val output_df_last_hist = spark.sql(hist_sql).coalesce(coalesce_value)

        output_df_last_hist.write.format("parquet").save(args.ultramain_tgt_dedupe_history_location)
        output_df_last_incr.write.format("parquet").save(args.ultramain_tgt_dedupe_current_location)
      }
      else
      {
        out_df.write.format("parquet").save(args.ultramain_tgt_dedupe_current_location)
      }
    }
  }
}
