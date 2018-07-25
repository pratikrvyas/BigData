package com.emirates.helix

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLImplicits;
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{ rowNumber, max, broadcast }
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions.row_number;
import scala.util.control.Exception._
import org.apache.spark.sql.{ SaveMode, DataFrame, SQLContext, Row }
import scala.util.{ Try, Success, Failure }
import java.text.SimpleDateFormat
import com.databricks.spark.avro._
import com.emirates.helix.utils._
object DedupMfpData extends sparkUtils {
  val conf = sparkConfig("Dedup OpsEff MFP Decomp Data");
  val sc = new SparkContext(conf);
  val sqlContext = getHiveContext(sc)
  val logger = LogManager.getRootLogger
  // This Method is to process the cartrawler Decomposed data
  def decompdataprocess(base_Input_Path: String, incr_Input_path: String, hist_Output_Path: String, inct_Output_Path: String, incr_Output_Path_Temp: String, loadType: String, months: Integer) {
    import sqlContext.implicits._
    //target path formation
    try {
      //target path formation
      var srcDataPath = "";
      var dfDedupDecomData = Seq((1, 2, 3), (4, 5, 6)).toDF
      if (loadType.toUpperCase == "INITIAL") {
        srcDataPath = base_Input_Path // + "/*/*/";
        //deduping the decomposed data
        dfDedupDecomData = dedupdecompdata(srcDataPath);
        //creating temp table on deduped result
        dfDedupDecomData.registerTempTable("dedupTemp");
        //below is to retrive the max date required to partition the data into current and history directories
        val max_flight_date = sqlContext.sql("select max(to_date(FLIGHT_DATE)) from dedupTemp").first().mkString;
        val incr_sql = "select * from dedupTemp where to_date(FLIGHT_DATE) >= add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"
        val hist_sql = "select * from dedupTemp where to_date(FLIGHT_DATE) < add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"
        val output_df_last_incr = sqlContext.sql(incr_sql)
        val output_df_last_hist = sqlContext.sql(hist_sql)
        //write the resultant data into respective HDFS directories
        output_df_last_incr.write.mode("overwrite").parquet(inct_Output_Path);
        output_df_last_hist.write.mode("overwrite").parquet(hist_Output_Path);
        output_df_last_incr.write.mode("overwrite").parquet(incr_Output_Path_Temp);
      } else {
        //read latest snap data from HDFS target location
        val dfTgtSnap = sqlContext.read.format("parquet").load(inct_Output_Path);
        //read latest input data from decomposed data
        dfDedupDecomData = dedupdecompdata(incr_Input_path);
        //Union on Decomposed sorce and merge target data
        val dfUnion = unionByName(dfDedupDecomData, dfTgtSnap)

        //Drop Duplicates
        val dfDropDup = dfUnion.dropDuplicates;
         val dfDedupRes = dedupdfdata(dfDropDup);
        dfDedupRes.write.mode("overwrite").parquet(incr_Output_Path_Temp);
      }
    } catch {
      case e: Exception =>
        ///logger.info(" decompdataprocess Method - Error while writing data into HDFS: " + e);
        throw new Exception("decompdataprocess: Runtime issue while ingesting Merge data into  location")
    }

  }
  //this is to perform dedup logic on source data
  def dedupdecompdata(Path: String): DataFrame = {
    try {
      import sqlContext.implicits._
      val dfColsList = Seq("FLIGHT_NUMBER","FLIGHT_DATE","FROM_SECTOR","TO_SECTOR","CAP_STAFF_NUMBER","CAP_NAME","EXTRA_REASON_CODE","EXTRA_REASON_COMMENT","PDF_FILE_NAME","USER_GENERATED","PUBLISHED_DATE","FILE_PATH","NUMBER_OF_PAGES","SOURCE_FOLDER","FOLDER_INDEX","VALID_MFP","SEQID","MODIFIED_ON","HELIX_UUID","HELIX_TIMESTAMP");
      val df = sqlContext.read.format("com.databricks.spark.avro").load(Path);
      val w = Window.partitionBy($"FLIGHT_NUMBER",$"FLIGHT_DATE",$"FROM_SECTOR",$"TO_SECTOR").orderBy($"MODIFIED_ON".desc);
      val dfDropDup = df.dropDuplicates;
      val dfRn = dfDropDup.withColumn("rn", row_number.over(w));
      val dfFilReq = dfRn.filter($"rn" === 1);
      val dfReq = select(dfFilReq, sqlContext, dfColsList)
      dfReq;
    } catch {
      case e: Exception =>
        //logger.info(" dedupdecompdata Method - Error while writing data into HDFS: " + e);
        throw new Exception("dedupdecompdata: Runtime issue while executing Deduping merging data")
    }
  }
 //this is to perform union on two data frames
  def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
    val columns = a.columns.toSet.intersect(b.columns.toSet).map(col).toSeq
    println(columns)
    a.select(columns: _*).unionAll(b.select(columns: _*))
  }
  //to perform dedup logic on the input DF
  def dedupdfdata(df: DataFrame): DataFrame = {
    try {
      import sqlContext.implicits._
      val dfColsList = Seq("FLIGHT_NUMBER","FLIGHT_DATE","FROM_SECTOR","TO_SECTOR","CAP_STAFF_NUMBER","CAP_NAME","EXTRA_REASON_CODE","EXTRA_REASON_COMMENT","PDF_FILE_NAME","USER_GENERATED","PUBLISHED_DATE","FILE_PATH","NUMBER_OF_PAGES","SOURCE_FOLDER","FOLDER_INDEX","VALID_MFP","SEQID","MODIFIED_ON","HELIX_UUID","HELIX_TIMESTAMP");
      //val df = sqlContext.read.format("com.databricks.spark.avro").load(Path);
      val w = Window.partitionBy($"FLIGHT_NUMBER",$"FLIGHT_DATE",$"FROM_SECTOR",$"TO_SECTOR").orderBy($"MODIFIED_ON".desc);
      val dfDropDup = df.dropDuplicates;
      val dfRn = dfDropDup.withColumn("rn", row_number.over(w));
      val dfFilReq = dfRn.filter($"rn" === 1);
      val dfReq = select(dfFilReq, sqlContext, dfColsList)
      dfReq;
    } catch {
      case e: Exception =>
        //logger.info(" dedupdecompdata Method - Error while writing data into HDFS: " + e);
        throw new Exception("dedupdecompdata: Runtime issue while executing Deduping merging data")
    }
  }

}