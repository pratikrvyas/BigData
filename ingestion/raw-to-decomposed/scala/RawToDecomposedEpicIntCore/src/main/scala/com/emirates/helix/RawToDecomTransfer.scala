package com.emirates.helix
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLImplicits;
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import scala.util.control.Exception._
import org.apache.spark.sql.{ SaveMode, DataFrame, SQLContext, Row }
import scala.util.{ Try, Success, Failure }
import java.text.SimpleDateFormat
import java.util.UUID;
import com.emirates.helix.utils._
object RawToDecomTransfer extends sparkUtils {
  val conf = sparkConfig("EPIC Int Core table Raw to Decompose data ingestion");
  val sc = new SparkContext(conf);
   val sqlContext = getHiveContext(sc)
  val logger = LogManager.getRootLogger
  def carTrawRawToDecomp( basePathSourceInit: String, basePathSourceIncr: String, basePathTargetInit: String, basePathTargetIncr: String, loadType: String) {
    Logger.getLogger("EPIC Int Core").info(" Start of Raw to Decomposed EPIC Int Core table: Start of Raw to Decompose insertion process");
    var srcDataPath="";
    var targetSavePath ="";
    if (loadType.toUpperCase == "INITIAL")
    {
      srcDataPath = basePathSourceInit;
      targetSavePath=basePathTargetInit;
    }
    else {
      srcDataPath = basePathSourceIncr;
      targetSavePath=basePathTargetIncr;
    }
    val srcRawDataRead = sqlContext.read.format("com.databricks.spark.avro").load(srcDataPath)
    Logger.getLogger("EpicIntCore-RawToDecomTransfer").info("Start of adding uuid and timestamp columns");
    //create UUID object
    var HELIX_UUID = udf(() => UUID.randomUUID().toString)
    //Drop absolute duplicates
    val srcRawDropDup = srcRawDataRead.dropDuplicates;
    //adding UUID and timestamp for the airport_Transfer dataframe
    val srcDataAddCols = srcRawDropDup.withColumn("HELIX_UUID", HELIX_UUID()).withColumn("HELIX_TIMESTAMP", unix_timestamp(current_timestamp()));
    val srcDataCastRes = cartrawDataFrmt(srcDataAddCols);
    try {
      //write data into target location
      //srcDataCastRes.write.mode("overwrite").parquet(targetSavePath);
      srcDataCastRes.write.mode("overwrite").format("com.databricks.spark.avro").save(targetSavePath);
      
    } catch {
      case e: Exception =>
        logger.info("carTrawRawToDecomp Method - Error while processing src data of Type F: " + e);
        throw new Exception("EpicIntCore - RawToDecomp: Runtime issue while ingesting RAW data")
    }

  }

  def cartrawDataFrmt(dataframe: DataFrame): DataFrame = {
try{
    import sqlContext.implicits._
    //formate dataframe columns
    val dfFrmt = dataframe.select($"CORE_ID".cast("string"), $"FLIGHT_NO_ID".cast("string"), $"FLIGHT_SUFFIX_ID".cast("string"), $"CXCD_ID".cast("string"), $"DEPNUM_ID".cast("string"), $"FLIGHT_DATE_ID".cast("string"), $"DEPSTN_ID".cast("string"), $"ARRSTN_ID".cast("string"), $"LEGNUM_ID".cast("string"), $"FLIGHT_NO".cast("string"), $"FLIGHT_NO_C".cast("string"), $"ACSUB_TYPE".cast("string"), $"TAIL_NO".cast("string"), $"SERVICE_TYPE".cast("string"), $"FLIGHT_STATUS".cast("string"), $"DEP_STN".cast("string"), $"ARR_STN".cast("string"), $"STA".cast("string"), $"STA_C".cast("string"), $"STD".cast("string"), $"STD_C".cast("string"), $"DOR_FLAG".cast("string"), $"DOR_REMARK".cast("string"), $"FLIGHT_DATE".cast("string"), $"FLIGHT_TIME".cast("string"), $"DEP_NUM".cast("string"), $"LEG_NUM".cast("string"), $"FLIGHT_SUFFIX".cast("string"), $"CXCD".cast("string"), $"ORG_BLK_TIME".cast("string"), $"ACT_BLK_TIME".cast("string"), $"EST_AIRBORNE".cast("string"), $"EST_AIRBORNE_C".cast("string"), $"ACT_AIRBORNE".cast("string"), $"ACT_AIRBORNE_C".cast("string"), $"EST_OFFBLOCK".cast("string"), $"EST_OFFBLOCK_C".cast("string"), $"ACT_OFFBLOCK".cast("string"), $"ACT_OFFBLOCK_C".cast("string"), $"EST_ONBLOCK".cast("string"), $"EST_ONBLOCK_C".cast("string"), $"ACT_ONBLOCK".cast("string"), $"ACT_ONBLOCK_C".cast("string"), $"EST_LANDED".cast("string"), $"EST_LANDED_C".cast("string"), $"ACT_LANDED".cast("string"), $"ACT_LANDED_C".cast("string"), $"RTG".cast("string"), $"TAXI_IN".cast("string"), $"TAXI_OUT".cast("string"), $"CABIN_DOOR_CLOSURE".cast("string"), $"CABIN_DOOR_CLOSURE_C".cast("string"), $"CARGO_DOOR_CLOSURE".cast("string"), $"CARGO_DOOR_CLOSURE_C".cast("string"), $"CREATED_DATE".cast("string"), $"LU_DATE".cast("string"), $"LASTDOORCL".cast("string"), $"LASTDOORCL_C".cast("string"), $"HELIX_UUID".cast("string"), $"HELIX_TIMESTAMP".cast("string"));
    dfFrmt;
  } catch {
      case e: Exception =>
        //logger.info("cartrawDataFrmt Method - Error while Formating src data of Type F: " + e);
        throw new Exception("EpicIntCore: Runtime issue while Formating RAW data")
    }
  }

}