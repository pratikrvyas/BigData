package com.emirates.cartrawlerrawtodecomp
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
import com.emirates.cartrawlerrawtodecomp.utils._
object rawDatatoDecompTransfer extends sparkUtils {
  val conf = sparkConfig("Ancillary Revenue Cartrawler Raw to Decompose data ingestion");
  val sc = new SparkContext(conf);
   val sqlContext = getHiveContext(sc)
  val logger = LogManager.getRootLogger
  def carTrawRawToDecomp(run_date: String, basePathSource: String, basePathTarget: String,carTrawTransType: String) {
    Logger.getLogger("carTrawRawToDecomp").info(carTrawTransType+" carTrawRawToDecomp: Start of Raw to Decompose insertion process");
    //derive parameters
    val carTrawDecompSrcpath = basePathSource.concat(run_date) + "/";
    val run_date_frmt = run_date.replace('-', '_')
    val srcPathAptTran = carTrawDecompSrcpath + carTrawTransType + "_" + run_date_frmt + ".csv"
    val srcPathRead = carTrawDecompSrcpath + run_date
    val tgtPathAptTran = basePathTarget + carTrawTransType + "/incremental/1.0/" + run_date + "/"
    //Load data from source location(i.e. Cartrawler RAW layer)
    val carTrawAptTrans = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(srcPathAptTran);
    Logger.getLogger("carTrawRawToDecomp").info(carTrawTransType+"Start of adding uuid and timestamp columns");
    //create UUID object
    var HELIX_UUID = udf(() => UUID.randomUUID().toString)
    //adding UUID and timestamp for the airport_Transfer dataframe
    val carTrawAptTransCol = carTrawAptTrans.withColumn("HELIX_UUID", HELIX_UUID()).withColumn("HELIX_TIMESTAMP", unix_timestamp(current_timestamp()));
    val carTrawAptTransRes = cartrawDataFrmt(carTrawAptTransCol);
    try {
      //write data into target location
      carTrawAptTransRes.write.mode("overwrite").parquet(tgtPathAptTran);
    } catch {
      case e: Exception =>
        logger.info("carTrawRawToDecomp Method - Error while processing src data of Type F: " + e);
        throw new Exception(carTrawTransType+"carTrawRawToDecomp: Runtime issue while ingesting RAW data")
    }

  }

  def cartrawDataFrmt(dataframe: DataFrame): DataFrame = {
try{
    import sqlContext.implicits._
    //formate dataframe columns
    val dfFrmt = dataframe.select($"Res_Id".cast("string"), $"Order_Id".cast("string"), $"Parent".cast("string"), $"Child".cast("string"), $"Firstname".cast("string"), $"Surname".cast("string"), $"Age".cast("string"), $"BusinessType".cast("string"), $"Residency".cast("string"), $"Res_Date".cast("string"), $"LocationPickUp".cast("string"), $"LocationDropoff".cast("string"), $"Pickup".cast("string"), $"Dropoff".cast("string"), $"Days".cast("string"), $"Rental_Commission".cast("string"), $"Insurance_Commission".cast("string"), $"Rate_Currency".cast("string"), $"Total_Rate".cast("string"), $"Status".cast("string"), $"Status1".cast("string"), $"Email".cast("string"), $"Total_Rate_Display".cast("string"), $"Currency_Display".cast("string"), $"CardType".cast("string"), $"Job_Execution_Date".cast("string"), $"HELIX_UUID".cast("string"), $"HELIX_TIMESTAMP".cast("string"));
    dfFrmt;
  } catch {
      case e: Exception =>
        logger.info("cartrawDataFrmt Method - Error while Formating src data of Type F: " + e);
        throw new Exception("carTrawRawToDecomp: Runtime issue while Formating RAW data")
    }
  }

}