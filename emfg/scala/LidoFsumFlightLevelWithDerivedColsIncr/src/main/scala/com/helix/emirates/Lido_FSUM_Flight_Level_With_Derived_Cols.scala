/*
*=====================================================================================================================================
* Created on     :   07/01/2017
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   Lido_FSUM_Flight_Level_With_Derived_Cols
* Description    :   This is spark application is used to derive derived of lido fsum flight level
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.helix.emirates.Lido_FSUM_Flight_Level_With_Derived_Cols" /home/ops_eff/ek_lido/lido_fsum_flight_level_with_derived_cols-1.0-SNAPSHOT.jar -i /data/helix/modelled/flight_genome/prerequisite/lido_fsum_flight_level/incremental/1.0/2018-01-04/11-00 -o /data/helix/modelled/flight_genome/prerequisite/lido_fsum_flight_level_with_drvd_cols/incremental/1.0/2018-01-04/11-00 -c snappy

package com.helix.emirates

import com.databricks.spark.avro._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

object Lido_FSUM_Flight_Level_With_Derived_Cols {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("arinc_with_cycle_info")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Lido_FSUM_Flight_Level_With_Derived_Cols")


  def derivedColsLogic(input_df : DataFrame) : DataFrame = {

    input_df.registerTempTable("lido_fsum_flight_level_Incremental")
    val drvd_df = sqlContext.sql("SELECT *,(cast(trim(PlaToWei) as int) - cast(trim(PlnZfw) as int)) AS TakeOffFuel_drvd,(cast(trim(TaxOutTimInMin) as int) + cast(trim(TriTimInMin) as int)) AS RampTime_drvd,(cast(trim(TaxOutTimInMin) as int) + cast(trim(TriTimInMin) as int) + cast(trim(TaxInTimInMin) as int)) AS EstimatedTime_drvd,CASE WHEN substring(trim(AveWinComInKts),1,1) = 'M' then regexp_replace(trim(AveWinComInKts),'M','-') WHEN substring(trim(AveWinComInKts),1,1) = 'P' then regexp_replace(trim(AveWinComInKts),'P','+') ELSE trim(AveWinComInKts) END AveWinComInKts_drvd,CASE WHEN length(trim(PerCor100)) = 4 then cast(substring(trim(PerCor100),3,2) as int)/10 ELSE trim(PerCor100) END PerCor1000_drvd,(PlaToWei - TriFue) AS EstimatedLandingWeight_drvd,CASE WHEN trim(RouOptCri) = 'INFLT' then 'Y'WHEN trim(RouOptCri) = 'inflt' then 'Y' ELSE '' END InflightIndicator_drvd FROM lido_fsum_flight_level_Incremental")

    val drvd_df_with_RampFuel_drvd_col = drvd_df.withColumn("RampFuel_drvd",$"TaxFueOut" + $"TakeOffFuel_drvd")


  return drvd_df_with_RampFuel_drvd_col
  }


  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path: String = null, compression: String = null)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("Lido_FSUM_Flight_Level_With_Derived_Cols") {
      head("Lido_FSUM_Flight_Level_With_Derived_Cols")
      opt[String]('i', "hdfs_input_path")
        .required()
        .action((x, config) => config.copy(hdfs_input_path = x))
        .text("Required parameter : Input file path for data")
      opt[String]('o', "hdfs_output_path")
        .required()
        .action((x, config) => config.copy(hdfs_output_path = x))
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
      val input_df = sqlContext.read.avro(config.hdfs_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_input_path + "is successfull")
      //CALLING  DERIVED LOGIC FUNCTION
      log.info("[INFO] DERIVING THE FLIGHT LEVEL COLUMNS (TakeOffFuel_drvd,RampFuel_drvd,RampTime_drvd,EstimatedTime_drvd,AveWinComInKts_drvd,PerCor1000_drvd,EstimatedLandingWeight_drvd,InflightIndicator_drvd) ...")
      val output_df = derivedColsLogic(input_df)
      log.info("[INFO] DERIVING THE FLIGHT LEVEL COLUMNS (TakeOffFuel_drvd,RampFuel_drvd,RampTime_drvd,EstimatedTime_drvd,AveWinComInKts_drvd,PerCor1000_drvd,EstimatedLandingWeight_drvd,InflightIndicator_drvd) is successfull")
      //WRITING DATA TO PREREQUISITE IN AVRO FORMAT WITH SNAPPY COMPRESSION
      log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ config.hdfs_output_path +"...")
      sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
      val output_df_coalesece = output_df.coalesce(2)
      output_df_coalesece.write.avro(config.hdfs_output_path)
      log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ config.hdfs_output_path +" successfully")

    }
  }
  }
