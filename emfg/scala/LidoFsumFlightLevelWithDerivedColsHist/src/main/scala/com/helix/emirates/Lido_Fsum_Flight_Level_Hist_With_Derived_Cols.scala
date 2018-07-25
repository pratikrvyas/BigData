/*
*=====================================================================================================================================
* Created on     :   15/01/2017
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   Lido_FSUM_Flight_Level_Hist_With_Derived_Cols
* Description    :   This is spark application is used to derive derived of lido fsum flight level for History data
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.helix.emirates.Lido_Fsum_Flight_Level_Hist_With_Derived_Cols" /home/ops_eff/ek_lido/Lido_Fsum_Flight_Level_Hist_With_Derived_Cols-1.0-SNAPSHOT.jar -i /data/helix/modelled/flight_genome/sync/lido_fsum_flight_level/02-01-2018/00-00 -o /data/helix/modelled/flight_genome/sync/lido_fsum_flight_level_with_drvd_cols/02-01-2018/00-00 -c snappy -p 2

package com.helix.emirates

import com.databricks.spark.avro._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}




object Lido_Fsum_Flight_Level_Hist_With_Derived_Cols {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("arinc_with_cycle_info")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Lido_Fsum_Flight_Level_Hist_With_Derived_Cols")

  sqlContext.udf.register("HHMMToMins", (hhmm : String) =>
    (

      if(hhmm != null && !hhmm.trim().isEmpty() && hhmm.trim().matches("""(\d\d\d\d)"""))
        ((hhmm.trim().substring(0,2).toInt * 60) + (hhmm.trim().substring(2,4).toInt)).toString
      else
        null.asInstanceOf[String]
      )
  )

  //This method is used to convert HHMMToMins history time based cols
  def hhMMToMinsLogic(input_df : DataFrame) : DataFrame = {

    input_df.registerTempTable("input_tbl")

    val to_mins_df = sqlContext.sql("select DepRun,ArrRun,CosIndEcoCru,FirstAltIat,SecondAltIat,ThirdAltIat,FourthAltIat,FliNum,DepAirIat,DatOfOriUtc,DesAirIat,RegOrAirFinNum,IatAirTyp,NoOfAlt,OfpNum,TaxFueOut,TaxFueIn,TriFue,BloFue,PlaHolFue,AddFue1,ReaForAdd1,AddFue2,ReaForAdd2,AddFue3,ReaForAdd3,AddFue4,ReaForAdd4,ConFue,TanCap,MaxTaxWei,FueOveDes,EcoFue,SavInUsd,PerCor100,ConCovIn,ConPer100,TriFueOneFlBel,Gai,PicNam,DisNam,DisRem,NoOfWayOfMaiRouDepDes,NoOfWayOfRouToDepAltDepDepAlt,NoOfWayOfRouToFirstDesAltDesFirstAlt,NoOfWayOfRouToSecondDesAltDesSecondAlt,NoOfWayOfRouToThirdDesAltDesThirdAlt,NoOfWayOfRouToFourthDesAltDesFourthAlt,NoOfWayOfRouToRecAirDepRecAir,NoOfWayOfRouToRecAirRecAirRecAlt,TotNumOfWayInMes,DepAirIca,DesAirIca,FliDupNo,MaxLanWei,TriDisInNmAir,TriDisInNmGro,AveWinComInKts,PlaToWei,MaxToWei,PlnZfw,MaxZfw,Sid,SidExiPoi,SidDisInNm,Sta,StaEntPoi,StaDisInNm,StdUtcOff,EtaUtcStaUtcInCasOfIniMes,TakAltIat,TakAltIca,FirstAltIca,SecondAltIca,ThirdAltIca,FourthAltIca,GreCirDisInNm,RouOptCri,AirSpePro,EnrFueAltEraIcaFor,NoEnrAltPer100,TriFueCorZfwPlu100Kg,TriFueCorZfwMin100Kg,AltAtTopOfCli100FeeOr10tMet,AltAtTopOfdec100FeeOr10tMet,FirstAltFliDisInNm,FirstAltFue,SecondAltFliDisInNm,SecondAltFue,ThirdAltFliDisInNm,ThirdAltFue,FourthAltFliDisInNm,FourthAltFue,helix_uuid,helix_timestamp,tibco_message_time,AirRadCalSig,AtcCal,DatOfMes,EtoAddFue,EtoBorTimInMin,EtoPet,EtoRulTimInMin,InfTyp,MosCriPoiLat,MosCriPoiLon,NumOfAdeAirOnlThoWhiHavBeeConByTheCal,NumOfEtoAre,NumOfPetIncEntAndExiPoi,NumOfSuiAir,OneEngOutTRUAirSpe1xTasInKno,OpeSuf,AirIcaCod,RelInd,SceNum,RclNum,EtoInd,UniOfMea,AirDes,AirOpeIat,PreRouNam,HHMMToMins(TaxOutTimInMin) as TaxOutTimInMin,HHMMToMins(TaxInTimInMin) as TaxInTimInMin,HHMMToMins(TriTimInMin) as TriTimInMin,HHMMToMins(PlaBloTimInMin) as PlaBloTimInMin,HHMMToMins(EstTotFliTimTimOfPlnInMin) as EstTotFliTimTimOfPlnInMin,HHMMToMins(PlaHolTimInMin) as PlaHolTimInMin,HHMMToMins(ExtTim1TimOfAddInMin) as ExtTim1TimOfAddInMin,HHMMToMins(ExtTim2TimOfAddInMin) as ExtTim2TimOfAddInMin,HHMMToMins(ExtTim3TimOfAddInMin) as ExtTim3TimOfAddInMin,HHMMToMins(ExtTim4TimOfAddInMin) as ExtTim4TimOfAddInMin,HHMMToMins(ConTimTimForConFueInMin) as ConTimTimForConFueInMin,HHMMToMins(FirstAltFliTimInMin) as FirstAltFliTimInMin,HHMMToMins(SecondAltFliTimInMin) as SecondAltFliTimInMin,HHMMToMins(ThirdAltFliTimInMin) as ThirdAltFliTimInMin,HHMMToMins(FourthAltFliTimInMin) as FourthAltFliTimInMin from input_tbl")

    return to_mins_df
  }

  //This method is used derive lido derived cols
  def derivedColsLogic(mins_df : DataFrame) : DataFrame = {

    mins_df.registerTempTable("lido_fsum_flight_level_Incremental")

    val drvd_df = sqlContext.sql("SELECT *,(cast(trim(PlaToWei) as int) - cast(trim(PlnZfw) as int)) AS TakeOffFuel_drvd,(cast(trim(TaxOutTimInMin) as int) + cast(trim(TriTimInMin) as int)) AS RampTime_drvd,(cast(trim(TaxOutTimInMin) as int) + cast(trim(TriTimInMin) as int) + cast(trim(TaxInTimInMin) as int)) AS EstimatedTime_drvd,CASE WHEN substring(trim(AveWinComInKts),1,1) = 'M' then regexp_replace(trim(AveWinComInKts),'M','-') WHEN substring(trim(AveWinComInKts),1,1) = 'P' then regexp_replace(trim(AveWinComInKts),'P','+') ELSE trim(AveWinComInKts) END AveWinComInKts_drvd,CASE WHEN length(trim(PerCor100)) = 4 then cast(substring(trim(PerCor100),3,2) as int)/10 ELSE trim(PerCor100) END PerCor1000_drvd,(PlaToWei - TriFue) AS EstimatedLandingWeight_drvd,CASE WHEN trim(RouOptCri) = 'INFLT' then 'Y'WHEN trim(RouOptCri) = 'inflt' then 'Y' ELSE '' END InflightIndicator_drvd FROM lido_fsum_flight_level_Incremental")

    val drvd_df_with_RampFuel_drvd_col = drvd_df.withColumn("RampFuel_drvd",$"TaxFueOut" + $"TakeOffFuel_drvd")

    return drvd_df_with_RampFuel_drvd_col
  }


  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path: String = null, compression: String = null, spark_partitions : Int = 0)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("Lido_FSUM_Flight_Level_Hist_With_Derived_Cols") {
      head("Lido_FSUM_Flight_Level_Hist_With_Derived_Cols")
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
      val input_df = sqlContext.read.avro(config.hdfs_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_input_path + "is successfull")
      //CALLING HHMMTOMins Function
      log.info("[INFO] CONVERTING INPUT TIME COLUMNS(HHMM) TO MINUTES ...")
      val min_df = hhMMToMinsLogic(input_df)
      log.info("[INFO] CONVERTING INPUT TIME COLUMNS(HHMM) TO MINUTES IS SUCCESSFULL")
      //CALLING  DERIVED LOGIC FUNCTION
      log.info("[INFO] DERIVING THE FLIGHT LEVEL COLUMNS (TakeOffFuel_drvd,RampFuel_drvd,RampTime_drvd,EstimatedTime_drvd,AveWinComInKts_drvd,PerCor1000_drvd,EstimatedLandingWeight_drvd,InflightIndicator_drvd) ...")
      val output_df = derivedColsLogic(min_df)
      log.info("[INFO] DERIVING THE FLIGHT LEVEL COLUMNS (TakeOffFuel_drvd,RampFuel_drvd,RampTime_drvd,EstimatedTime_drvd,AveWinComInKts_drvd,PerCor1000_drvd,EstimatedLandingWeight_drvd,InflightIndicator_drvd) is successfull")
      //WRITING DATA TO PREREQUISITE IN AVRO FORMAT WITH SNAPPY COMPRESSION
      log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ config.hdfs_output_path +"...")
      sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
      val output_df_coalesece = output_df.coalesce(config.spark_partitions)
      output_df_coalesece.write.avro(config.hdfs_output_path)
      log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ config.hdfs_output_path +" successfully")

    }
  }

}
