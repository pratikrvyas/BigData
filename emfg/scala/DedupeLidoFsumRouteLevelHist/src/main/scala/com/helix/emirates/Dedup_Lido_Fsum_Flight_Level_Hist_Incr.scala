/*
*=====================================================================================================================================
* Created on     :   17/01/2018
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=>OpsEfficnecy
* Filename       :   Dedup_Lido_Fsum_Flight_Level_Hist_Incr
* Description    :   This is spark application is used to dedup lido fsum flight level (HIST+INCR)
* ======================================================================================================================================
*/

//spark-submit --master yarn --queue ingest --class "com.helix.emirates.Dedup_Lido_Fsum_Flight_Level_Hist_Incr" /home/ops_eff/ek_lido/Dedup_Lido_Fsum_Flight_Level-1.0-SNAPSHOT.jar -h /data/helix/modelled/flight_genome/sync/lido_fsum_flight_level_with_drvd_cols/*/* -i /data/helix/modelled/flight_genome/prerequisite/lido_fsum_flight_level_with_drvd_cols/incremental/1.0/*/* -o /data/helix/modelled/flight_genome/dedup/lido/lido_fsum_flight_level_hist_incr/incremental/17-01-2018/00-00 -v /data/helix/modelled/flight_genome/dedup/lido/lido_fsum_flight_level_hist_incr/full/17-01-2018/00-00 -r 15 -c snappy -p 2

package com.helix.emirates

import com.databricks.spark.avro._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import java.text.SimpleDateFormat
import org.apache.spark.sql.hive._
import org.apache.spark.sql.Column

object Dedup_Lido_Fsum_Flight_Level_Hist_Incr {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("Dedup_Lido_Fsum_Flight_Level_Hist_Incr")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val hiveContext = new HiveContext(sc)
  sc.setLogLevel("INFO")
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Dedup_Lido_Fsum_Flight_Level_Hist_Incr")

  def dedup(hist_input_df : DataFrame , incr_input_df : DataFrame) : DataFrame = {

    //val ip = sqlContext.read.parquet("/data/helix/uat/modelled/emfg/dedupe/lido/lido_fsum_flight_level/current")
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
    val drop_dup_df = union_all_df.cache().repartition(4000).dropDuplicates

    //ELIMINATE DUPLICATES AT FLIGHT LEVEL
    drop_dup_df.registerTempTable("drop_dup_tbl")

    val rank_df = hiveContext.sql("select *,DENSE_RANK() OVER (PARTITION BY cast(trim(FliDupNo) as int),trim(FliNum),trim(DatOfOriUtc),trim(DepAirIat),trim(DesAirIat),trim(RegOrAirFinNum),trim(DepRun),trim(ArrRun),trim(CosIndEcoCru),trim(FirstAltIat),trim(FirstAltIca),trim(SecondAltIat),trim(SecondAltIca),trim(ThirdAltIat),trim(ThirdAltIca),trim(FourthAltIat),trim(FourthAltIca),trim(tibco_message_time) ORDER BY trim(tibco_message_time),trim(helix_uuid) DESC) as rank from drop_dup_tbl")

    rank_df.registerTempTable("rank_tbl")

    val latest_df = hiveContext.sql("select * from rank_tbl where rank = 1")

    //FORMATTING FLIGHT DATE TIME - YYYY-MM-DD HH:MM:SS
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
        else
          flinum.trim().concat("-bad")
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
          stat.trim().concat("-bad")
        )
    )

    val depstat_df = tailnum_df.withColumn("DepAirIat",stationformatter($"DepAirIat"))
    val arrstat_df = depstat_df.withColumn("DesAirIat",stationformatter($"DesAirIat"))

    //FILTERING BAD RECORDS IF ANY
    arrstat_df.registerTempTable("arrstat_tbl")
    val bad_df = hiveContext.sql("select * from arrstat_tbl where substring(trim(FliNum),length(FliNum)-4) = '-bad' or substring(trim(DepAirIat),length(DepAirIat)-4) = '-bad' or substring(trim(DatOfOriUtc),length(DatOfOriUtc)-4) = '-bad' or substring(trim(DesAirIat),length(DesAirIat)-4) = '-bad'")

    //GETTING DEDUP DATA
    val dedup_df = hiveContext.sql("select * from arrstat_tbl where substring(trim(FliNum),length(FliNum)-4) != '-bad' or substring(trim(DepAirIat),length(DepAirIat)-4) != '-bad' or substring(trim(DatOfOriUtc),length(DatOfOriUtc)-4) != '-bad' or substring(trim(DesAirIat),length(DesAirIat)-4) != '-bad'")

    val drop_dup_dedup_df = dedup_df.dropDuplicates()

    //UNIT TEST
    //union_all_df.registerTempTable("union_all_tbl")
    //val distinct_df = hiveContext.sql("select distinct FliNum,DepAirIat,DatOfOriUtc,DesAirIat,RegOrAirFinNum,FliDupNo from  union_all_tbl")

    //val trg_cnt = dedup_df.count
    //val src_cnt = distinct_df.count

    //val msg = if(trg_cnt == src_cnt) "Unit Test Passed Counts Matched Source count :"+ src_cnt +" Target Count "+ trg_cnt else "Unit Test Failed Counts UnMatched Source count :"+ src_cnt +" Target Count "+ trg_cnt
    //log.info("[INFO] " + msg)

    //ADDED TWO MORE COLS AS PER THE NEW REQUIREMENT FOR FLIGHT DETAILS SATELLITE
    import org.apache.spark.sql.functions._
    val final_df_cos = drop_dup_dedup_df.withColumn("CosIndEcoCru",when($"CosIndEcoCru".isNotNull,trim($"CosIndEcoCru")).otherwise(lit(null.asInstanceOf[String]))).withColumn("touch_down_fuel_drvd",replaceNullOrEmptyWithZero(trim($"PlaHolFue")) + replaceNullOrEmptyWithZero(trim($"FirstAltFue"))).withColumn("additional_fuel_drvd",replaceNullOrEmptyWithZero(trim($"AddFue1"))+replaceNullOrEmptyWithZero(trim($"AddFue2"))+replaceNullOrEmptyWithZero(trim($"AddFue3"))+replaceNullOrEmptyWithZero(trim($"AddFue4"))).withColumn("touch_down_fuel_drvd",$"touch_down_fuel_drvd".cast("int")).withColumn("additional_fuel_drvd",$"additional_fuel_drvd".cast("int"))

    //val final_trim_df = final_df_cos.select(trim($"EtoPet").as("EtoPet"),trim($"ThirdAltIca").as("ThirdAltIca"),trim($"RelInd").as("RelInd"),trim($"ReaForAdd3").as("ReaForAdd3"),trim($"SidDisInNm").as("SidDisInNm"),trim($"NoOfWayOfMaiRouDepDes").as("NoOfWayOfMaiRouDepDes"),trim($"PlaHolTimInMin").as("PlaHolTimInMin"),$"EstimatedLandingWeight_drvd".as("EstimatedLandingWeight_drvd"),trim($"FourthAltIca").as("FourthAltIca"),trim($"AveWinComInKts").as("AveWinComInKts"),$"TakeOffFuel_drvd".as("TakeOffFuel_drvd"),trim($"FueOveDes").as("FueOveDes"),trim($"NumOfEtoAre").as("NumOfEtoAre"),trim($"DatOfOriUtc").as("DatOfOriUtc"),trim($"SecondAltIat").as("SecondAltIat"),trim($"EtaUtcStaUtcInCasOfIniMes").as("EtaUtcStaUtcInCasOfIniMes"),trim($"EtoRulTimInMin").as("EtoRulTimInMin"),trim($"DatOfMes").as("DatOfMes"),$"RampTime_drvd".as("RampTime_drvd"),trim($"MaxLanWei").as("MaxLanWei"),trim($"TriFue").as("TriFue"),trim($"MaxTaxWei").as("MaxTaxWei"),trim($"PerCor100").as("PerCor100"),trim($"Sid").as("Sid"),trim($"ConCovIn").as("ConCovIn"),trim($"ConFue").as("ConFue"),trim($"CosIndEcoCru").as("CosIndEcoCru"),trim($"TanCap").as("TanCap"),trim($"RegOrAirFinNum").as("RegOrAirFinNum"),trim($"FourthAltFue").as("FourthAltFue"),trim($"SceNum").as("SceNum"),trim($"PlaToWei").as("PlaToWei"),trim($"NoOfAlt").as("NoOfAlt"),trim($"OneEngOutTRUAirSpe1xTasInKno").as("OneEngOutTRUAirSpe1xTasInKno"),trim($"AirRadCalSig").as("AirRadCalSig"),trim($"FourthAltFliTimInMin").as("FourthAltFliTimInMin"),trim($"EtoBorTimInMin").as("EtoBorTimInMin"),trim($"DesAirIat").as("DesAirIat"),trim($"AirSpePro").as("AirSpePro"),trim($"StaEntPoi").as("StaEntPoi"),trim($"PicNam").as("PicNam"),trim($"ExtTim3TimOfAddInMin").as("ExtTim3TimOfAddInMin"),trim($"ReaForAdd1").as("ReaForAdd1"),trim($"PreRouNam").as("PreRouNam"),trim($"DisRem").as("DisRem"),trim($"ThirdAltIat").as("ThirdAltIat"),trim($"helix_timestamp").as("helix_timestamp"),trim($"AddFue3").as("AddFue3"),trim($"MaxToWei").as("MaxToWei"),trim($"TotNumOfWayInMes").as("TotNumOfWayInMes"),trim($"DepAirIca").as("DepAirIca"),trim($"PlaHolFue").as("PlaHolFue"),trim($"AveWinComInKts_drvd").as("AveWinComInKts_drvd"),trim($"Gai").as("Gai"),trim($"ArrRun").as("ArrRun"),trim($"OfpNum").as("OfpNum"),trim($"InflightIndicator_drvd").as("InflightIndicator_drvd"),trim($"AddFue2").as("AddFue2"),trim($"NoOfWayOfRouToDepAltDepDepAlt").as("NoOfWayOfRouToDepAltDepDepAlt"),trim($"ExtTim1TimOfAddInMin").as("ExtTim1TimOfAddInMin"),trim($"NoOfWayOfRouToRecAirRecAirRecAlt").as("NoOfWayOfRouToRecAirRecAirRecAlt"),trim($"MaxZfw").as("MaxZfw"),trim($"NumOfAdeAirOnlThoWhiHavBeeConByTheCal").as("NumOfAdeAirOnlThoWhiHavBeeConByTheCal"),trim($"SecondAltFliTimInMin").as("SecondAltFliTimInMin"),trim($"NoOfWayOfRouToSecondDesAltDesSecondAlt").as("NoOfWayOfRouToSecondDesAltDesSecondAlt"),trim($"GreCirDisInNm").as("GreCirDisInNm"),trim($"Sta").as("Sta"),trim($"MosCriPoiLat").as("MosCriPoiLat"),trim($"FourthAltFliDisInNm").as("FourthAltFliDisInNm"),trim($"PerCor1000_drvd").as("PerCor1000_drvd"),trim($"PlaBloTimInMin").as("PlaBloTimInMin"),trim($"NumOfSuiAir").as("NumOfSuiAir"),trim($"NoOfWayOfRouToFourthDesAltDesFourthAlt").as("NoOfWayOfRouToFourthDesAltDesFourthAlt"),trim($"helix_uuid").as("helix_uuid"),trim($"NoOfWayOfRouToFirstDesAltDesFirstAlt").as("NoOfWayOfRouToFirstDesAltDesFirstAlt"),trim($"NoEnrAltPer100").as("NoEnrAltPer100"),trim($"NoOfWayOfRouToThirdDesAltDesThirdAlt").as("NoOfWayOfRouToThirdDesAltDesThirdAlt"),trim($"EtoAddFue").as("EtoAddFue"),trim($"ThirdAltFliDisInNm").as("ThirdAltFliDisInNm"),trim($"AirOpeIat").as("AirOpeIat"),trim($"TaxInTimInMin").as("TaxInTimInMin"),trim($"DepRun").as("DepRun"),trim($"TriFueCorZfwPlu100Kg").as("TriFueCorZfwPlu100Kg"),trim($"FliNum").as("FliNum"),trim($"ExtTim2TimOfAddInMin").as("ExtTim2TimOfAddInMin"),trim($"TriDisInNmGro").as("TriDisInNmGro"),trim($"AltAtTopOfdec100FeeOr10tMet").as("AltAtTopOfdec100FeeOr10tMet"),trim($"AddFue4").as("AddFue4"),trim($"EtoInd").as("EtoInd"),trim($"AddFue1").as("AddFue1"),trim($"NoOfWayOfRouToRecAirDepRecAir").as("NoOfWayOfRouToRecAirDepRecAir"),trim($"EnrFueAltEraIcaFor").as("EnrFueAltEraIcaFor"),trim($"FliDupNo").as("FliDupNo"),trim($"AtcCal").as("AtcCal"),trim($"TriDisInNmAir").as("TriDisInNmAir"),trim($"ThirdAltFliTimInMin").as("ThirdAltFliTimInMin"),trim($"TriFueCorZfwMin100Kg").as("TriFueCorZfwMin100Kg"),trim($"AirDes").as("AirDes"),trim($"TriTimInMin").as("TriTimInMin"),trim($"NumOfPetIncEntAndExiPoi").as("NumOfPetIncEntAndExiPoi"),trim($"ExtTim4TimOfAddInMin").as("ExtTim4TimOfAddInMin"),trim($"ReaForAdd4").as("ReaForAdd4"),trim($"SecondAltFliDisInNm").as("SecondAltFliDisInNm"),trim($"InfTyp").as("InfTyp"),trim($"StdUtcOff").as("StdUtcOff"),trim($"ReaForAdd2").as("ReaForAdd2"),trim($"MosCriPoiLon").as("MosCriPoiLon"),trim($"DisNam").as("DisNam"),trim($"UniOfMea").as("UniOfMea"),trim($"DesAirIca").as("DesAirIca"),trim($"TakAltIat").as("TakAltIat"),$"EstimatedTime_drvd".as("EstimatedTime_drvd"),trim($"ConTimTimForConFueInMin").as("ConTimTimForConFueInMin"),trim($"FirstAltIat").as("FirstAltIat"),trim($"FirstAltFliDisInNm").as("FirstAltFliDisInNm"),trim($"DepAirIat").as("DepAirIat"),trim($"EcoFue").as("EcoFue"),trim($"BloFue").as("BloFue"),trim($"SidExiPoi").as("SidExiPoi"),trim($"OpeSuf").as("OpeSuf"),trim($"AirIcaCod").as("AirIcaCod"),trim($"RclNum").as("RclNum"),trim($"tibco_message_time").as("tibco_message_time"),trim($"ConPer100").as("ConPer100"),trim($"EstTotFliTimTimOfPlnInMin").as("EstTotFliTimTimOfPlnInMin"),$"RampFuel_drvd".as("RampFuel_drvd"),trim($"StaDisInNm").as("StaDisInNm"),trim($"AltAtTopOfCli100FeeOr10tMet").as("AltAtTopOfCli100FeeOr10tMet"),trim($"FirstAltFliTimInMin").as("FirstAltFliTimInMin"),trim($"SavInUsd").as("SavInUsd"),trim($"TaxFueOut").as("TaxFueOut"),trim($"SecondAltFue").as("SecondAltFue"),trim($"PlnZfw").as("PlnZfw"),trim($"TaxOutTimInMin").as("TaxOutTimInMin"),trim($"RouOptCri").as("RouOptCri"),trim($"FirstAltFue").as("FirstAltFue"),trim($"TriFueOneFlBel").as("TriFueOneFlBel"),trim($"FourthAltIat").as("FourthAltIat"),trim($"TakAltIca").as("TakAltIca"),trim($"FirstAltIca").as("FirstAltIca"),trim($"ThirdAltFue").as("ThirdAltFue"),trim($"TaxFueIn").as("TaxFueIn"),trim($"SecondAltIca").as("SecondAltIca"),trim($"IatAirTyp").as("IatAirTyp"),$"rank".as("rank"))

    return trimAllColumns(final_df_cos)
  }

  def writetohdfs(output_df : DataFrame,months : Int,spark_partitions : Int,hdfs_output_path_incr : String, hdfs_output_path_hist : String) = {

    hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    output_df.repartition(spark_partitions*100).registerTempTable("lido_dedup_temp_tbl")
    //hiveContext.cacheTable("lido_dedup_temp_tbl")

    val max_flight_date = hiveContext.sql("select max(DatOfOriUtc) from lido_dedup_temp_tbl").first().mkString

    log.info("[INFO] MAX FLIGHT DATE IS "+ max_flight_date +"...")

    val incr_sql = "select * from lido_dedup_temp_tbl where to_date(DatOfOriUtc) >= add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"
    val hist_sql = "select * from lido_dedup_temp_tbl where to_date(DatOfOriUtc) < add_months(to_date('" + max_flight_date + "')," + -1 * months + ")"

    log.info("[INFO] HISTORY SQL QUERY "+ hist_sql +"...")
    log.info("[INFO] INCREMENTAL SQL QUERY "+ incr_sql +"...")

    val output_df_last_incr = hiveContext.sql(incr_sql)
    val output_df_last_hist = hiveContext.sql(hist_sql)

    val output_df_last_incr_coalesece = output_df_last_incr.coalesce(spark_partitions)
    val output_df_last_hist_coalesece = output_df_last_hist.coalesce(spark_partitions)

    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_incr +"...")
    output_df_last_incr_coalesece.write.parquet(hdfs_output_path_incr)
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_incr +" successfully")

    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_hist +"...")
    output_df_last_hist_coalesece.write.parquet(hdfs_output_path_hist)
    log.info("[INFO] WRITING DATA TO OUTPUT PATH "+ hdfs_output_path_hist +" successfully")
  }

  //Schema for reading spark cmd arguments
  case class Config(hdfs_hist_input_path : String = null,hdfs_incr_input_path : String = null,hdfs_output_path_incr: String = null, compression: String = null, spark_partitions : Int = 0, no_of_months : Int = 0,hdfs_output_path_hist : String = null)

  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("Dedup_Lido_Fsum_Flight_Level_Hist_Incr") {
      head("Dedup_Lido_Fsum_Flight_Level_Hist_Incr")
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

      //READING DATA FORM PREREQUISITE
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_hist_input_path + "...")
      val hist_input_df = hiveContext.read.avro(config.hdfs_hist_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_hist_input_path + "is successfull")

      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_incr_input_path + "...")
      val incr_input_df = hiveContext.read.avro(config.hdfs_incr_input_path)
      log.info("[INFO] READING INPUT DATA FROM THE PATH" + config.hdfs_incr_input_path + "is successfull")

      //CALLING HHMMTOMins Function
      log.info("[INFO] PERFORMING DEDUP (UNION,DUPLICATES,LATEST) ...")
      val output_df = dedup(hist_input_df,incr_input_df)
      log.info("[INFO] PERFORMING DEDUP (UNION,DUPLICATES,LATEST) IS COMPLETED SUCCESSFULLY")

      //WRITING DATA TO PREREQUISITE IN AVRO FORMAT WITH SNAPPY COMPRESSION
      writetohdfs(output_df,config.no_of_months,config.spark_partitions,config.hdfs_output_path_incr,config.hdfs_output_path_hist)

    }
  }


}
