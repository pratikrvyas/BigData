package com.emirates.flight_genome

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions.{lit, monotonicallyIncreasingId, row_number, udf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ArrayBuffer
import java.net._

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}

import scala.annotation.tailrec

class AGSTransformers extends Serializable {

  /**
    * This is a collection of functions to transform raw AGS data.
    */


  /**
    * This function loads AVRO files found in the path specified into DataFrame.
    * @param path String e.g. "file:///C:/file.avro" or "hdfs://helixcloud/data/helix/decomposed/flightfull/core/incremental/1.0/2017-10-20/00-00"
    * @return Array of file paths
    */
  def listFiles(path: String)(implicit sc: SparkContext): List[SourceFile] = {
    val fs = FileSystem.get(new URI(path), sc.hadoopConfiguration)
    val fs_files = fs.listFiles(new Path(path), true)
    val files = new ArrayBuffer[String]()
    while(fs_files.hasNext){
      files += fs_files.next().getPath.toString
    }
    files.filter(file => file.endsWith("CSV.gz")).map(path => {
      SourceFile(path)
    }).toList
  }

  /**
    * This function returns a DataFrame for a given csvFile
    * @param file csvFile
    * @return DataFrame
    */
  def getDF(file: SourceFile)(implicit sqlContext: HiveContext): DataFrame = {
    val fileName = file.fileName
    var folderName = file.folderName
    val df = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .load(file.path)

//    df.select("C0").collect.foreach(println)
//    df.where("C0 rlike '^.*Refline.*$'").collect.foreach(println)

    val rl = df.where("C0 rlike '^.*Refline.*$'").head()
    val pk_date = rl.get(1)
    val pk_flnum = rl.get(2)
    val pk_tail = rl.get(3)
    val pk_origin = rl.get(4)
    val pk_dest = rl.get(5)

    df
      .withColumn("MonotonicallyIncreasingId", monotonicallyIncreasingId)
      .withColumn("RowNumber", row_number().over(Window.orderBy("MonotonicallyIncreasingId")))
      .drop("MonotonicallyIncreasingId")
      .withColumn("PK_Date", lit(s"$pk_date"))
      .withColumn("PK_FlNum", lit(s"$pk_flnum"))
      .withColumn("PK_Tail", lit(s"$pk_tail"))
      .withColumn("PK_Origin", lit(s"$pk_origin"))
      .withColumn("PK_Dest", lit(s"$pk_dest"))
      .withColumn("FileName", lit(s"$fileName"))
      .withColumn("FolderName", lit(s"$folderName"))

  }

  /**
    * This recursive function returns a unified DataFrame for a given list of csvFiles
    * @param files List of csvFiles
    * @return DataFrame
    */
  @tailrec
  private def getUnifiedDF(acc: DataFrame, files: List[SourceFile])(implicit sqlContext: HiveContext): DataFrame = {
    files match {
      case Nil => acc
      case head :: tail => getUnifiedDF(acc.unionAll(getDF(head)), tail)
    }
  }

  /**
    * This functions returns a schema for Route Points DataFrame
    * @return
    */
  def getRoutePointsSchema(): StructType = {
    StructType(
      StructField("UTC", StringType, true) ::
        StructField("Date", StringType, true) ::
        StructField("AltStandard", StringType, true) ::
        StructField("AltQNH", StringType, true) ::
        StructField("CAS", StringType, true) ::
        StructField("FlightPhase", StringType, true) ::
        StructField("TAS", StringType, true) ::
        StructField("MACH", StringType, true) ::
        StructField("TrackDistance", StringType, true) ::
        StructField("N11", StringType, true) ::
        StructField("N12", StringType, true) ::
        StructField("N13", StringType, true) ::
        StructField("N14", StringType, true) ::
        StructField("IVV", StringType, true) ::
        StructField("GrossWeight", StringType, true) ::
        StructField("FuelBurned", StringType, true) ::
        StructField("LATPC", StringType, true) ::
        StructField("LONPC", StringType, true) ::
        StructField("AirGroundDiscrete", StringType, true) ::
        StructField("TransitionAltitudeDiscrete", StringType, true) ::
        StructField("HeadingTrue", StringType, true) ::
        StructField("BaroSetting", StringType, true) ::
        StructField("CG", StringType, true) ::
        StructField("ISADEV", StringType, true) ::
        StructField("WINDDirection", StringType, true) ::
        StructField("WindSpeed", StringType, true) ::
        StructField("WindComponent", StringType, true) ::
        StructField("TAT", StringType, true) ::
        StructField("SAT", StringType, true) ::
        StructField("CrossWindComponent", StringType, true) ::
        StructField("CrossWindDriftVector", StringType, true) ::
        StructField("FOB", StringType, true) ::
        StructField("FOB_T1", StringType, true) ::
        StructField("FOB_T2", StringType, true) ::
        StructField("FOB_T3", StringType, true) ::
        StructField("FOB_T4", StringType, true) ::
        StructField("FOB_T5", StringType, true) ::
        StructField("FOB_T6", StringType, true) ::
        StructField("FOB_T7", StringType, true) ::
        StructField("FOB_T8", StringType, true) ::
        StructField("FOB_T9", StringType, true) ::
        StructField("FOB_T10", StringType, true) ::
        StructField("FOB_T11", StringType, true) ::
        StructField("FOB_T12", StringType, true) ::
        StructField("Height", StringType, true) ::
        StructField("RowNumber", IntegerType, true) ::
        StructField("PK_Date", StringType, true) ::
        StructField("PK_FlNum", StringType, true) ::
        StructField("PK_Tail", StringType, true) ::
        StructField("PK_Origin", StringType, true) ::
        StructField("PK_Dest", StringType, true) ::
        StructField("FileName", StringType, true) ::
        StructField("FolderName", StringType, true) :: Nil)

  }

  /**
    * This functions returns a schema for Refline DataFrame
    * @return
    */
  def getReflineSchema(): StructType = {
    StructType(
      StructField("Refline", StringType, true) ::
        StructField("Date", StringType, true) ::
        StructField("FlightNumber", StringType, true) ::
        StructField("Tail", StringType, true) ::
        StructField("Origin", StringType, true) ::
        StructField("Destination", StringType, true) ::
        StructField("EngStartTime", StringType, true) ::
        StructField("EngStopTime", StringType, true) ::
        StructField("V1", StringType, true) ::
        StructField("V2", StringType, true) ::
        StructField("VR", StringType, true) ::
        StructField("AssumedTemperature", StringType, true) ::
        StructField("TO_FLAP", StringType, true) ::
        StructField("ZFW", StringType, true) ::
        StructField("RWY_TO", StringType, true) ::
        StructField("BAROMB_TO", StringType, true) ::
        StructField("SAT_TO", StringType, true) ::
        StructField("WC_TO", StringType, true) ::
        StructField("WD_TO", StringType, true) ::
        StructField("WS_TO", StringType, true) ::
        StructField("AI_TO", StringType, true) ::
        StructField("RWY_LD", StringType, true) ::
        StructField("BAROMB_LD", StringType, true) ::
        StructField("SAT_LD", StringType, true) ::
        StructField("WC_LD", StringType, true) ::
        StructField("WD_LD", StringType, true) ::
        StructField("WS_LD", StringType, true) ::
        StructField("VAPP", StringType, true) ::
        StructField("FLAP_LD", StringType, true) ::
        StructField("APU_FUEL", StringType, true) ::
        StructField("FUEL_OFF", StringType, true) ::
        StructField("FUEL_ON", StringType, true) ::
        StructField("FileName", StringType, true) ::
        StructField("FolderName", StringType, true) :: Nil)

  }

  /**
    * This UDF functions return UUID and timestamp.
    */
  val helix_uuid = udf(() => java.util.UUID.randomUUID().toString)
  val helix_timestamp =  udf(() => (System.currentTimeMillis()/1000L).toString)


  /**
    * This collection of functions that add various columns to a given DataFrame.
    *
    * @param df DataFrame
    * @return DafaFrame
    */
  implicit class AGSAdditions(df: DataFrame) {

    /* This method adds HELIX columns */
    def withHELIX(): DataFrame = {
      df
        .withColumn("HELIX_UUID", helix_uuid())
        .withColumn("HELIX_TIMESTAMP", helix_timestamp())
    }

  }


  /**
    * This function loads CSV files found in the path specified into DataFrame.
    * @param files String e.g. "file:///C:/file.avro" or "hdfs://helixcloud/data/helix/decomposed/flightfull/core/incremental/1.0/2017-10-20/00-00"
    * @return Array of file paths
    */
  def loadFiles2(files: List[SourceFile])(implicit sc: SparkContext, sqlContext: HiveContext): DataFrame = {

    val empty_rp = sqlContext.createDataFrame(sc.emptyRDD[Row], getRoutePointsSchema)
    val empty_rl = sqlContext.createDataFrame(sc.emptyRDD[Row], getReflineSchema)
    getUnifiedDF(empty_rp, files)

  }

    /**
    * This function loads CSV files found in the path specified into DataFrame.
    * @param files String e.g. "file:///C:/file.avro" or "hdfs://helixcloud/data/helix/decomposed/flightfull/core/incremental/1.0/2017-10-20/00-00"
    * @return Array of file paths
    */
  def loadFiles(files: List[SourceFile])(implicit sc: SparkContext, sqlContext: HiveContext): Tuple2[DataFrame, DataFrame] = {

    val empty_rp = sqlContext.createDataFrame(sc.emptyRDD[Row], getRoutePointsSchema)
    val empty_rl = sqlContext.createDataFrame(sc.emptyRDD[Row], getReflineSchema)
    val rl_cols = Seq(
      "Date",
      "AltStandard",
      "AltQNH",
      "CAS",
      "FlightPhase",
      "TAS",
      "MACH",
      "TrackDistance",
      "N11",
      "N12",
      "N13",
      "N14",
      "IVV",
      "GrossWeight",
      "FuelBurned",
      "LATPC",
      "LONPC",
      "AirGroundDiscrete",
      "TransitionAltitudeDiscrete",
      "HeadingTrue",
      "BaroSetting",
      "CG",
      "ISADEV",
      "WINDDirection",
      "WindSpeed",
      "WindComponent",
      "TAT",
      "SAT",
      "CrossWindComponent",
      "CrossWindDriftVector",
      "FOB",
      "FileName",
      "FolderName"
    )

    val df = getUnifiedDF(empty_rp, files)
    val filtered_rl = df.where("UTC rlike '^.*Refline.*$'")
    val rp = df.except(filtered_rl)
    val rl = empty_rl.unionAll(filtered_rl.select("UTC", rl_cols: _*))

    (
      rl.withHELIX, rp.withHELIX
    )

  }

  /**
    * This function repartition a given DataFrame and then saves it to a specified location.
    * @param df DataFrame
    * @param partitions Int, Number of partitions
    * @param path String, location to save DataFrame (see examples in the description of loadDF function)
    */
  def saveDF(df: DataFrame, partitions: Int, path: String): Unit = {
    val saveWithoutRepartitioning = df.write.format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save(path)
    val saveWithRepartitioning = (p: Int) => df.repartition(p).write.format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save(path)
    val saveWithCoalesce = (p: Int) => df.coalesce(p).write.format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save(path)

    val current_partitions = df.rdd.partitions.length
    partitions match {
      case 0 => saveWithoutRepartitioning
      case p if(p < 0 || p == current_partitions) => saveWithoutRepartitioning
      case p if(p > 0 && p < current_partitions) => saveWithCoalesce(p)
      case p if(p > 0 && p > current_partitions) => saveWithRepartitioning(p)
      case _ => saveWithoutRepartitioning
    }


  }






















  /*******************************************************************************
  /**
    * This function loads AVRO files found in the path specified into DataFrame.
    * @param path String e.g. "file:///C:/file.avro" or "hdfs://helixcloud/data/helix/decomposed/flightfull/core/incremental/1.0/2017-10-20/00-00"
    * @return DataFrame
    */
  def loadDF(path: String): DataFrame = {
    sqlContext.read.format("com.databricks.spark.avro").load(path)
  }

  /**
    * This function merge list of DataFrames specified into one DataFrame.
    * @param dfs List[DataFrame] e.g. List(df1, df2, df3)
    * @return DataFrame
    */
  def unionDF(dfs: List[DataFrame]): DataFrame = {
    dfs.reduce(_.unionAll(_))
  }

  /**
    * This function deduplicates CORE records in the DataFrame specified.
    * @param coreDF DataFrame with duplicating CORE records
    * @return DataFrame without duplicating CORE records
    */
  //def dedupe(coreDF: DataFrame, sqlContext: HiveContext): DataFrame = {
  def dedupe(coreDF: DataFrame): DataFrame = {

    import sqlContext.implicits._

    val coreGrp = Window.partitionBy($"FlightId.ArrStn", $"FlightId.DepStn", $"FlightId.DepNum", $"FlightId.CxCd",
      $"FlightId.LegNum", $"FlightId.FltSuffix", $"FlightId.FltNum", $"FlightId.FltDate._ATTRIBUTE_VALUE")
      .orderBy(trim($"Audit.TransDateTime").desc)

    coreDF.withColumn("RN", row_number().over(coreGrp)).filter($"RN" === 1).drop("RN")
  }


    *******************************************************************************/




}
