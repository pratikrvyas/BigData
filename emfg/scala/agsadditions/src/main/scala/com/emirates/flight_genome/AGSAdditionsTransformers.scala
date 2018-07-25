package com.emirates.flight_genome

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window

class AGSAdditionsTransformers extends Serializable {


  /**
    * This is a collection of functions to transform raw AGS data.
    */


  /**
    * This function returns a DataFrame for a given path
    * @param path String
    * @return DataFrame
    */
  def getDF(path: String)(implicit sqlContext: HiveContext): DataFrame = {
    sqlContext.read.format("com.databricks.spark.avro").load(path)
  }


  /**
    * This UDF function returns timestamp.
    */
  val timestamp = (tz: String, cols: Seq[Column]) => to_utc_timestamp(unix_timestamp(concat(cols: _*),
    "dd/MM/yy HH:mm:ss").cast("timestamp"),tz)

  /**
    * This collection of functions that add various columns to a given DataFrame.
    *
    * @param df DataFrame
    * @return DafaFrame
    */
  implicit class AGSAdditions(df: DataFrame) {

    /** This method adds Altitude **/
    def withAltitude(): DataFrame = {
      for (col <- Array("AltStandard", "AltQNH", "TransitionAltitudeDiscrete")) {
        assert(df.columns contains col, s"Required $col is missing.")
      }
      assert(df.columns.contains("Altitude") == false, "Altitude column exists.")
      df.selectExpr("*", "CASE WHEN TRIM(TransitionAltitudeDiscrete) in ('IN_STD', 'ALT STD') " +
        "THEN TRIM(AltStandard) ELSE TRIM(AltQNH) END AS Altitude")
    }

    /** This method adds Latitude **/
    def withLatitude(): DataFrame = {
      assert(df.columns contains "LATPC", s"Required LATPC is missing.")
      assert(df.columns.contains("Latitude") == false, "Latitude column exists.")
      df.selectExpr("*", "CASE " +
        "WHEN LATPC rlike \"S.*\" THEN CONCAT('-',SUBSTR(TRIM(LATPC),2)) " +
        "WHEN LATPC rlike \"N.*\" THEN SUBSTR(TRIM(LATPC),2) " +
        "WHEN LATPC rlike \"W.*\" THEN CONCAT('-',SUBSTR(TRIM(LATPC),2)) " +
        "WHEN LATPC rlike \"E.*\" THEN SUBSTR(TRIM(LATPC),2) " +
        "ELSE SUBSTR(TRIM(LATPC),2) END AS Latitude")
    }

    /** This method adds Longitude **/
    def withLongitude(): DataFrame = {
      assert(df.columns contains "LONPC", s"Required LONPC is missing.")
      assert(df.columns.contains("Longitude") == false, "Longitude column exists.")
      df.selectExpr("*", "CASE " +
        "WHEN LONPC rlike \"S.*\" THEN CONCAT('-',SUBSTR(TRIM(LONPC),2)) " +
        "WHEN LONPC rlike \"N.*\" THEN SUBSTR(TRIM(LONPC),2) " +
        "WHEN LONPC rlike \"W.*\" THEN CONCAT('-',SUBSTR(TRIM(LONPC),2)) " +
        "WHEN LONPC rlike \"E.*\" THEN SUBSTR(TRIM(LONPC),2) " +
        "ELSE SUBSTR(TRIM(LONPC),2) END AS Longitude")
    }

    /** This method adds WindDirectionAmmended **/
    def withWindDirectionAmmended(): DataFrame = {
      assert(df.columns contains "WINDDirection", s"Required WINDDirection is missing.")
      assert(df.columns.contains("WindDirectionAmmended") == false, "WindDirectionAmmended column exists.")
      df.selectExpr("*", "CASE " +
        "WHEN CAST(TRIM(WINDDirection) AS Double) <= 0 THEN ROUND(CAST(TRIM(WINDDirection) AS Double)+360,2) " +
        "ELSE ROUND(CAST(TRIM(WINDDirection) AS Double),2) END AS WindDirectionAmmended")
    }

    /** This method adds WindComponentAGS **/
    def withWindComponentAGS(): DataFrame = {
      assert(df.columns contains "WindComponent", s"Required WindComponent is missing.")
      assert(df.columns.contains("WindComponentAGS") == false, "WindComponentAGS column exists.")
      df.selectExpr("*", "CAST(TRIM(WindComponent) AS Double) AS WindComponentAGS")
    }


    /** This method adds WindComponentNAV **/
    def withWindComponentNAV(): DataFrame = {
      assert(df.columns contains "WindComponent", s"Required WindComponent is missing.")
      assert(df.columns.contains("WindComponentNAV") == false, "WindComponentNAV column exists.")
      df.selectExpr("*", "(TRIM(WindComponent) * -1) AS WindComponentNAV")
    }


    /** This method adds GroundSpeed **/
    def withGroundSpeed(): DataFrame = {
      for (col <- Array("WindComponentNAV", "TAS")) {
        assert(df.columns contains col, s"Required $col is missing.")
      }
      assert(df.columns.contains("GroundSpeed") == false, "GroundSpeed column exists.")
      df.selectExpr("*", "(TRIM(TAS) + WindComponentNAV) AS GroundSpeed")
    }


    /* This method adds InitialTrackDistance and other Window based columns */
    def withWindowCalculations(implicit sqlContext: HiveContext): DataFrame = {

      for (col <- Array("FileName", "Date", "UTC", "FlightPhase", "TrackDistance", "GroundSpeed")) {
        assert(df.columns contains col, s"Required $col is missing.")
      }

      for (col <- Array("DistancePhases", "SequenceNumber", "InitialTrackDistance", "TrackDistanceFromInitial",
        "AirDist", "AccAirDist", "PreviousTimestamp", "AccTime")) {
        assert(df.columns.contains(col) == false, s"$col column exists.")
      }

      val df_ts = df
        .withColumn("Timestamp", timestamp("UTC", Seq(df("Date"), df("UTC"))))
        .selectExpr("*", "CASE WHEN TRIM(FlightPhase) IN ('ENG. ST', 'ENG. STO', 'TAXI OU', " +
          "'ENG. S', 'TAXI O', 'ENG. STOP', 'TAXI OUT') THEN FALSE ELSE TRUE END AS DistancePhases")
        .selectExpr("*", "CASE WHEN TRIM(TrackDistance) <> 0 AND DistancePhases = TRUE THEN TRUE " +
          "ELSE FALSE END AS TrackDistanceAboveZero")

      import sqlContext.implicits._

      val TimestampGrp1_ = Window
        .partitionBy($"FolderName", $"FileName", $"TrackDistanceAboveZero")
        .orderBy($"RowNumber".asc)

      val TimestampGrp3_ = Window
        .partitionBy("FolderName", "FileName")
        .orderBy()

      val TimestampGrp3 = Window
        .partitionBy($"FolderName", $"FileName")
        .orderBy($"RowNumber".asc)

      val TimestampGrp4 = Window
        .partitionBy($"FolderName", $"FileName")
        .rowsBetween(Long.MinValue, 0)
        .orderBy($"RowNumber".asc)

      df_ts
        .withColumn("FirstRN", first("RowNumber").over(TimestampGrp1_))
        .selectExpr("*", "CASE WHEN  DistancePhases = TRUE AND FirstRN = RowNumber THEN TRIM(TrackDistance) " +
          "ELSE NULL END AS InitTD")
        .selectExpr("*", "CASE WHEN  DistancePhases = TRUE AND FirstRN = RowNumber THEN RowNumber " +
          "ELSE NULL END AS InitRN")
        .withColumn("InitialTrackDistance", max("InitTD").over(TimestampGrp3_))
        .withColumn("InitialTrackDistanceRN", max("InitRN").over(TimestampGrp3_))

        .withColumn("PreviousTrackDistance", lag("TrackDistance", 1, -1).over(TimestampGrp3))

        .selectExpr("*", "CASE " +
          "WHEN RowNumber <= 20 OR RowNumber < InitialTrackDistanceRN THEN 0 " +
          "ELSE ROUND(ABS(TRIM(InitialTrackDistance) - TRIM(TrackDistance)),1) END AS TrackDistanceAmended")

        .selectExpr("*", "CASE " +
          "WHEN RowNumber <= 20 THEN 0 " +
          "WHEN GroundSpeed = 0 THEN 0 " +
          "WHEN RowNumber <= InitialTrackDistanceRN THEN 0 " +
          "ELSE ROUND((ABS(TRIM(TrackDistance) - TRIM(PreviousTrackDistance))) * (TRIM(TAS) / GroundSpeed),2) END AS AirDist")
        .withColumn("AccAirDist0", sum("AirDist").over(TimestampGrp3))
        .selectExpr("*","ROUND(AccAirDist0,2) as AccAirDist")
        .drop("AccAirDist0")

        .withColumn("PreviousTimestamp", lag("Timestamp", 1, 0).over(TimestampGrp3))
        .selectExpr("*", "CASE " +
          "WHEN RowNumber <= 21 THEN 0 " +
          "ELSE (unix_timestamp(Timestamp)-unix_timestamp(PreviousTimestamp)) / 60 END AS TimeDiff")
        .withColumn("AccTime0", sum("TimeDiff").over(TimestampGrp4))
        .selectExpr("*","ROUND(AccTime0,2) as AccTime")
        .drop("AccTime0")
    }

    /**
      * This function repartitions a given DataFrame and then saves it to a specified location.
      *
      * @param partitions Int, Number of partitions
      * @param path       String, location to save DataFrame (see examples in the description of loadDF function)
      */
    def save(partitions: Int, path: String): Unit = {
      val saveWithoutRepartitioning = df.write.mode(SaveMode.Overwrite).parquet(path)
      val saveWithRepartitioning = (p: Int) => df.repartition(p).write.mode(SaveMode.Overwrite).parquet(path)
      val saveWithCoalesce = (p: Int) => df.coalesce(p).write.mode(SaveMode.Overwrite).parquet(path)

      val current_partitions = df.rdd.partitions.length
      partitions match {
        case 0 => saveWithoutRepartitioning
        case p if (p < 0 || p == current_partitions) => saveWithoutRepartitioning
        case p if (p > 0 && p < current_partitions) => saveWithCoalesce(p)
        case p if (p > 0 && p > current_partitions) => saveWithRepartitioning(p)
        case _ => saveWithoutRepartitioning
      }
    }

  }
}