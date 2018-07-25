/*----------------------------------------------------------
* Created on : 21/01/2018
* Author : Vera Ekimenko (S794642), Fayaz (S796466)
* Email : vera.ekimenko@dnata.com, fayazbasha.shaik@dnata.com
* Version : 1.0
* Project : Helix-OpsEfficnecy
* Filename : CoreTransformers.scala
* Description : Scala application package object to save Historical CORE data to Incremental schema
* ----------------------------------------------------------
*/

package com.emirates.helix.emfg

import java.util.Locale

import com.emirates.helix.emfg.CoreApp.sqlContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions.{array, collect_list, struct, when, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

class CoreTransformers extends Serializable {
  /**
    * This is a collection of functions to transform CORE data.
    */

  /**
    * This function loads AVRO files found in the path specified into DataFrame.
    * @param path String e.g. "file:///C:/file.avro" or "hdfs://helixcloud/data/helix/decomposed/flightfull/core/incremental/1.0/2017-10-20/00-00"
    * @return DataFrame
    */
  def loadDF(path: String): DataFrame = {
    sqlContext.read.format("com.databricks.spark.avro").load(path)
  }

  /**
    * Thif method syncs Historical CORE data with Incremental
    * @param fi DataFrame with Flight Info Historical CORE (Omega) table
    * @param mvt DataFrame with Movement Historical CORE (Omega) table
    * @param del DataFrame with Delays Historical CORE (Omega) table
    * @param sc SparkContext
    * @param sqlContext HiveContext
    * @return DataFrame with schema identical to Incremental
    */
  def sync(fi: DataFrame, mvt: DataFrame, del: DataFrame)(implicit sc: SparkContext ,sqlContext: HiveContext): DataFrame = {

    import sqlContext.implicits._

    val formatEffectiveTo = udf { (str: String) => {
      str.substring(0,10)+"T"+str.substring(11,19)+".000+04:00"
    }
    }

    fi
      .join(mvt,(mvt("FLIGHTSEQNO2") === fi("FLIGHTSEQNO")),"leftouter")
      .join(del,del("FLIGHTSEQNO3") === fi("FLIGHTSEQNO"),"leftouter")
      .withColumn("CHECK",lit(true))
      .select(
        when($"CHECK",struct(
          when($"CHECK",$"TRANSDATETIMETZ").as("TransDateTime"),
          when($"CHECK",$"TRANSUSERID").as("TransUserId")
        )).as("Audit"),
        $"Delays".cast("struct<Delay:array<struct<PostedOn:string,amount:string,code:string,type:string>>>"),
        when($"CHECK",array(
          when($"CHECK",struct(
            when($"CHECK",$"ACOWNER").as("AcOwner"),
            when($"CHECK",$"ACSUBTYPE").as("AcSubType"),
            when($"CHECK",$"ACVERSION").as("AcVersion"),
            when($"CHECK",$"ARRSTN").as("ArrStn"),
            when($"CHECK",$"BLKTIMEACTPT").as("BlkTimeAct"),
            when($"CHECK",$"BLKTIMEORIGPT").as("BlkTimeOrig"),
            when($"CHECK",$"CABIN_DOOR_CLOSURE").as("CABIN_DOOR_CLOSURE"),
            when($"CHECK",$"CARGO_DOOR_CLOSURE").as("CARGO_DOOR_CLOSURE"),
            when($"CHECK",$"CXCD").as("CxCd"),
            when($"CHECK",$"DEPNUM").as("DepNum"),
            when($"CHECK",$"DEPSTN").as("DepStn"),
            when($"CHECK",$"DORFLAG").as("DorFlag"),
            when($"CHECK",$"DORREMARK").as("DorRemark"),
            when($"CHECK",struct(
              when($"CHECK",$"FLIGHTDATEPD").as("_ATTRIBUTE_VALUE"),
              when($"CHECK",lit(null).cast("string")).as("_Type")
            )).as("FltDate"),
            when($"CHECK",struct(
              when($"CHECK",lit(null).cast("string")).as("_ATTRIBUTE_VALUE"),
              when($"CHECK",lit(null).cast("string")).as("_Type")
            )).as("FltDateLocal"),
            when($"CHECK",$"FLTNUM").as("FltNum"),
            when($"CHECK",$"FLTSTATUS").as("FltStatus"),
            when($"CHECK",$"FLTSUFFIX").as("FltSuffix"),
            when($"CHECK",$"FLTTIMEPT").as("FltTime"),
            when($"CHECK",array(
              when($"CHECK",struct(
                when($"CHECK",lit(null).cast("string")).as("_ATTRIBUTE_VALUE"),
                when($"CHECK",lit(null).cast("string")).as("_Type"),
                when($"CHECK",lit(null).cast("string")).as("_Units")
              ))
            )).as("Fuel"),
            when($"CHECK",$"LEGNUM").as("LegNum"),
            when($"CHECK",$"MvtDateTime"
              .cast("array<struct<_ATTRIBUTE_VALUE:string,_Type:string>>"))
              .as("MvtDateTime"),
            when($"CHECK",$"ONWARDFLIGHT").as("OnwardFlight"),
            when($"CHECK",array(
              when($"CHECK",$"REMARK")
            )).as("Remark"),
            when($"CHECK",struct(
              when($"CHECK",$"RTG_FLAG").as("_ATTRIBUTE_VALUE"),
              when($"CHECK",$"LAST_DOOR_CLOSURE").as("_LastDoorCl")
            )).as("Rtg"),
            when($"CHECK",array(
              when($"CHECK",struct(
                when($"CHECK",$"SCHDTIMEDEPTZ").as("_ATTRIBUTE_VALUE"),
                when($"CHECK",lit("Departure")).as("_Type")
              )),
              when($"CHECK",struct(
                when($"CHECK",$"SCHDTIMEARRTZ").as("_ATTRIBUTE_VALUE"),
                when($"CHECK",lit("Arrival")).as("_Type")
              ))
            )).as("SchdDateTime"),
            when($"CHECK",$"SERVICETYPE").as("ServiceType"),
            when($"CHECK",array(
              when($"CHECK",struct(
                when($"CHECK",$"SIREMARKARR").as("_ATTRIBUTE_VALUE"),
                when($"SIREMARKARR".isNotNull,lit("Arrival")).as("_Type")
              )),
              when($"CHECK",struct(
                when($"CHECK",$"SIREMARKDEP").as("_ATTRIBUTE_VALUE"),
                when($"SIREMARKDEP".isNotNull,lit("Departure")).as("_Type")
              ))
            )).as("SiRemark"),
            when($"CHECK",struct(
              when($"CHECK",$"TAILNO").as("_ATTRIBUTE_VALUE"),
              when($"CHECK",$"TAILNOTYPE").as("_ActionType")
            )).as("TailNo"),
            when($"CHECK",$"TAXI_IN").as("TaxiIn"),
            when($"CHECK",$"TAXI_OUT").as("TaxiOut")
          ))
        )).alias("Events"),
        when($"CHECK",struct(
          when($"CHECK",$"ARRSTNPK").as("ArrStn"),
          when($"CHECK",$"CXCDPK").as("CxCd"),
          when($"CHECK",$"DEPNUMPK").as("DepNum"),
          when($"CHECK",$"DEPSTNPK").as("DepStn"),
          when($"CHECK",struct(
            when($"CHECK",$"FLIGHTDATEPDPK").as("_ATTRIBUTE_VALUE"),
            when($"CHECK",lit(null).cast("string")).as("_Type")
          )).as("FltDate"),
          when($"CHECK",$"FLTNUMPK").as("FltNum"),
          when($"CHECK",$"FLTSUFFIXPK").as("FltSuffix"),
          when($"CHECK",$"LEGNUMPK").as("LegNum")
        )).alias("FlightId"),
        when($"CHECK",struct(
          when($"CHECK",array(
            when($"CHECK",struct(
              when($"CHECK",lit(null).cast("string")).as("PostedOn"),
              when($"CHECK",lit(null).cast("string")).as("Schedtime")
            ))
          )).as("SchedChangeArr"),
          when($"CHECK",array(
            when($"CHECK",struct(
              when($"CHECK",lit(null).cast("string")).as("PostedOn"),
              when($"CHECK",lit(null).cast("string")).as("Schedtime")
            ))
          )).as("SchedChangeDep")
        )).as("SchedChanges"),
        when($"CHECK",lit("Update")).as("_ActionType"),
        when($"CHECK",lit("Full")).as("_MessageType"),
        when($"CHECK",lit("COREODS")).as("_Originator"),
        when($"CHECK",lit("http://dxbafpww03/EOCportal/schemas FlightEvent.xsd")).as("_schemaLocation"),
        when($"CHECK",lit("http://dxbafpww03/EOCportal/schemas")).as("_xmlns"),
        when($"CHECK",lit("http://www.w3.org/2001/XMLSchema-instance")).as("_xsi"),
        when($"CHECK",formatEffectiveTo($"EFFECTIVETO")).as("tibco_messageTime")
      )

  }

  /**
    * This collection of functions that add various columns to a given DataFrame.
    *
    * @param df DataFrame
    * @return DafaFrame
    */
  implicit class CoreExtra(df: DataFrame){

    import sqlContext.implicits._

    /* These UDFs are used by the implicit transformation */
    val properTime = udf { (str: String) => {
      str match {
        case s: String => ("00000" + s.replace("null", "")).takeRight(5)
        case _ => "00000"
      }
    }
    }

    val properDate = udf((str: String) => str.take(10))

    val transDateTimeWithTimezone = udf { (str: String) => {
      val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.0").withLocale(Locale.ENGLISH).withZoneUTC()
      val str2 = format.parseDateTime(str).minusHours(4).toString
      str2.substring(0,10)+"T"+str2.substring(11,19)+"+04:00"
    }
    }
    val mvtDateTimeWithTimezone = udf { (loc: String, utc: String) => {
      if(loc != null && utc != null) {
        if (loc != utc) {
          val pattern = "yyyy-MM-dd HH:mm:ss.0"
          val format = DateTimeFormat.forPattern(pattern).withLocale(Locale.ENGLISH).withZoneUTC()
          val loc1 = format.parseDateTime(loc)
          val utc1 = format.parseDateTime(utc)
          val tz = DateTimeZone.forOffsetMillis((loc1.getMillis - utc1.getMillis).toInt)
          val r = utc1.withZone(tz).toString
          r.substring(0, 19) + r.substring(23, 29)
        } else {
          loc.substring(0, 19) + "+00:00"
        }
      }else{
        ""
      }
    }
    }

    val helix_uuid = udf(() => java.util.UUID.randomUUID().toString)
    val helix_timestamp =  udf(() => (System.currentTimeMillis()/1000L).toString)
    val pk = List("ARRSTN","CXCD","DEPNUM","DEPSTN","FLIGHTDATEPD","FLTSUFFIX","FLTNUM","LEGNUM")
    val pkGrp = Window.partitionBy(pk.map(col(_)):_*).orderBy($"TRANSDATETIME".asc)


    /* This function transforms MVT dataframe */
    def withMvtDateTime(): DataFrame = {
      /* df.groupBy("FLIGHTSEQNO","TRXNO")
        .agg(collect_list(struct($"MVTDATETIMETZ",$"MVTTYPE")).as("MvtDateTime"))
        .withColumnRenamed("FLIGHTSEQNO","FLIGHTSEQNO2")
        // updated the above code
      */
      import sqlContext.implicits._
     /* val mvt_grp1 = Window.partitionBy("FLIGHTSEQNO", "MVTTYPE")
      val mvt_df2 = df.withColumn("MAX_MVTDATETIMETZ",max($"MVTDATETIMETZ").over(mvt_grp1)).filter($"MAX_MVTDATETIMETZ" === $"MVTDATETIMETZ")
      val mvt_grp2 = Window.partitionBy("FLIGHTSEQNO","MVTTYPE").orderBy("MAX_MVTDATETIMETZ")
      val mvt_df3 = mvt_df2.withColumn("RANKED",row_number().over(mvt_grp2)).filter($"RANKED" === 1)
      val mvt_final_df = mvt_df3.groupBy("FLIGHTSEQNO").agg(collect_list(struct($"MVTDATETIMETZ",$"MVTTYPE")).as("MvtDateTime")).withColumnRenamed("FLIGHTSEQNO","FLIGHTSEQNO2")
      return mvt_final_df
      */
      df.filter($"EFFECTIVETO" === "2075-12-31 00:00:00.0").groupBy("FLIGHTSEQNO")
        .agg(collect_list(struct($"MVTDATETIMETZ",$"MVTTYPE")).as("MvtDateTime"))
        .withColumnRenamed("FLIGHTSEQNO","FLIGHTSEQNO2")
        // updated the above code and added filter

    }

    /* This function transforms Delays dataframe */
    def withDelays(): DataFrame = {

      df.filter($"EFFECTIVETO" === "2075-12-31 00:00:00.0").groupBy("FLIGHTSEQNO")
        .agg(struct(
          collect_list(
            struct(
              $"MODON",
              $"DELAYAMOUNT",
              $"DELAYCODE",
              $"DELAYTYPE"
            ) //.cast(del_schema)
          ) //.as("Delay")
        ).as("Delays"))
        .withColumnRenamed("FLIGHTSEQNO","FLIGHTSEQNO3")
    }

    /* This function returns time columns in the proper format */
    def withProperTime(): DataFrame = {
      List("FLTTIME", "BLKTIMEORIG", "BLKTIMEACT").foldLeft(df){(df,column) =>
        df.withColumn(column+"PT",properTime(col(column)))
      }
    }

    /* This function returns time columns in the proper format */
    def withProperDate(): DataFrame = {
      df.withColumn("FLIGHTDATEPD",properDate($"FLIGHTDATE"))
    }

    /* This function creates primary keys  */
    def withPK(): DataFrame = {
      pk.foldLeft(df.withColumn("RN",row_number()over(pkGrp))) { (df, col) =>
        df
          .selectExpr("*",s"CASE WHEN RN = 1 THEN $col END AS 0$col")
          .withColumn(col+"PK",max(s"0$col")over(pkGrp))
      }
    }

    /* This function converts datetime columns with timezones  */
    def withTZ(): DataFrame = {
      df
        .withColumn("TRANSDATETIMETZ", transDateTimeWithTimezone($"TRANSDATETIME"))
        .withColumn("SCHDTIMEARRTZ", mvtDateTimeWithTimezone($"SCHDTIMEARR",$"SCHDTIMEARRUTC"))
        .withColumn("SCHDTIMEDEPTZ", mvtDateTimeWithTimezone($"SCHDTIMEDEP",$"SCHDTIMEDEPUTC"))
    }

    /* This function converts datetime columns with timezones  */
    def withMVTTZ(): DataFrame = {
      df
        .withColumn("MVTDATETIMETZ", mvtDateTimeWithTimezone($"MVTDATETIME",$"MVTDATETIMEUTC"))

    }

    /* This function adds HELIX columns */
    def withHELIX(): DataFrame = {
      df
        .withColumn("HELIX_UUID", helix_uuid())
        .withColumn("HELIX_TIMESTAMP", helix_timestamp())
    }
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
      case p if (p < 0 || p == current_partitions) => saveWithoutRepartitioning
      case p if (p > 0 && p < current_partitions) => saveWithCoalesce(p)
      case p if (p > 0 && p > current_partitions) => saveWithRepartitioning(p)
      case _ => saveWithoutRepartitioning
    }
  }

}