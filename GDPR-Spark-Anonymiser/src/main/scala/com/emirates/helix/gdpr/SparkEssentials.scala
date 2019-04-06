package com.emirates.helix.gdpr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

trait SparkEssentials {


  // gets the spark session
  def getSparkSession(appName : String) : SparkSession = {
    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()

     spark
  }

  //  change struct "skyward" in last stDF
  // val schema = tsDF.schema.fields.find(_.name == "skyward").get
  // val updatedStructNames: Seq[Column] = schema.dataType.asInstanceOf[StructType].fieldNames.map(name => col("skyward." + name))
  // val withUpdatedSchema = stDF.withColumn("skyward", struct(updatedStructNames: _*))


  // preserves the order the columns while doing union
  def getStructRecursiveDataFrame(df1 : DataFrame, df2 : DataFrame,columns : Array[String]) : DataFrame = {
    if(columns.isEmpty) {
      df2
    }
    else {
      val col_name = columns.head
      val col_schema = df1.schema.fields.find(_.name == col_name).get
      if(col_schema.dataType.typeName.equals("struct")){
        val updatedStructNames: Seq[Column] = col_schema.dataType.asInstanceOf[StructType].fieldNames.map(name => col(col_name + "." + name))
        getStructRecursiveDataFrame(df1,df2.withColumn(col_name, struct(updatedStructNames: _*)),columns.tail)
      }
      else{ getStructRecursiveDataFrame(df1,df2,columns.tail)}
    }
  }

  def unionByName(a:  org.apache.spark.sql.DataFrame, b:  org.apache.spark.sql.DataFrame):  org.apache.spark.sql.DataFrame = {
    val b_new_df = getStructRecursiveDataFrame(a,b,a.columns)
    val columns_seq = a.columns.toSet.intersect(b_new_df.columns.toSet).map(col).toSeq
    a.select(columns_seq: _*).union(b_new_df.select(columns_seq: _*))
  }

  // Converts String to Integer and avoids null pointer exception
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  // Converts String to String and avoids null pointer exception
  def toStr(s: String): Option[String] = {
    try {
      Some(s.toString)
    } catch {
      case e: Exception => None
    }
  }

  // get the columns name from the dataframe
  def getCols(df : DataFrame): Array[String] = {
    val col_list = df.columns.map{x => x.toLowerCase}

    col_list
  }

  def getColType(df : DataFrame, col_name : String) : String = {
    val col_schema = df.schema.fields.find(_.name == col_name).get
     col_schema.dataType.typeName

  }

  def getDateTime(format :String) : String = {

    val dateFormatter = new SimpleDateFormat(format)
    var submittedDateConvert = new Date()
    val submittedAt = dateFormatter.format(submittedDateConvert)

    submittedAt
  }

}