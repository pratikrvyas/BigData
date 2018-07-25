package com.emirates.helix

import org.apache.spark.sql.DataFrame
import java.util.UUID
import org.apache.spark.sql.functions._

object UDF {


  val generateUUID = udf(() => UUID.randomUUID().toString)


  val generateTimestamp = udf(() => (System.currentTimeMillis() / 1000L).toString)


  def getIndex(text: String): Int = {
    var index = 0

    index = text.toUpperCase.indexOf("LOAD", 0)

    index
  }

  val udf_getIndex = udf(getIndex(_: String))


  def getLoadsheetIndex(text: String): Int = text.toUpperCase match {
    case "LOADSHEET" => 0
    case "-  LOADSH" => 3
    case _ => 0
  }

  val udf_getLoadsheetIndex = udf(getLoadsheetIndex(_: String))



  def writeToDecomposed(out_df: DataFrame, out_path : String) : Unit = {


    import com.databricks.spark.avro._
    out_df.write.avro(out_path)

  }










}