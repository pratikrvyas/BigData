/*
*=====================================================================================================================================
* Created on     :   12/11/2017
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=OpsEfficnecy
* Filename       :   FixedLengthNDataToAvro.scala
* Description    :   Spark application to extract fixed length data with N repetition from  JSON sequence feeds and write to decomposed as avro with compression
* ======================================================================================================================================
*/


//spark-submit --master yarn --queue ingest --class "com.emirates.helix.FixedLengthNDataToAvro" /home/ops_eff/ek_lido/FixedLengthNDataToAvro-1.0.jar -i /data/helix/raw/lido/flight_summary/incremental/1.0/2017-11-06/05-15/*.snappy -o /user/ops_eff/lido/fsum/tmp2 -s /user/ops_eff/lido/fsum/schema/fusm_mapping.csv -c snappy

package com.emirates.helix

import org.apache.hadoop.io.{BytesWritable, LongWritable}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._



object FixedLengthNDataToAvro  {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("FixedLengthNDataToAvro")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  //Schema for reading spark cmd arguments
  case class Config(hdfs_input_path : String = null,hdfs_output_path: String = null, hdfs_mapping_file: String = null, compression: String = null)

  def toMappingFormat(hdfs_mapping_file : String): collection.mutable.LinkedHashMap[String, List[Any]] = {
    val mapping_csv = sc.textFile(hdfs_mapping_file)
    val rows = mapping_csv.map(line => line.split(",").map(_.trim))
    val array_mapping = rows.collect
    var indexes = collection.mutable.LinkedHashMap[String, List[Any]]()

    for( i <- 0 to array_mapping.size - 1){

      if(array_mapping(i).size == 3){
        indexes += (array_mapping(i)(0) -> List(array_mapping(i)(1),array_mapping(i)(2)))
      }else if (array_mapping(i).size > 3){
        indexes += (array_mapping(i)(0) -> (for( x <- 1 to array_mapping(i).size -1 ) yield array_mapping(i)(x)).toList)
      }else{
        println("[ERROR] Invalid Mapping document. Please check")
        sys.exit(1)
      }
    }

    return indexes
  }

  def msgParser(json_tbl : org.apache.spark.sql.DataFrame, indexes : collection.mutable.LinkedHashMap[String, List[Any]]) : ArrayBuffer[scala.collection.mutable.Map[String,Any]] = {
    var dataList = ArrayBuffer[scala.collection.mutable.Map[String,Any]]()
    var strtpoint = 0
    json_tbl.collect.foreach(msg => if(!msg.mkString.isEmpty())
    {
      var data = collection.mutable.LinkedHashMap[String, Any]()
      var noofMsgEndlen = 0
      indexes.foreach (eachIndexList => if(eachIndexList._2.size == 2 && !eachIndexList._2(0).toString.equals("#"))
      {
        strtpoint = eachIndexList._2(0).toString.trim.toInt
        val end = eachIndexList._2(1).toString.trim.toInt
        val eachMsg = msg(0).toString
        data += (eachIndexList._1 -> eachMsg.substring(strtpoint,end))
        strtpoint = end
      }
      else if(eachIndexList._2.size > 2 && !eachIndexList._2(0).toString.equals("#")){
        val noofMsgStrtInd = eachIndexList._2(0).toString.trim.toInt
        val noofMsgEndInd = eachIndexList._2(1).toString.trim.toInt
        val eachMsg = msg(0).toString
        val noofMsgs = eachMsg.substring(noofMsgStrtInd,noofMsgEndInd).trim.toInt
        strtpoint = eachIndexList._2(2).toString.trim.toInt
        var subDataList : List[scala.collection.mutable.LinkedHashMap[String,String]] = Nil
        for( nmsgs <- 1 to noofMsgs){
          var subData = collection.mutable.LinkedHashMap[String, String]()
          for( j <- 3 until eachIndexList._2.size by 2){
            subData += (eachIndexList._2(j).toString -> eachMsg.substring(strtpoint,(strtpoint+eachIndexList._2(j+1).toString.trim.toInt)))
            strtpoint=strtpoint+eachIndexList._2(j+1).toString.trim.toInt
          }
          subDataList = subDataList :+ subData
        }
        data += (eachIndexList._1 -> subDataList)
      }
      else if(eachIndexList._2.size == 2 && eachIndexList._2(0).toString.equals("#")){
        noofMsgEndlen = eachIndexList._2(1).toString.trim.toInt
        val eachMsg = msg(0).toString
        data += (eachIndexList._1 -> eachMsg.substring(strtpoint,(noofMsgEndlen+strtpoint)))
        strtpoint = noofMsgEndlen+strtpoint
      }
      else if(eachIndexList._2.size > 2 && eachIndexList._2(0).toString.equals("#")){
        val noofMsgStrtInd = strtpoint - noofMsgEndlen
        val noofMsgEndIndlen = noofMsgEndlen
        val eachMsg = msg(0).toString
        val noofMsgs = eachMsg.substring(noofMsgStrtInd,noofMsgStrtInd+noofMsgEndIndlen).trim.toInt
        var subDataList : List[scala.collection.mutable.LinkedHashMap[String,String]] = Nil
        for( nmsgs <- 1 to noofMsgs){
          var subData = collection.mutable.LinkedHashMap[String, String]()
          for( j <- 3 until eachIndexList._2.size by 2){
            subData += (eachIndexList._2(j).toString -> eachMsg.substring(strtpoint,(strtpoint+eachIndexList._2(j+1).toString.trim.toInt)))
            strtpoint=strtpoint+eachIndexList._2(j+1).toString.trim.toInt
          }
          subDataList = subDataList :+ subData
        }
        data += (eachIndexList._1 -> subDataList)
      }
      else{
        println("[ERROR] Syntax in Mapping document - Please check")
      }
      )
      data += ("tibco_message_time" -> msg(1).toString)
      dataList += data
      println("[INFO] Actual Message Len: "+msg.mkString.length)
      println("[INFO] Parsed Message Len: "+strtpoint)
    }
    )
    return dataList
  }

  //FORMATTING TO JSON OBJECT FORMAT, ADDING UUID , TIMESTAMP AND STORING TO HDFS
  def jsonMsgFormatSnappyAvroStore(json_tbl : org.apache.spark.sql.DataFrame,map_data_input : ArrayBuffer[scala.collection.mutable.Map[String,Any]], hdfs_ouput_path : String) : String = {

    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.native.JsonMethods._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    import com.databricks.spark.avro._

    //FORMATTING TO JSON OBJECT
    var map_data = map_data_input
    val json_data_Rdd = sc.parallelize(for(x <- 0 to map_data.size - 1) yield Serialization.write(map_data(x)))
    val json_data_df = sqlContext.read.json(json_data_Rdd)

    //ADDING UUID AND TIMESTAMP
    import java.util.UUID
    val generateUUID = udf(() => UUID.randomUUID().toString)
    val data_with_genUUID_df = json_data_df.withColumn("helix_uuid",generateUUID())
    val data_with_genTimestamp_df = data_with_genUUID_df.withColumn("helix_timestamp",current_timestamp())

    //WRITING DATA TO DECOMPOSED IN AVRO FORMAT WITH SNAPPY COMPRESSION
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
    data_with_genTimestamp_df.write.avro(hdfs_ouput_path)

    //Count Test
    val df = sqlContext.read.avro(hdfs_ouput_path+"/*.avro")
    df.registerTempTable("test_tbl")
    val decomposed_msg_cnt = sqlContext.sql("select count(1) as DecomposedCount from test_tbl")
    val decomposed_msg_cnt_array = decomposed_msg_cnt.rdd.collect
    val decomposed_msg_cnt_str = decomposed_msg_cnt_array(0)(0).toString

    val raw_cnt = json_tbl.count

    if(decomposed_msg_cnt_str.equals(raw_cnt.toString)){
      return "[INFO] Count Testcase  Passed : " +raw_cnt.toString+ " = " +decomposed_msg_cnt_str
    }else {
      return "[ERROR] Count Testcase Failed : " +raw_cnt.toString+ " != " +decomposed_msg_cnt_str
    }

  }

  def loadRawMsg(hdfs_input_path : String ) : org.apache.spark.sql.DataFrame = {
    val json_str= sc.sequenceFile[LongWritable, BytesWritable](hdfs_input_path).map(x => new String(x._2.copyBytes(), "utf-8"))
    val json_tbl = sqlContext.read.json(json_str).select("message","metadata.messageTime")
    return json_tbl
  }

  def main(args:Array[String]) : Unit = {

    sc.setLogLevel("ERROR")

    val parser = new scopt.OptionParser[Config]("FixedLengthNDataToAvro") {
      head("FixedLengthNDataToAvro")
      opt[String]('i',"hdfs_input_path")
        .required()
        .action((x,config) => config.copy(hdfs_input_path = x))
        .text("Required parameter : Input file path for raw data")
      opt[String]('o',"hdfs_output_path")
        .required()
        .action((x,config) => config.copy(hdfs_output_path = x))
        .text("Required parameter : Output file path for decomposed data")
      opt[String]('s',"hdfs_mapping_file")
        .required()
        .action((x,config) => config.copy(hdfs_mapping_file = x))
        .text("Required parameter : Decomposed data schema path")
      opt[String]('c',"compression")
        .required()
        .valueName("snappy OR deflate")
        .validate(x => {if(!(x.matches(compress_rex.toString()))) Left("Invalid compression specified : "+ x) else Right(true)} match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x,config) => config.copy(compression = x))
        .text("Required parameter : Output data compression format, snappy or deflate")
    }

    parser.parse(args, Config()) map { config =>
      // hdfs_mapping_file=/user/ops_eff/lido/fsum/schema/fusm_mapping.csv
      val indexes = toMappingFormat(config.hdfs_mapping_file)
      //hdfs_input_path = /data/helix/raw/lido/flight_summary/incremental/1.0/2017-11-06/05-15/*.snappy
      val json_tbl = loadRawMsg(config.hdfs_input_path)
      val map_data_input = msgParser(json_tbl, indexes)
      //hdfs_input_path = /data/helix/raw/lido/flight_summary/incremental/1.0/2017-11-06/05-15/*.snappy
      val status_msg = jsonMsgFormatSnappyAvroStore(json_tbl, map_data_input, config.hdfs_output_path)
      println(status_msg)
    }
  }
}

