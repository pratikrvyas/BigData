
/*
*=====================================================================================================================================
* Created on     :   14/11/2017
* Author         :   Ravindra Chellubani
* Version        :   1.0
* Project        :   Helix=OpsEfficnecy
* Filename       :   FixedLengthNDataToAvro_Weather.scala
* Description    :   Spark application to extract fixed length data with N repetition from  JSON sequence feeds and write to decomposed as avro with compression
* ======================================================================================================================================
*/


//spark-submit --master yarn  --driver-memory 5g --total-executor-cores 10 --executor-cores 10 --executor-memory 10g  --queue opseff --conf "spark.kryoserializer.buffer.max=1023mb" --conf "spark.driver.maxResultSize=3g" --class "helix.emirates.com.FixedLengthNDataToAvro_Weather" /home/ops_eff/ek_lido/FixedLengthNDataToAvro_Weather-1.0.jar -i /data/helix/raw/lido/weather/incremental/1.0/2017-11-06/05-15 -o /data/helix/decomposed/lido/weather/incremental/1.0/2017-11-06/05-15 -s /apps/helix/conf/avro/decomposed/lido/weather/incremental/2017-11-13/weather_mapping.csv -c snappy


package helix.emirates.com

import scala.util.control._
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._



object FixedLengthNDataToAvro_Weather {

  private val compress_rex = """^snappy$|^deflate$""".r
  val conf = new SparkConf().setAppName("FixedLengthNDataToAvro_Weather")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  sc.setLogLevel("ERROR")

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

  //FORMATTING TO JSON OBJECT FORMAT, ADDING UUID , TIMESTAMP AND STORING TO HDFS
  def jsonMsgFormatSnappyAvroStore(map_data_input : ArrayBuffer[scala.collection.mutable.Map[String,Any]], hdfs_ouput_path : String) = {

    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.native.JsonMethods._
    import org.json4s.jackson.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)
    import com.databricks.spark.avro._

    //FORMATTING TO JSON OBJECT
    var map_data = map_data_input

    var no_of_partitions = (map_data_input.toString.length*8*2/(1000*1000*100))*100
    println("[INFO] No of Dynamic Partitions : "+no_of_partitions)

    if(no_of_partitions < 10){
      no_of_partitions = 10
    }

    val json_data_Rdd = sc.parallelize(for(x <- 0 to map_data.size - 1) yield Serialization.write(map_data(x)),no_of_partitions)

   // println("working -1 : "+json_data_Rdd.partitions.size)
    val json_data_df = sqlContext.read.json(json_data_Rdd)

    //ADDING UUID AND TIMESTAMP
    import java.util.UUID
    val generateUUID = udf(() => UUID.randomUUID().toString)
    val data_with_genUUID_df = json_data_df.withColumn("helix_uuid",generateUUID())
    val data_with_genTimestamp_df = data_with_genUUID_df.withColumn("helix_timestamp",current_timestamp())

    //WRITING DATA TO DECOMPOSED IN AVRO FORMAT WITH SNAPPY COMPRESSION
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
    data_with_genTimestamp_df.write.mode("append").avro(hdfs_ouput_path)

  }


  def msgParser(json_tbl : org.apache.spark.sql.DataFrame, indexes : collection.mutable.LinkedHashMap[String, List[Any]],hdfs_ouput_path : String) {
    var dataList = ArrayBuffer[scala.collection.mutable.Map[String,Any]]()
    var strtpoint = 0
    val loop = new Breaks
    var msgcnt = 0
    var actual_msg_cnt = json_tbl.count
    println("[INFO] Actual Message Count: "+actual_msg_cnt)

    val msgs = json_tbl.collect.foreach(msg => if(!msg.mkString.isEmpty())
    {
      var data = collection.mutable.LinkedHashMap[String, Any]()
      var noofMsgEndlen = 0
      var subsubindex_i = 0
      val eachTibcoDateTime = msg(1)
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
        var subDataList : List[scala.collection.mutable.LinkedHashMap[String,Any]] = Nil
        for( nmsgs <- 1 to noofMsgs){
          var subData = collection.mutable.LinkedHashMap[String, Any]()
          loop.breakable {
            for( j <- 3 until eachIndexList._2.size by 2){
              if(eachIndexList._2(j+1).toString.trim matches """\d+"""){
                subData += (eachIndexList._2(j).toString -> eachMsg.substring(strtpoint,(strtpoint+eachIndexList._2(j+1).toString.trim.toInt)))
                strtpoint=strtpoint+eachIndexList._2(j+1).toString.trim.toInt
              } else {
                val rsep = eachIndexList._2(j+1).toString.trim
                val rmat = s"\\Q$rsep\\E".r
                //println("regex "+rmat)
                val subsubindex = rmat.findAllMatchIn(eachMsg).map(_.start).toList
                //println("subsubindex :"+subsubindex)
                if(subsubindex_i < subsubindex.size){
                  //println("subData += (eachIndexList._2(j).toString ->  eachMsg.substring("+strtpoint+","+(subsubindex(subsubindex_i)+1)+"))")
                  subData += (eachIndexList._2(j).toString ->  eachMsg.substring(strtpoint,subsubindex(subsubindex_i)+1))
                  //println("("+strtpoint+","+subsubindex(subsubindex_i)+")")
                  //println("subData :"+ subData)
                  //println("subsubindex_i :"+ subsubindex_i)
                  strtpoint=subsubindex(subsubindex_i)+1
                  subsubindex_i = subsubindex_i + 1
                  loop.break
                }
              }
            }
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
        val eachMsg = msg.toString
        val noofMsgs = eachMsg.substring(noofMsgStrtInd,noofMsgStrtInd+noofMsgEndIndlen).trim.toInt
        var subDataList : List[scala.collection.mutable.LinkedHashMap[String,Any]] = Nil
        for( nmsgs <- 1 to noofMsgs){
          var subData = collection.mutable.LinkedHashMap[String, Any]()
          for( j <- 3 until eachIndexList._2.size by 2){
            subData += (eachIndexList._2(j).toString -> eachMsg.substring(strtpoint,(strtpoint+eachIndexList._2(j+1).toString.trim.toInt)))
            strtpoint=strtpoint+eachIndexList._2(j+1).toString.trim.toInt
          }
          subDataList = subDataList :+ subData
        }
        data += (eachIndexList._1 -> subDataList)
      }
      else{
        println("[ERROR] Syntax error in Mapping document - Please check")
      }
      )
      data += ("tibco_message_time" -> msg(1).toString)
      dataList += data
      msgcnt = msgcnt + 1
      if(msgcnt == 1000 && actual_msg_cnt > 1000 ){
        println("[INFO] Parsed No of Messages: "+dataList.size)
        val status_msg = jsonMsgFormatSnappyAvroStore(dataList,hdfs_ouput_path)
        msgcnt = 0
        actual_msg_cnt = actual_msg_cnt - 1000
        dataList.clear
      }else if(actual_msg_cnt == dataList.size){
        println("[INFO] Parsed No of Messages: "+dataList.size)
        val status_msg = jsonMsgFormatSnappyAvroStore(dataList,hdfs_ouput_path)
        msgcnt = 0
        dataList.clear
      }
    }
    )
  }

  def loadRawMsg(hdfs_input_path : String ) : org.apache.spark.sql.DataFrame = {
    val json_str= sc.sequenceFile[LongWritable, BytesWritable](hdfs_input_path).map(x => new String(x._2.copyBytes(), "utf-8"))
    val json_tbl = sqlContext.read.json(json_str).select("message","metadata.messageTime")
    return json_tbl
  }

  def main(args:Array[String]) : Unit = {

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

    def CountTestCase(json_tbl : org.apache.spark.sql.DataFrame,hdfs_output_path : String) : String = {

      import com.databricks.spark.avro._
      //Count Test
      val df = sqlContext.read.avro(hdfs_output_path+"/*.avro")
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

    parser.parse(args, Config()) map { config =>
      // hdfs_mapping_file=/user/ops_eff/lido/fsum/schema/fusm_mapping.csv
      val indexes = toMappingFormat(config.hdfs_mapping_file)
      //hdfs_input_path = /data/helix/raw/lido/flight_summary/incremental/1.0/2017-11-06/05-15/*.snappy
      val json_tbl = loadRawMsg(config.hdfs_input_path)
      //hdfs_output_path = /data/helix/decomposed/lido/weather/incremental/1.0/2017-11-06/05-15
      msgParser(json_tbl,indexes,config.hdfs_output_path)
      val status_msg = CountTestCase(json_tbl,config.hdfs_output_path)
      println(status_msg)
    }
  }
}

