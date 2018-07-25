package com.emirates.helix.converter.xml

import org.apache.avro.Schema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration
import java.io.FileNotFoundException

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, AtomicType, ArrayType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.udf

//----------------xml_loader_import
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import com.databricks.spark.xml.XmlReader
import com.databricks.spark.xml.XmlInputFormat
import org.apache.log4j.LogManager
import scala.util.{ Try, Failure, Success }
import org.apache.spark.util.SizeEstimator

/**
 * @author s759044
 *
 * The Message converter is xml to specified outputFormat converter application.  For now,
 * it is a simple minimal viable product:
 *   - Read in sequence files from a input HDFS directory.
 *   - Then it replace the namespace and \n characters.
 *   - Then it converts the XML strings to dataframe using spark-xml library.
 * Now it dosen't support conversion of xml to avro files with duplicate field names inside
 * the Same schema. It will support it in near future.
 *
 * Modify the command line flags to the values of your choosing.
 * ${NAMESPACE} is optional.Its based on the message.
 * Notice how they come after you specify the jar when using spark-submit.
 *
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.emirates.helix.converter.xml.GenericXmlConverter"
 *     --master yarn \
 *     --jars /home/sarojk/conf/xml_avro/spark-xml_2.10-0.3.5.jar \
 *     /home/sarojk/conf/xml_avro/helix-message-converter-0.0.1 ${INPUT_HDFS_PATH} \
 *     ${XML_ROOT_ELEMENT} \
 *     ${OUTPUT_HDFS_PATH} \
 *     ${WRITE_MODE} \
 *     ${OUTPUT_FILE_FORMAT} \
 *     ${COMPRESSION_CODEC} \
 *     ${NAMESPACE} optional parameter\
 */

object SchemaXmlFileToAvro extends App {

  if (args.size < 6) {
    throw new IllegalArgumentException("""All six parameters -
      [inputPath] [rootElement] [outputPath] [writeMode] [outputFormat] [compression] are mandatory.
      [NameSpace] is optional""")
  }

  //Mandatory parameters

  private val inputPath = args(0)
  private val rootElement = args(1)
  private val outputPath = args(2)
  private val writeMode = args(3)
  private val outputFormat = args(4)
  private val compression = args(5)

  //Optional parameters
  private var nameSpace: Option[String] = None

  if (args.size == 7) {
    nameSpace = Some(args(6))
  }

  val logger = LogManager.getRootLogger

  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  sqlContext.setConf("spark.sql.parquet.compression.codec", compression)

  if (nameSpace isDefined) {
    val start_tag_key = "<" + nameSpace.getOrElse("ns0") + ":" + rootElement + ">"
    println("start_tag_key : " + start_tag_key)
    val end_tag_key = "<" + "/" + nameSpace.getOrElse("ns0") + ":" + rootElement + ">"
    println("end_tag_key : " + end_tag_key)
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, start_tag_key)
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, end_tag_key)
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

    val rdd_xml = sc.newAPIHadoopFile(
      inputPath,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text])

    logger.info("Replacing the namespace and \\n character if passed as argument")

    def b2s(a: Array[Byte]): String = new String(a)
    val rdd_bytes = rdd_xml.map { case (k, v) => (k, v.copyBytes) }
    val rdd_xml_str = rdd_bytes.map { case (k, v) => b2s(v) }.map(p => p.toString.replace("ns0:", "").replace(":ns0", ""))

    val df_xml = new XmlReader().withRowTag(rootElement).withAttributePrefix("_").withValueTag("_ATTRIBUTE_VALUE").xmlRdd(sqlContext, rdd_xml_str)
    df_xml.write.format(outputFormat).mode(writeMode).save(outputPath)

  } else {
    val df_xml = new XmlReader().withRowTag(rootElement).withAttributePrefix("_").withValueTag("_ATTRIBUTE_VALUE").xmlFile(sqlContext, inputPath)
    df_xml.write.format(outputFormat).mode(writeMode).save(outputPath)
  }

  logger.info("=================================================================")
  logger.info("              RESULTS STORED SUCCESSFULLY !                      ")
  logger.info("=================================================================")

}