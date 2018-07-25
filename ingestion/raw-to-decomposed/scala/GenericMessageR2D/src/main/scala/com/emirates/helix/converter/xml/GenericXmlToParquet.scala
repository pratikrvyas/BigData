
package com.emirates.helix.converter.xml

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.udf

// Sequence file reader import

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.conf.Configuration

// Spark-xml library  file import

import com.databricks.spark.xml.XmlInputFormat
import com.databricks.spark.xml.XmlReader

import org.apache.log4j.LogManager
import scala.util.{ Try, Failure, Success }
import org.apache.spark.util.SizeEstimator

/**
 * @author s759044
 *
 * The Message converter is xml to specified outputFormat converter application.  For now,
 * it is a simple minimal viable product:
 *   - Read in sequence files from a input HDFS directory.
 *   - It converts the message json string into a dataframe.
 *   - Then it picks up the message field and replace the namespace and \n characters.
 *   - Then it converts the XML strings to dataframe using spark-xml library.
 * Now it dosen't support conversion of xml to avro files with duplicate field names inside
 * the Same schema. It will support it in near future.
 *
 * Modify the command line flags to the values of your choosing.
 * ${NOOFPARTITIONS} is either 0 or no of partitions you want based on the data size.
 * ${NAMESPACE} is optional.Its based on the message.
 * ${XML_SCHEMA_FILE} is a full fledged xml with all the values as string.
 * Notice how they come after you specify the jar when using spark-submit.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.emirates.helix.converter.xml.MessageConverter"
 *     --master yarn \
 *     --jars SPARK_XML_JAR_PATH \
 *     /home/sarojk/conf/xml_avro/helix-message-converter-0.0.1 ${INPUT_HDFS_PATH} \
 *     ${XML_SCHEMA_FILE} \
 *     ${XML_ROOT_ELEMENT} \
 *     ${WRITE_MODE} \
 *     ${OUTPUT_HDFS_PATH} \
 *     ${OUTPUT_FILE_FORMAT} \
 *     ${COMPRESSION_CODEC} \
 *     ${NOOFPARTITIONS} \
 *     ${NAMESPACE} optional parameter\
 */

object GenericXmlToParquet extends App {

  if (args.length < 8) {
    throw new IllegalArgumentException("""All eight parameters - [inputPath] [schemaFile] [rootElement] [writeMode] [outputPath] [outputFormat] [compression] [noOfPartitions] are mandatory.
	          [NameSpace] is optional""")
    System.exit(1)
  }

  // mandatory parameters

  // input parameters
  private val inputPath = args(0)
  private val schemaFile = args(1)
  private val rootElement = args(2)

  // output parameters
  private val writeMode = args(3)
  private val outputPath = args(4)
  private val outputFormat = args(5)
  private val compression = args(6)
  private val noOfPartitions = args(7)

  //Optional parameters
  private var nameSpace: Option[String] = None

  if (args.size == 9) {
    nameSpace = Some(args(8))
  }

  val logger = LogManager.getRootLogger

  // set app name based on message type

  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  sqlContext.setConf("spark.sql.parquet.compression.codec", compression)
  sqlContext.setConf("spark.sql.avro.compression.codec", compression)

  logger.info("Message conversion started")

  logger.info("STEP:1 -- Converting message sequence files to dataframe")

  val rdd_json: RDD[String] = readFile(inputPath, sc)

  logger.info("STEP:1 -- completed")

  logger.info("STEP:2 -- Replacing the namespace and \\n character from message column")

  val rdd_json_str: RDD[String] = if (nameSpace isDefined) {
    rdd_json.map(p => p.toString.replace("ns0:", "").replace(":ns0", ""))
  } else {
    rdd_json
  }

  val df_json = sqlContext.read.json(rdd_json_str)

  logger.info("STEP:2 -- completed")

  logger.info("STEP:3 -- Converting dataframe to RDD of string.")

  val rdd_xml_str: RDD[String] = df_json.select("message").map { case Row(a: String) => (a) }

  logger.info("STEP:3 -- completed")

  logger.info("STEP:4 -- Generating the schema from schema xml file")

  val df_schema = sqlContext.read.format("xml").option("rowTag", rootElement).option("attributePrefix", "_").option("valueTag", "_ATTRIBUTE_VALUE").load(schemaFile)

  logger.info("Print spark schema of the output file.")

  df_schema.printSchema

  val stringSchema = df_schema.schema

  logger.info("STEP:4 -- completed")

  logger.info("STEP:5 -- Convert the rdd to dataframe using target schema")

  // udf to generate UUID for every row.
  val generateUUID = udf(() => java.util.UUID.randomUUID.toString)

  val df_xml = Try { new XmlReader().withRowTag(rootElement).withAttributePrefix("_").withValueTag("_ATTRIBUTE_VALUE").withSchema(stringSchema).xmlRdd(sqlContext, rdd_xml_str) }

  df_xml match {
    case Failure(df) =>
      logger.error("ERROR 1001 : Error during xml to dataframe conversion", df.getCause)
      System.exit(1)
    case Success(df) =>
      logger.info("STEP:5 -- XML to dataframe conversion completed")
      logger.info("STEP:6 -- Generate uuid,timestamp and store the dataframe into output hdfs path")
      val noOfPartitionInt = noOfPartitions.toInt
      if (noOfPartitionInt == 0) {
        val estpartitionCount: Int = ((((SizeEstimator.estimate(df) / 1024) / 1024) / 100) + 1).toInt
        df.withColumn("HELIX_UUID", generateUUID()).withColumn("HELIX_TIMESTAMP", unix_timestamp().cast("String")).coalesce(estpartitionCount).write.format(outputFormat).mode(writeMode).save(outputPath)
      } else {
        df.withColumn("HELIX_UUID", generateUUID()).withColumn("HELIX_TIMESTAMP", unix_timestamp().cast("String")).coalesce(noOfPartitionInt).write.format(outputFormat).mode(writeMode).save(outputPath)
      }
      logger.info("STEP:6 -- completed")
  }

  logger.info("Converted the messages to " + outputFormat)

  logger.info("=================================================================")
  logger.info("              RESULTS STORED SUCCESSFULLY !                      ")
  logger.info("=================================================================")

  /**
   * This function will convert array of bytes to string
   *
   * @param Array of bytes
   * @return String
   */
  def b2s(a: Array[Byte]): String = new String(a)

  /**
   * This function will read sequence file from HDFS
   * and returns value
   *
   * @param path The HDFS input path
   * @param sc The Spark Context
   * @return Value from Sequence file
   */
  def readFile(path: String, sc: SparkContext): RDD[String] = {
    val file = sc.sequenceFile(path, classOf[LongWritable], classOf[BytesWritable])
    val rdd_bytes = file.map { case (k, v) => (k, v.copyBytes) }
    rdd_bytes.map { case (k, v) => b2s(v) }
  }

}