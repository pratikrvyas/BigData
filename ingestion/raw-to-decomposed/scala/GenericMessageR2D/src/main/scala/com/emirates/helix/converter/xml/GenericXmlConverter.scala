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
import org.apache.spark.sql.types.{ArrayType, AtomicType, IntegerType, StringType, StructField, StructType}
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
import org.apache.log4j.LogManager
import scala.util.{ Try, Failure, Success }
import org.apache.spark.util.SizeEstimator
import com.emirates.helix.converter.udf.UDF._

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
 * ${AVRO_SCHEMA_FILE} can be generated from the full fledged xml using SchemaXmlFileToAvro converter.
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
 *     ${AVRO_SCHEMA_FILE} \
 *     ${HDFSURI} \
 *     ${OUTPUT_HDFS_PATH} \
 *     ${WRITE_MODE} \
 *     ${OUTPUT_FILE_FORMAT} \
 *     ${COMPRESSION_CODEC} \
 *     ${NOOFPARTITIONS} \
 *     ${NAMESPACE} optional parameter\
 */

object GenericXmlConverter extends App {
  if (args.size < 9) {
    throw new IllegalArgumentException("""All nine parameters -
      [inputPath] [rootElement] [avroSchemaFile] [hdfsUri] [outputPath] [writeMode] [outputFormat] [compression] [noOfPartitions] are mandatory.
      [NameSpace] is optional""")
  }

  //Mandatory parameters

  private val inputPath = args(0)
  private val rootElement = args(1)
  private val schemaFile = args(2)
  private val hdfsUri = args(3)
  private val outputPath = args(4)
  private val writeMode = args(5)
  private val outputFormat = args(6)
  private val compression = args(7)
  private val noOfPartitions = args(8)

  //Optional parameters
  private var nameSpace: Option[String] = None

  if (args.size == 10) {
    nameSpace = Some(args(9))
  }

  val logger = LogManager.getRootLogger

  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  sqlContext.setConf("spark.sql.parquet.compression.codec", compression)

  /**
   * This function will convert array of bytes to string
   *
   * @param a array of bytes
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

  // Perpeare spark struct type schema from avro schema

  logger.info("Read avro schema file from HDFS")

  val hdpConf = new Configuration
  hdpConf.set("fs.defaultFS", hdfsUri)
  val fs = FileSystem.get(hdpConf)
  val file = fs.open(new Path(schemaFile))
  val schema = new Schema.Parser().parse(file)

  logger.info("STEP:1 --Converting Avro schema to Spark Sql schema started")

  import com.emirates.helix.converter.common.SchemaConverters.toSqlType

  val schemaFields = schema.getFields()
  var arr = new Array[org.apache.spark.sql.types.StructField](schemaFields.size);
  var i = 0;
  import scala.collection.JavaConversions._
  for (field <- schemaFields) {
    val sField = new StructField(field.name, toSqlType(field.schema()).dataType);
    arr(i) = sField
    i = i + 1
  }
  val sType = new StructType(arr)

  println(sType)

  logger.info("STEP:1 --Converting Avro schema to Spark Sql schema completed")

  logger.info("Message conversion started")

  logger.info("STEP:2 -- Converting sequence file to rdd of json string")

  val rdd_json: RDD[String] = readFile(inputPath, sc)

  logger.info("STEP:2 -- Completed")

  logger.info("STEP:3 -- Replacing the namespace and \\n character if passed as argument")

  val rdd_json_str: RDD[String] = if (nameSpace isDefined) {
    rdd_json.map(p => p.toString.replace("ns0:", "").replace(":ns0", ""))
  } else {
    rdd_json
  }

  logger.info("STEP:3 -- completed")

  logger.info("STEP:4 -- Convert rdd of json string to dataframe")

  val df_json = sqlContext.read.json(rdd_json_str)

  logger.info("STEP:4 -- completed")

  logger.info("STEP:5 -- Converting dataframe to RDD of string.")

  val rdd_xml_str: RDD[String] = df_json.select(addTibcotime($"message",$"metadata.messageTime")).map { case Row(a: String) => (a) }

  logger.info("STEP:5 -- completed")


  logger.info("STEP: 6 -- Converting XMLs to " + outputFormat + " with " + " compression " + compression)

  val df_xml = Try { new XmlReader().withRowTag(rootElement).withAttributePrefix("_").withValueTag("_ATTRIBUTE_VALUE").withSchema(sType).xmlRdd(sqlContext, rdd_xml_str) }
  df_xml match {
    case Failure(df) =>
      logger.error("Error during xml to dataframe conversion", df.getCause)
      System.exit(1)
    case Success(df) =>
      logger.info("STEP:6 -- XML to dataframe conversion completed")
      logger.info("STEP:7 -- Generate uuid,timestamp and store the dataframe into output hdfs path")
      val noOfPartitionInt = noOfPartitions.toInt
      if (noOfPartitionInt == 0) {
        val estpartitionCount: Int = ((((SizeEstimator.estimate(df) / 1024) / 1024) / 100) + 1).toInt
        df.withColumn("HELIX_UUID", generateUUID()).withColumn("HELIX_TIMESTAMP", unix_timestamp().cast("String")).coalesce(estpartitionCount).write.format(outputFormat).mode(writeMode).save(outputPath)
      } else {
        df.withColumn("HELIX_UUID", generateUUID()).withColumn("HELIX_TIMESTAMP", unix_timestamp().cast("String")).coalesce(noOfPartitionInt).write.format(outputFormat).mode(writeMode).save(outputPath)
      }
      logger.info("STEP:7 -- completed")
  }

  file.close()

  logger.info("=================================================================")
  logger.info("              RESULTS STORED SUCCESSFULLY !                      ")
  logger.info("=================================================================")

}