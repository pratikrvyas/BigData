package com.emirates.cartrawlerrawtodecomp.utils

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import java.net.URI
import java.text.SimpleDateFormat

trait sparkUtils {
  /**
   * Creates a Spark Configuration object
   *
   * @param appName The name of the spark Application
   */
  def sparkConfig(appName: String): SparkConf = {
    val conf = new SparkConf().setAppName(appName)
      .set("spark.hadoop.mapreduce.output.fileoutputformat.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.broadcast.compress", "true")
      .set("spark.core.connection.ack.wait.timeout", "600")
      .set("spark.akka.frameSize", "512")
      .set("spark.akka.threads", "10")
      .set("spark.eventLog.enabled", "true")
      .set("spark.io.compression.codec", "snappy")
    conf
  }

  /**
   * @param sqlContext The SQLContext to configure
   */
  def getSQLContext(sc: SparkContext): SQLContext = {

    val sqlContext = new SQLContext(sc);
    setSqlContextConfig(sqlContext)
    sqlContext
  }

  /**
   * @param sqlContext The SQLContext to configure
   */
  def getHiveContext(sc: SparkContext): HiveContext = {

    val sqlContext = new HiveContext(sc)
    setSqlContextConfig(sqlContext)
    sqlContext
  }

  /**
   * Decorates a SQLContext with parameters
   */
  def setSqlContextConfig(sqlContext: SQLContext) {

    // Turn on parquet filter push-down,stats filtering, and dictionary filtering
    sqlContext.setConf("parquet.filter.statistics.enabled", "true")
    sqlContext.setConf("parquet.filter.dictionary.enabled", "true")
    sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")

    // Use non-hive read path
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "true")

    // Turn off schema merging
    sqlContext.setConf("spark.sql.parquet.mergeSchema", "false")
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet.mergeSchema", "false")

    // Set parquet compression
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    // Configure avro parameters
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
  }

  /**
   * Selects a subset of columns from the existing Dataframe
   */
  def select(df: DataFrame, sqlContext: SQLContext, colList: Seq[String]): DataFrame = {
    import sqlContext.implicits._
    val keys = colList.map(l => $"$l")
    df.select(keys: _*)
  }
  
  def groupAndAggregate(df: DataFrame,  aggregateFun: Map[String, String], cols: List[String] ): DataFrame ={
  val grouped = df.groupBy(cols.head, cols.tail: _*)
  val aggregated = grouped.agg(aggregateFun)
  aggregated
}
   def getDateTimeSnap(basepath: String, processdt: String, Days: Int, timeSnap: Boolean, sc: SparkContext): (String, String) = {

    val previousmap = scala.collection.mutable.Map[String, Long]();
    val latestmap = scala.collection.mutable.Map[String, Long]();
    var bFileNotFound: Boolean = true;
    var NoOfDays = Days;
    //val basepath = "/data/helix/modelled/emirates_genome/fos/ff_daily_fares";
    //val path = basepath + "/snapdt=" + processdt;
    val path = basepath + processdt;
    var newPath = "";
    var previousFolder = "";
    var latestFolder = "";

    var fs: FileSystem = null;
    var fileDateStatus: Array[FileStatus] = null;

    while (bFileNotFound && NoOfDays < 400) {
      //Get the new date
      val getXDate = addDays(processdt, -1 * NoOfDays);
      println("NewDate " + getXDate);
      println("NoOfDays " + NoOfDays.toString());
      //newPath = basepath + "/snapdt=" + getXDate
      newPath = basepath + getXDate

      //Check the folder exists in HDFS
      fs = FileSystem.get(new URI(newPath), sc.hadoopConfiguration);
      if (fs.exists(new Path(newPath))) {
        if (timeSnap) {
          fileDateStatus = fs.listStatus(new Path(newPath));
          if (fileDateStatus.size > 0) {
            //Get all the snap time folders in snap date folder
            for (j <- 0 until fileDateStatus.size) {
              //println(fileDateStatus(j).getPath + "-" + fileDateStatus(j).getModificationTime.toString())
             previousmap.put(fileDateStatus(j).getPath.toString(), fileDateStatus(j).getModificationTime);
            }
            bFileNotFound = false;
          } else {
            NoOfDays += 1;
          }
        } else {
          previousmap.put(newPath, 1);
          bFileNotFound = false;
        }
      } else {
        println(newPath);
        NoOfDays += 1;
      }
    }
    if (previousmap.size > 0) {
      val lstMap = previousmap.toList.sortWith((x, y) => x._2 > y._2);
      val hdPrevious = lstMap.head;
      previousFolder = hdPrevious._1;
    }
    //Get the latest version from HDFS
    //newPath = basepath + "/snapdt=" + processdt;
    newPath = basepath + processdt;
    //Check the folder exists in HDFS
    fs = FileSystem.get(new URI(newPath), sc.hadoopConfiguration);
    if (fs.exists(new Path(newPath))) {
      if (timeSnap) {
        fileDateStatus = fs.listStatus(new Path(newPath));
        if (fileDateStatus.size > 0) {
          //Get all the snap time folders in snap date folder
          for (j <- 0 until fileDateStatus.size) {
            //println(fileDateStatus(j).getPath + "-" + fileDateStatus(j).getModificationTime.toString())
            latestmap.put(fileDateStatus(j).getPath.toString(), fileDateStatus(j).getModificationTime);
          }
        }
      } else {
        latestmap.put(newPath, 1);
      }
    }
    if (latestmap.size > 0) {
      val lstLatestMap = latestmap.toList.sortWith((x, y) => x._2 > y._2);
      val hdLatest = lstLatestMap.head;
      latestFolder = hdLatest._1;
    }
    (previousFolder, latestFolder);
  }
   
   def addDays(pDate: String, pPeriod: Int): String = {
    try {
      val format = new SimpleDateFormat("yyyy-MM-dd");
      val c = java.util.Calendar.getInstance()
      c.setTime(format.parse(pDate));
      c.add(java.util.Calendar.DATE, pPeriod)
      val output = format.format(c.getTime())
      output.toString
    } catch {
      case e: Exception =>
        println("exception caught: " + e.getMessage);
        return "";
    }
  };
    
  
   def getLatestPartitionPath(path: String, sc: SparkContext): String = {
    val date = extractTimeComponent(path, sc.hadoopConfiguration)
    //val dtPath = s"${path}/snapdt=${date}"
    val dtPath = s"${path}/${date}"
    val time = extractTimeComponent(dtPath, sc.hadoopConfiguration)
    //val timePath = s"${dtPath}/snaptime=${time}"
    val timePath = s"${dtPath}/${time}"
    timePath
  }
   private def extractTimeComponent(path: String, hadoopConfiguration: Configuration): String = {
    val map = scala.collection.mutable.Map[String, Long]()
    val fs = FileSystem.get(new URI(path), hadoopConfiguration)
    val status = fs.listStatus(new Path(path))
    status.foreach(x => if (x.isDirectory()) (map(x.getPath.toString()) = x.getModificationTime))
    val lstMap = map.toList.sortWith((x, y) => x._2 > y._2)
    val hd = lstMap.head
    val key = hd._1
    val splitStr = key.split("/").last
    splitStr.split("=").last
  }
   
   
}