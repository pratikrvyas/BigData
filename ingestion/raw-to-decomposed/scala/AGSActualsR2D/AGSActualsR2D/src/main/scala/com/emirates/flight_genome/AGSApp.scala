package com.emirates.flight_genome

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object AGSApp extends AGSTransformers {

  /**
    * This is a Spark application that takes AGS Actuals data in the Raw layer (*.csv.gz files), splits them into
    * 2 tables with additional columns (flight primary keys, file names and HELIX identities)
    * Created: 24-12-2017
    * Author: Vera Ekimenko
    * Location: Dubai, Burj Al Salam, 20th Floor, OpsEff Team (HELIX)
    */
  @transient lazy val conf = new SparkConf().setAppName("AGSActualsR2D")
  conf.set("spark.hadoop.mapred.output.compress", "true")
  conf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.snappy")
  conf.set("spark.eventLog.enabled", "true")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(
    classOf[Array[String]],
    classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
    classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
    classOf[Array[Object]],
    classOf[org.apache.spark.unsafe.types.UTF8String]
  ))
  @transient implicit lazy val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  @transient implicit lazy val sqlContext:HiveContext = new HiveContext(sc)


  /**
    * This is the main method taking the parameters and calling all functions in the logical sequence.
    * @param args pathToReadCSVFiles pathToWriteRefline pathToWriteRoutePoints numberOfPartitions (optional)
    */
  def main(args: Array[String]): Unit = {

    assert(3 to 4 contains args.size, "Incorrect number of parameters: " + args.size + " (expected at least 3)")

    val in_path = args(0).trim
    val out_path_rl = args(1).trim
    val out_path_rp = args(2).trim
    val parts = try {
      args(3).trim.toInt
    } catch {
      case _:Exception => 0
    }

      val (rl,rp) = loadFiles(listFiles(in_path))
      saveDF(rl,parts,out_path_rl)
      saveDF(rp,parts,out_path_rp)

    }

}
