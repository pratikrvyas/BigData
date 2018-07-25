package com.emirates.flight_genome

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object AGSAdditionsApp extends AGSAdditionsTransformers {

  /**
    * This is a Spark application that takes AGS Actuals data in the Decomposed layer and adds
    * additional columns (WindComponentAGS, WindComponentNAV, GroundSpeed, AirDist, AccAirDist)
    * Created: 31-12-2017
    * Author: Vera Ekimenko
    * Location: Dubai, Burj Al Salam, 20th Floor, OpsEff Team (HELIX)
    */
  @transient lazy val conf = new SparkConf().setAppName("AGSAdditionsApp")
  conf.set("spark.hadoop.mapred.output.compress", "true")
  conf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.snappy")
  conf.set("spark.eventLog.enabled", "true")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrationRequired", "true")
  conf.registerKryoClasses(Array(
    classOf[Array[String]],
    classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
    classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
    classOf[Array[Object]],
    classOf[org.apache.spark.unsafe.types.UTF8String],
    classOf[com.emirates.flight_genome.AGSAdditionsTransformers]
  ))
  @transient implicit lazy val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  @transient implicit lazy val sqlContext: HiveContext = new HiveContext(sc)


  /**
    * This is the main method taking the parameters and calling all functions in the logical sequence.
    *
    * @param args pathToReadCSVFiles pathToWriteRefline pathToWriteRoutePoints numberOfPartitions (optional)
    */
  def main(args: Array[String]): Unit = {

    assert(2 to 3 contains args.size, "Incorrect number of parameters: " + args.size + " (expected at least 2)\n" +
      "Expected parameters (by position): \n" +
      "  1. <hdfs_path_to_folder_to_read_input_avro_files> \n" +
      "  2. <hdfs_path_to_folder_to_write_output_avro_files> \n" +
      "  3. <number_of_partitions_for_output_repartitioning>(optional) \n")

    val in_path = args(0).trim
    val out_path = args(1).trim
    val parts = try {
      args(2).trim.toInt
    } catch {
      case _: Exception => 0
    }

    println()

    val df = getDF(in_path)
        .drop("HELIX_UUID")
        .drop("HELIX_TIMESTAMP")
        .dropDuplicates()
      .withAltitude
      .withLatitude
      .withLongitude
      .withWindDirectionAmmended
      .withWindComponentAGS
      .withWindComponentNAV
      .withGroundSpeed
      .withWindowCalculations
      .drop("DistancePhases")
      .drop("SequenceNumber")
      .drop("TrackDistanceAboveZero")
      .drop("FirstRN")
      .drop("InitialTrackDistance")
      .drop("PreviousTrackDistance")
      .drop("InitialTrackDistanceRN")
      .drop("InitRN")
      .drop("PreviousTimestamp")
      .drop("InitTD")
      .drop("TimeDiff")
      .save(parts, out_path)

  }

}