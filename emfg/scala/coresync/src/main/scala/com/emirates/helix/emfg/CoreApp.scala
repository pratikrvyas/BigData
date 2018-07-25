/*----------------------------------------------------------
* Created on : 21/01/2018
* Author : Vera Ekimenko (S794642)
* Email : vera.ekimenko@dnata.com
* Version : 1.0
* Project : Helix-OpsEfficnecy
* Filename : CoreApp.scala
* Description : Scala application with main method to save Historical CORE data to Incremental schema
* ----------------------------------------------------------
*/

package com.emirates.helix.emfg

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.reflect.ClassTag
import java.util.HashMap

object CoreApp extends CoreTransformers {

  @transient lazy val conf = new SparkConf().setAppName("CoreASIS")
  conf.set("spark.eventLog.enabled", "true")
  conf.set("spark.rdd.compress", "true")
  conf.set("spark.broadcast.compress", "true")
  conf.set("spark.core.connection.ack.wait.timeout", "600")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(
    classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
    classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
    classOf[Array[Object]],
    classOf[org.apache.spark.unsafe.types.UTF8String],
    classOf[org.apache.spark.sql.catalyst.util.GenericArrayData],
    classOf[Array[org.apache.spark.sql.Row]],
    classOf[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema],
    classOf[com.emirates.helix.emfg.CoreTransformers],
    classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
    Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
    classOf[java.util.HashMap[_,_]],
    Class.forName("scala.reflect.ClassTag$$anon$1"),
    Class.forName("java.lang.Class"),
    Class.forName("scala.collection.mutable.WrappedArray$ofRef"),
    classOf[Array[String]],
    Class.forName("org.apache.spark.sql.sources.HadoopFsRelation$FakeFileStatus"),
    ClassTag(Class.forName("org.apache.spark.sql.sources.HadoopFsRelation$FakeFileStatus")).wrap.runtimeClass,
    classOf[Array[_]]
  ))
  @transient lazy implicit val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  @transient lazy implicit val sqlContext:HiveContext = new HiveContext(sc)


  /**
    * This is the main method taking the parameters and calling all functions in the logical sequence.
    * @param args
    */
  def main(args: Array[String]): Unit = {

    assert(4 to 5 contains args.size, "Incorrect number of parameters: " + args.size + " (expected at least 4)." +
      "\r\nUsage: <ODS Fligth Info path> <ODS MVTTypes path> <ODS Delay path> <Output path> <Number of partitions (optional)>")

    val fi_path = args(0).trim() + "/*.avro"
    val mvt_path = args(1).trim() + "/*.avro"
    val del_path = args(2).trim() + "/*.avro"

    val out_path = args(3).trim()

    val parts = try {
      args(4).trim.toInt
    } catch {
      case _:Exception => 0
    }

    saveDF(
      sync(
        loadDF(fi_path).withProperTime.withProperDate.withTZ.withPK,
        loadDF(mvt_path).withMVTTZ.withMvtDateTime,
        loadDF(del_path).withDelays
      ).withHELIX,
      parts,
      out_path
    )

  }


}