package com.emirates.flight_genome

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object SparkSupport {

  private val conf =
    new SparkConf()
//      .set("spark.sql.warehouse.dir", "file:///C:/tools/IdeaProjects/Core/spark-warehouse")
      .set("spark.local.dir", "/tmp/spark-temp")
      .setMaster("local[2]")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")
      .setAppName("CoreAppTest")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  @transient lazy val sqlContext: SQLContext = new SQLContext(sc)
  @transient lazy val hiveContext: HiveContext = new HiveContext(sc)

}

trait SparkSupport {

  implicit def sc: SparkContext = SparkSupport.sc
  implicit def sqlContext: SQLContext = SparkSupport.sqlContext
  implicit def hiveContext: HiveContext = SparkSupport.hiveContext

  def waitFor[T](future: Future[T], duration: Duration = 5.second): T =
    Await.result(future, duration)
}
