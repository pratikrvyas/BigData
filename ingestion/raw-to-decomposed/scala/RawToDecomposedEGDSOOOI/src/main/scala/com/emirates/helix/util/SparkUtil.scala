/*----------------------------------------------------------
 * Created on  : 21/12/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : SparkUtil.scala
 * Description : Class file for utility functions
 * ----------------------------------------------------------
 */
package com.emirates.helix.util

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.log4j.Logger

/**
  * Spark utility class with methods to read supporting files
  */
object SparkUtil {

  private lazy val logger:Logger = Logger.getLogger(SparkUtil.getClass)

  /** Utility function to read avro schema file and generate schema object
    *
    *  @param url HDFS url (name node)
    *  @param path HDFS path for schema file
    *  @return Option[Schema] Schema object in case of success or else None
    */
  def schemaReader(url: String, path: String):  Option[Schema]= {
    val hdpConf = new Configuration()
    var file:FSDataInputStream = null
    hdpConf.set("fs.defaultFS",url)
    try {
      file = FileSystem.get(hdpConf).open(new Path(path))
      val schema = new Schema.Parser().parse(file)
      Some(schema)
    } catch {
      case e: Exception => {
        logger.error("Error while reading avro schema file: "+ url+path)
        logger.error(e.getMessage)
        None
      }
    }
    finally {
      if(null != file) {
        file.close()
      }
    }
  }
}
