package com.emirates.helix.converter.udf

import java.util.UUID
import java.io.Serializable

import org.apache.spark.sql.functions.udf

import scala.xml.{Elem, Node}
import scala.xml.XML.loadString
import scala.xml.transform.{RewriteRule, RuleTransformer}

object UDF extends Serializable {

  /**
    * UDF function for generating unique ID
    *
    */
  val generateUUID =  udf(() => UUID.randomUUID().toString)

  /**
    * UDF function for adding tibco timestamp to the xml message
    *
    */
  val addTibcotime = udf((message: String, tibcotime: String) => {
    val addRule = new RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case elem: Elem if elem.label == "BothFlightEvent" =>
          elem.copy(child = (elem.child ++ <tibco_messageTime>{tibcotime}</tibco_messageTime>))
        case n => n
      }
    }
    try {
      val transformer = new RuleTransformer(addRule)
      transformer(loadString(message)).toString()
    }
    catch {
      case e:Exception => null.asInstanceOf[String]
    }
  })
}
