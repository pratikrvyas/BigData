package com.emirates.flight_genome

import java.io.FileNotFoundException
import org.scalatest._
import org.apache.spark.sql.Row

class AGSAppTest extends FlatSpec with Matchers with SparkSupport {

  "Return correct CSV files: test" should "pass" in {
    val ags = new AGSTransformers
//    val list = ags.listFiles("file:///C:/Users/s794642/Documents/AGS/input")
    val list = ags.listFiles("file:////home/verae/AGS/src/test/resources")
    val paths = list.map(t => t.path)
    paths should contain allOf(
      "file:/home/verae/AGS/src/test/resources/1591365.CSV.gz",
      "file:/home/verae/AGS/src/test/resources/1591381.CSV.gz",
      "file:/home/verae/AGS/src/test/resources/1591379.CSV.gz"
    )

  }

  "Dodgy path should return error: test" should "pass" in {
    val ags = new AGSTransformers
    an [FileNotFoundException] should be thrownBy ags.listFiles("path")

  }

  "No files should return nothing: test" should "pass" in {
    val ags = new AGSTransformers
//    val list = ags.listFiles("file:///C:/Users/s794642/Documents/AGS/out")
    val list = ags.listFiles("file:////home/verae/AGS/src/main")
    list shouldBe (List())

  }

  "Correct csvFile returns correct DataFrame: test" should "pass" in {
    val ags = new AGSTransformers
//    val csv = ags.csvFile("file:/C:/Users/s794642/Documents/AGS/input/1591365.CSV.gz","1591365.CSV.gz","input")
    val csv = SourceFile("file:////home/verae/AGS/src/test/resources/1591365.CSV.gz")
    val df = ags.getDF(csv)
    df.count() shouldBe (28)
    df.columns.toList should contain allOf ("PK_Date","PK_FlNum","PK_Tail","PK_Origin","PK_Dest","FileName","FolderName")

    val h = df.head
    h.getAs[String]("FileName") shouldBe ("1591365.CSV.gz")
    h.getAs[String]("FolderName") shouldBe ("resources")
    h.getAs[String]("PK_Date") shouldBe (" 06/09/17 ")
    h.getAs[String]("PK_FlNum") shouldBe (" UAE 323 ")
    h.getAs[String]("PK_Tail") shouldBe (" A6- EDF ")
    h.getAs[String]("PK_Origin") shouldBe (" ICN ")
    h.getAs[String]("PK_Dest") shouldBe (" DXB ")

  }

  "Blank csvFile returns error: test" should "pass" in {
    val ags = new AGSTransformers
    val csv = SourceFile("")
    an [ArrayIndexOutOfBoundsException] should be thrownBy ags.getDF(csv)

  }

  "loadFiles should return correct DF: test" should "pass" in {
    val ags = new AGSTransformers
//    val in_path = "file:/C:/Users/s794642/Documents/AGS/input"
    val in_path = "file:////home/verae/AGS/src/test/resources"
    val (df_rl, df_rp) = ags.loadFiles(ags.listFiles(in_path))
    df_rl.count() shouldBe (3)
    df_rp.count() shouldBe (80)
    df_rl.columns.toList should contain allOf ("FileName","FolderName","HELIX_UUID","HELIX_TIMESTAMP")
    df_rp.columns.toList should contain allOf ("PK_Date","PK_FlNum","PK_Tail","PK_Origin","PK_Dest","FileName","FolderName","HELIX_UUID","HELIX_TIMESTAMP")

    df_rp.select("FileName").distinct.count shouldBe (3)
    df_rp.select("FileName").distinct.collectAsList() should contain allOf (Row("1591381.CSV.gz"),Row("1591379.CSV.gz"),Row("1591365.CSV.gz"))
    df_rp.select("FolderName").distinct.count shouldBe (1)
    df_rp.select("FolderName").distinct.collectAsList() should contain (Row("resources"))
    df_rp.select("PK_Date").distinct.count shouldBe (2)
    df_rp.select("PK_Date").distinct.collectAsList() should contain allOf (Row(" 05/09/17 "),Row(" 06/09/17 "))
    df_rp.select("PK_FlNum").distinct.count shouldBe (3)
    df_rp.select("PK_FlNum").distinct.collectAsList() should contain allOf (Row(" UAE 186 "),Row(" UAE 418 "),Row(" UAE 323 "))
    df_rp.select("PK_Tail").distinct.count shouldBe (3)
    df_rp.select("PK_Tail").distinct.collectAsList() should contain allOf (Row(" A6- EDF "),Row(" A6- EDI "),Row(" A6- EDN "))
    df_rp.select("PK_Origin").distinct.count shouldBe (3)
    df_rp.select("PK_Origin").distinct.collectAsList() should contain allOf (Row(" ICN "),Row(" BCN "),Row(" BKK "))
    df_rp.select("PK_Dest").distinct.count shouldBe (2)
    df_rp.select("PK_Dest").distinct.collectAsList() should contain allOf (Row(" DXB "),Row(" SYD "))
    df_rp.select("HELIX_TIMESTAMP").distinct.count shouldBe (1)
    df_rp.select("HELIX_UUID").distinct.count shouldBe (80)

    df_rl.select("FileName").distinct.count shouldBe (3)
    df_rl.select("FileName").distinct.collectAsList() should contain allOf (Row("1591381.CSV.gz"),Row("1591379.CSV.gz"),Row("1591365.CSV.gz"))
    df_rl.select("FolderName").distinct.count shouldBe (1)
    df_rl.select("FolderName").distinct.collectAsList() should contain (Row("resources"))

    df_rl.select("HELIX_TIMESTAMP").distinct.count shouldBe (1)
    val timestamp_pattern = raw"\d{10}".r
    val check_ts = df_rl.select("HELIX_TIMESTAMP").distinct.head.getString(0) match {
      case timestamp_pattern(_*) => true
      case _ => false
    }
    check_ts shouldBe (true)

    df_rl.select("HELIX_UUID").distinct.count shouldBe (3)
    val uuid_pattern = raw"[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}".r
    val check_uuid = df_rl.select("HELIX_UUID").distinct.head.getString(0) match {
      case uuid_pattern(_*) => true
      case _ => false
    }
    check_uuid shouldBe (true)

  }

}

