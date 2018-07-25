/*----------------------------------------------------------
* Created on  : 10/12/2017
* Author      : Pratik(S796368)
* Version     : 1.0
* Project     : Helix-OpsEfficnecy
* Description : Spark scala application to read the ALTEA Loadsheet fixed length raw data
  *               and store to the the decomposed layer as avro
  * ----------------------------------------------------------
  */



package com.emirates.helix




import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import com.emirates.helix.UDF._


object FixedlengthToAvro {

  private val compress_rex = """^snappy$|^deflate$""".r

  case class Config(in_path: String = null, out_path: String = null, schema_path: String = null,
                    hdfs_url: String = null, compression: String = null, rex: String = null)



  def main(args: Array[String]): Unit = {



    val parser = new scopt.OptionParser[Config]("ALTEALoadsheetMessageR2D") {
      head("ALTEALoadsheetMessageR2D")
      opt[String]('i', "in_path")
        .required()
        .action((x, config) => config.copy(in_path = x))
        .text("Required parameter : Input file path for raw data")
      opt[String]('o', "out_path")
        .required()
        .action((x, config) => config.copy(out_path = x))
        .text("Required parameter : Output file path for decomposed data")
      opt[String]('s', "schema_path")
        .required()
        .action((x, config) => config.copy(schema_path = x))
        .text("Required parameter : Decomposed data schema path")
      opt[String]('h', "hdfs_url")
        .required()
        .action((x, config) => config.copy(hdfs_url = x))
        .text("Required parameter : Hadoop name node url in the format hdfs://<host>")
      opt[String]('c', "compression")
        .required()
        .valueName("snappy OR deflate")
        .validate(x => {
          if (!(x.matches(compress_rex.toString()))) Left("Invalid compression specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(compression = x))
        .text("Required parameter : Output data compression format, snappy or deflate")
      opt[String]('x', "rex")
        .optional()
        .action((x, config) => config.copy(rex = x))
        .text("Optional parameter : Needed only if any invalid characters have to be removed from json string")
    }



    parser.parse(args, Config()) map { config =>
      val sc = new SparkContext(new SparkConf().set("spark.sql.avro.compression.codec", config.compression))
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      import sqlContext.implicits._

      val hdpConf = new Configuration()
      hdpConf.set("fs.defaultFS", config.hdfs_url)
      val schema = new Schema.Parser()
        .parse(FileSystem.get(hdpConf).open(new Path(config.schema_path)))



      // Read raw layer file  ( compression : snappy , file format :  JSON , message format : Fixed Length )
      val json_rdd = sc.sequenceFile[LongWritable, BytesWritable](config.in_path)
        .map(x => new String(x._2.copyBytes(), "utf-8"))


      val rowIndex=5
      val firstSplit=0
      val secondSplit=1
      val thirdSplit=2
      val fourthSplit=3
      val fifthSplit=4
      val sixthSplit=5
      var skipNumOfRawsForCabSec=2
      var skipNumOfRawsForLMC=1
      var skipRows=0

      // new
      val getIndexUnderloadIdentifier=udf((msg:String)=>{
        var index=0
        var k = 1
        var testflag=0
        var msgRowNum=msg.split("\r\n").length-1.toInt
        while( msgRowNum > 0 &&  testflag == 0 ) {
          k = msg.split("\r\n")(msgRowNum).trim().toUpperCase.indexOf("UNDERLOAD",0)
          if(k >= 0) { testflag = 1 }
          msgRowNum = msgRowNum - 1
        }
        msgRowNum+1
      })

      // new
      val getLMCIndexFZ = udf((msg: String) =>  {
        var lmcIndex=0
        var flt =msg.split("\r\n")(6).trim().split(" ").filterNot(_.isEmpty)(1).split("/")(0)
        if(flt.trim().substring(0,2)=="FZ")
        {
          lmcIndex=1
        }
        lmcIndex
      })


      val getRowWordConstant = udf((msg: String,rowSplitChar: String,rowIndex: Int,splitIndex: Int,replcae: String ) => {
        var str =""
        if (msg.split(rowSplitChar).length > rowIndex)
        {
          if (msg.split(rowSplitChar)(rowIndex).replace(replcae,"").trim().split(" ").filterNot(_.isEmpty).length > splitIndex)
          {
            if(splitIndex < 0)
            {
              str=msg.split(rowSplitChar)(rowIndex).trim()
            }
            else{
              str=msg.split(rowSplitChar)(rowIndex).replace(replcae,"").trim().split(" ").filterNot(_.isEmpty)(splitIndex).trim()
            }
          }}
        str
      })
      val getRowWord = udf((msg: String,rowSplitChar: String,rowIndex: Int,splitIndex: Int,replcae: String ) => {
        var str =""
        if (msg.split(rowSplitChar).length > rowIndex+skipRows)
        {
          if (msg.split(rowSplitChar)(rowIndex+skipRows).replace(replcae,"").trim().split(" ").filterNot(_.isEmpty).length > splitIndex)
          {
            if(splitIndex < 0)
            {
              str=msg.split(rowSplitChar)(rowIndex+skipRows).trim()
            }
            else{
              str=msg.split(rowSplitChar)(rowIndex+skipRows).replace(replcae,"").trim().split(" ").filterNot(_.isEmpty)(splitIndex).trim()
            }
          }}
        str
      })
      val getSubStr = udf((text: String,start: Int,end: Int) => {
        var endindex=end
        if( end > text.length ){endindex= text.length}
        val str=text.substring(start,endindex)
        str})
      val getSplit = udf((msg: String,splitChar: String,splitIndex: Int) => {
        var str=""
        if(msg.split(splitChar).length > splitIndex)
        {
          str=msg.split(splitChar)(splitIndex).trim()
        }
        str
      })

      val getCabSecIndex1 = udf((msg: String,rowIndex: Int,skipCabSecRows: Int ) =>  {
        var cabinSectionIndex=rowIndex
        if(msg.trim().substring(0,2)=="FZ")
        {
          cabinSectionIndex=cabinSectionIndex+skipCabSecRows
        }
        cabinSectionIndex + skipRows
      })
      val getCabSecIndex = udf((msg: String,currentRowIndex: Int,skipCabSecRows: Int ) =>  {
        var cabinSectionIndex=currentRowIndex
        var flt =msg.split("\r\n")(6).trim().split(" ").filterNot(_.isEmpty)(1).split("/")(0)
        if(flt.trim().substring(0,2)=="FZ")
        {
          cabinSectionIndex=cabinSectionIndex+skipCabSecRows
        }
        cabinSectionIndex + skipRows
      })
      val getLMCIndex1 = udf((msg: String,lmcrowIndex: Int,skipLMCRows: Int ) =>  {
        var cabinSectionIndex=lmcrowIndex
        var lmcIndex=0
        if(msg.trim().substring(0,2)=="FZ")
        {
          cabinSectionIndex=cabinSectionIndex+skipLMCRows
          lmcIndex=cabinSectionIndex
          lmcIndex=lmcIndex+skipNumOfRawsForLMC
        }
        lmcIndex + skipRows
      })
      val getLMCIndex = udf((msg: String,rowNumIndex: Int,skipCabSecRows: Int ) =>  {
        var cabinSectionIndex=0
        var lmcIndex=rowNumIndex
        var flt =msg.split("\r\n")(6).trim().split(" ").filterNot(_.isEmpty)(1).split("/")(0)
        if(flt.trim().substring(0,2)=="FZ")
        {
          lmcIndex=lmcIndex+skipCabSecRows+skipNumOfRawsForLMC
        }
        lmcIndex + skipRows
      })

      //LoadInCompartment
      //updated
      val getLoadInCompartment = udf((msg: String,currentRowIndex: Int ) =>  {
        var str=""
        var rowIndex=currentRowIndex + skipRows
        if (msg.split("\r\n").length > rowIndex)
        {
          if(msg.split("\r\n")(rowIndex).trim().toUpperCase.indexOf("T",0) > 0)
          {
            str=msg.split("\r\n")(rowIndex).trim().split("T")(0)
          }
          else
          {
            str=msg.split("\r\n")(rowIndex).trim() + " " + msg.split("\r\n")(rowIndex+1).trim().split("T")(0)
            // skipRows=skipRows+1
          }
        }
        str})

      //TotalWeightOfAllCompartmentsValue
      val getTotalWeightOfAllCompartmentsValue = udf((msg: String,rowIndex: Int ) =>  {
        var str=""
        if (msg.split("\r\n").length > rowIndex)
        {
          if(msg.split("\r\n")(rowIndex).trim().toUpperCase.indexOf("T",0) > 0)
          {
            if(msg.split("\r\n")(rowIndex).trim().split("T").length>1){str=msg.split("\r\n")(rowIndex).trim().split("T")(1)}
          }
          else
          {
            if(msg.split("\r\n")(rowIndex+1).trim().split("T").length>1){str=msg.split("\r\n")(rowIndex+1).trim().split("T")(1)}
          }
        }
        str.trim()})

      // updated
      val getCabinSectionTotals = udf((msg: String,currentRowIndex: Int ) =>  {
        var str=""
        if (msg.split("\r\n").length > currentRowIndex)
        {
          if(msg.split("\r\n")(currentRowIndex).trim().toUpperCase.indexOf("PW",0) > 0)
          {
            str=msg.split("\r\n")(currentRowIndex).trim().split("PW")(0)
          }
          else
          {
            str=msg.split("\r\n")(currentRowIndex).trim() + " " + msg.split("\r\n")(currentRowIndex+1).trim().split("PW")(0)
            // skipRows=skipRows+1
          }
        }
        str.trim()})



      val df= sqlContext.read.json(json_rdd).select(
        $"metadata.messageTime".as("tibco_message_time"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex),lit(firstSplit),lit("-"))).as("LoadsheetIdentifier"),
        lit(getSubStr(getRowWord($"message",lit("\r\n"),lit(rowIndex+1),lit(firstSplit),lit("")),lit(0),lit(5))).as("FinalLoadsheetIdentifier"),
        lit(getSubStr(getRowWord($"message",lit("\r\n"),lit(rowIndex+1),lit(firstSplit),lit("")),lit(5),lit(7))).as("FinalLoadsheetEditionNumberIdentifier"),
        lit(getSplit(getRowWord($"message",lit("\r\n"),lit(rowIndex+1),lit(secondSplit),lit("")),lit("/"),lit(firstSplit))).as("FlightNumber"),
        lit(getSplit(getRowWord($"message",lit("\r\n"),lit(rowIndex+1),lit(secondSplit),lit("")),lit("/"),lit(secondSplit))).as("Dayofmovement"),
        lit(getSubStr(getRowWord($"message",lit("\r\n"),lit(rowIndex+1),lit(thirdSplit),lit("")),lit(0),lit(3))).as("DeparturePort"),
        lit(getSubStr(getRowWord($"message",lit("\r\n"),lit(rowIndex+1),lit(thirdSplit),lit("")),lit(3),lit(6))).as("ArrivalPort"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+1),lit(fourthSplit),lit(""))).as("AircraftRegistration"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+1),lit(fifthSplit),lit(""))).as("FlightDate"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+2),lit(firstSplit),lit(""))).as("CrewIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+2),lit(secondSplit),lit(""))).as("CrewCompliment"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+2),lit(thirdSplit),lit(""))).as("PassengerIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+2),lit(fourthSplit),lit(""))).as("TotalPassengerNumbersPerClassValue"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+2),lit(fifthSplit),lit(""))).as("TotalPassengerIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+2),lit(sixthSplit),lit(""))).as("TotalPassengersOnBoardIncludingInfants"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+3),lit(firstSplit),lit(""))).as("ZeroFuelWeightIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+3),lit(secondSplit),lit(""))).as("ZeroFuelWeight"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+3),lit(thirdSplit),lit(""))).as("ZeroFuelWeightMaximumIdentifier"),
        lit(regexp_extract(lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+3),lit(fourthSplit),lit(""))), "([0-9]+)([A-Za-z]+)?" , 1 )).as("MaximumZeroFuelWeight"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+4),lit(firstSplit),lit(""))).as("TakeOffFuelIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+4),lit(secondSplit),lit(""))).as("TakeOffFuel"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+5),lit(firstSplit),lit(""))).as("TakeOffWeightIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+5),lit(secondSplit),lit(""))).as("TakeOffWeight"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+5),lit(thirdSplit),lit(""))).as("TakeOffWeightMaximumIdentifier"),
        lit(regexp_extract(lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+5),lit(fourthSplit),lit(""))), "([0-9]+)([A-Za-z]+)?" , 1 )).as("MaximumTakeOffWeight"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+6),lit(firstSplit),lit(""))).as("TripFuelIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+6),lit(secondSplit),lit(""))).as("TripFuel"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+7),lit(firstSplit),lit(""))).as("LandingWeightIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+7),lit(secondSplit),lit(""))).as("LandingWeight"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+7),lit(thirdSplit),lit(""))).as("LandingWeightMaximumIdentifier"),
        lit(regexp_extract(lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+7),lit(fourthSplit),lit(""))), "([0-9]+)(([A-Za-z]+)?)" , 1 )).as("MaximumLandingWeight"),
        lit(regexp_extract(lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+7),lit(fourthSplit),lit(""))), "([0-9]+)(([A-Za-z]+)?)" , 2 )).as("LimitingFactorIdentifier"),  ///change 1->2
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+8),lit(-1),lit(""))).as("BalanceandSeatingIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+9),lit(firstSplit),lit(""))).as("DryOperatingWeightLabel"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+9),lit(secondSplit),lit(""))).as("DryOperatingWeightValue"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+9),lit(thirdSplit),lit(""))).as("DOILabel"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+9),lit(fourthSplit),lit(""))).as("DryOperatingIndexvalue"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+10),lit(firstSplit),lit(""))).as("LIZFWLabel"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+10),lit(secondSplit),lit(""))).as("LoadedIndexZFWvalue"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+10),lit(thirdSplit),lit(""))).as("AerodynamicChordZFWLabel"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+10),lit(fourthSplit),lit(""))).as("AerodynamicChordZFWValue"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+11),lit(firstSplit),lit(""))).as("LITOWLabel"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+11),lit(secondSplit),lit(""))).as("LoadedIndexTOWvalue"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+11),lit(thirdSplit),lit(""))).as("AerodynamicChordTOWLabel"),
        lit(getRowWord($"message",lit("\r\n"),lit(rowIndex+11),lit(fourthSplit),lit(""))).as("AerodynamicChordTOWvalue"),
        lit(getCabinSectionTotals($"message",lit(getCabSecIndex($"message",lit(rowIndex+12),lit(skipNumOfRawsForCabSec))))).as("CabinSectionTotals"),
        lit(getRowWord($"message",lit("\r\n"),lit(lit(getCabSecIndex($"message",lit(rowIndex+12),lit(skipNumOfRawsForCabSec))) ),lit(secondSplit),lit(""))).as("TotalPassengerWeightIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(lit(getCabSecIndex($"message",lit(rowIndex+12),lit(skipNumOfRawsForCabSec))) ),lit(thirdSplit),lit(""))).as("TotalPassengerWeightValue"),
        lit(getLoadInCompartment($"message",lit(lit(lit(getCabSecIndex($"message",lit(rowIndex+13),lit(skipNumOfRawsForCabSec))) )))).as("LoadInCompartment"),
        trim(lit("T")).as("TotalWeightInCompartmentIdentifier"),
        lit(getTotalWeightOfAllCompartmentsValue($"message",lit(getCabSecIndex($"message",lit(rowIndex+13),lit(skipNumOfRawsForCabSec))))).as("TotalWeightOfAllCompartmentsValue"),
        lit(getRowWord($"message",lit("\r\n"),lit(getIndexUnderloadIdentifier($"message")),lit(firstSplit),lit(""))).as("UnderloadIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(getIndexUnderloadIdentifier($"message")),lit(secondSplit),lit(""))).as("UnderloadValue"),
        lit(getRowWord($"message",lit("\r\n"),lit(getIndexUnderloadIdentifier($"message") + 1 + lit(getLMCIndexFZ($"message"))),lit(firstSplit),lit(""))).as("LMCIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(getIndexUnderloadIdentifier($"message") + 2 + lit(getLMCIndexFZ($"message"))),lit(firstSplit),lit(""))).as("DestinationIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(getIndexUnderloadIdentifier($"message") + 2 + lit(getLMCIndexFZ($"message"))),lit(secondSplit),lit(""))).as("SpecificationIdentifier"),
        lit(getRowWord($"message",lit("\r\n"),lit(getIndexUnderloadIdentifier($"message") + 2 + lit(getLMCIndexFZ($"message"))),lit(thirdSplit),lit(""))).as("PlusMinusIndicator"),
        lit("WT").as("WTIndicator"),
        lit("INDEX").as("INDEXIndicator"),
        lit(getRowWord($"message",lit("\r\n"),lit(getIndexUnderloadIdentifier($"message") + 7 + lit(getLMCIndexFZ($"message"))),lit(-1),lit(""))).as("SupplementaryInformationIdentifier"),
        $"message".as("LoadsheetMessage")
      ).filter(trim($"LoadsheetIdentifier")==="LOADSHEET" )


      //val df1 = df.dropDuplicates

      // add helixId , timestamp.
      // save to decomposed as AVRO
      df.withColumn("HELIX_UUID",generateUUID())
        .withColumn("HELIX_TIMESTAMP",generateTimestamp())
        .write.format("com.databricks.spark.avro")
        .mode("overwrite")
        .option("avroSchema",schema.toString)
        .save(config.out_path)


    }


  }
}

