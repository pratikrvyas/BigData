/*----------------------------------------------------------------------------
 * Created on  : 01/22/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : DmisFormatIncrSchemaProcessor.scala
 * Description : Secondary class file for formatting DMIS incremental data.
 * ---------------------------------------------------------------------------
 */

package com.emirates.helix

import com.emirates.helix.model.Model._
import com.emirates.helix.util.SparkUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Companion object for DmisFormatIncrSchemaProcessor class
  */
object DmisFormatIncrSchemaProcessor {
  /**
    * DmisFormatIncrSchemaProcessor instance creation
    * @param args user parameter instance
    * @return DmisFormatIncrSchemaProcessor instance
    */
  def apply(args: DMISIncrFrmtArgs): DmisFormatIncrSchemaProcessor = {
    var processor = new DmisFormatIncrSchemaProcessor()
    processor.args = args
    processor
  }
}

/**
  * DmisFormatIncrSchemaProcessor class with methods to
  * generate schema sync prerequisite data for DMIS incremental.
  */
class DmisFormatIncrSchemaProcessor extends SparkUtils {
  private var args: DMISIncrFrmtArgs = _

  /**  Method performing DMIS incremental data formatting
    *
    *  @return DataFrame
    */
  def processData(in_df: DataFrame)(implicit sqlContext:SQLContext):  DataFrame = {
    import sqlContext.implicits._

    in_df.select($"FlightInfo.actualDate._ATTRIBUTE_VALUE".alias("actualDate"),
      $"FlightInfo.adhocTelexDate".alias("adhocTelexDate"),
      $"FlightInfo.adhocType._ATTRIBUTE_VALUE".alias("adhocType"),
      $"FlightInfo.aircraftType._ATTRIBUTE_VALUE".alias("aircraftType"),
      $"FlightInfo.aircraftTypeSize".alias("aircraftTypeSize"),
      $"FlightInfo.airlineCode._ATTRIBUTE_VALUE".alias("airlineCode"),
      $"FlightInfo.arrivalDepartureFlag".alias("arrivalDepartureFlag"),
      $"FlightInfo.arrivalStation".alias("arrivalStation"),
      $"FlightInfo.baggageBeltNumber._ATTRIBUTE_VALUE".alias("baggageBeltNumber"),
      $"FlightInfo.bayNumber._ATTRIBUTE_VALUE".alias("bayNumber"),
      $"FlightInfo.cargoDoorCloseDate._ATTRIBUTE_VALUE".alias("cargoDoorCloseDate"),
      $"FlightInfo.cashCreditYN".alias("cashCreditYN"),
      $"FlightInfo.chargedAirlineCode._ATTRIBUTE_VALUE".alias("chargedAirlineCode"),
      $"FlightInfo.choksOffDate".alias("choksOffDate"),
      $"FlightInfo.choksOnDate".alias("choksOnDate"),
      $"FlightInfo.codeshareFlight1._ATTRIBUTE_VALUE".alias("codeshareFlight1"),
      $"FlightInfo.codeshareFlight2".alias("codeshareFlight2"),
      $"FlightInfo.codeshareFlight3".alias("codeshareFlight3"),
      $"FlightInfo.codeshareFlight4".alias("codeshareFlight4"),
      $"FlightInfo.codeshareFlight5".alias("codeshareFlight5"),
      $"FlightInfo.departureStation".alias("departureStation"),
      $"FlightInfo.dubaiOrigin._ATTRIBUTE_VALUE".alias("dubaiOrigin"),
      $"FlightInfo.estimatedDate._ATTRIBUTE_VALUE".alias("estimatedDate"),
      $"FlightInfo.flightCancelDate".alias("flightCancelDate"),
      $"FlightInfo.flightCombiType._ATTRIBUTE_VALUE".alias("flightCombiType"),
      $"FlightInfo.flightHandlingType".alias("flightHandlingType"),
      $"FlightInfo.flightNumber".alias("flightNumber"),
      $"FlightInfo.flightRemarks".alias("flightRemarks"),
      $"FlightInfo.flightStatus._ATTRIBUTE_VALUE".alias("flightStatus"),
      $"FlightInfo.flightSubType._ATTRIBUTE_VALUE".alias("flightSubType"),
      $"FlightInfo.flightUpdatedDate".alias("flightUpdatedDate"),
      $"FlightInfo.flightcreatedDate".alias("flightcreatedDate"),
      $"FlightInfo.gateNumber._ATTRIBUTE_VALUE".alias("gateNumber"),
      $"FlightInfo.handlingTerminal._ATTRIBUTE_VALUE".alias("handlingTerminal"),
      $"FlightInfo.latestDate._ATTRIBUTE_VALUE".alias("latestDate"),
      $"FlightInfo.linkFlightAirlineCode".alias("linkFlightAirlineCode"),
      $"FlightInfo.linkedFlightArrivalDepartureFlag._ATTRIBUTE_VALUE".alias("linkedFlightArrivalDepartureFlag"),
      $"FlightInfo.linkedFlightDate._ATTRIBUTE_VALUE".alias("linkedFlightDate"),
      $"FlightInfo.linkedFlightNumber._ATTRIBUTE_VALUE".alias("linkedFlightNumber"),
      $"FlightInfo.pairFlightAirlineCode".alias("pairFlightAirlineCode"),
      $"FlightInfo.pairFlightArrivalDepartureFlag._ATTRIBUTE_VALUE".alias("pairFlightArrivalDepartureFlag"),
      $"FlightInfo.pairFlightDate._ATTRIBUTE_VALUE".alias("pairFlightDate"),
      $"FlightInfo.pairFlightnumber._ATTRIBUTE_VALUE".alias("pairFlightnumber"),
      $"FlightInfo.parkingTerminal._ATTRIBUTE_VALUE".alias("parkingTerminal"),
      $"FlightInfo.partialHandling".alias("partialHandling"),
      $"FlightInfo.paxDoorCloseDate._ATTRIBUTE_VALUE".alias("paxDoorCloseDate"),
      $"FlightInfo.registrationNumber._ATTRIBUTE_VALUE".alias("registrationNumber"),
      $"FlightInfo.scheduledDate".alias("scheduledDate"),
      $"FlightInfo.shortFinalDate".alias("shortFinalDate"),
      $"FlightInfo.touchdownAirborneDate".alias("touchdownAirborneDate"),
      $"FlightInfo.uldCharges._ATTRIBUTE_VALUE".alias("uldCharges"),
      $"FlightInfo.viaRoutes".alias("viaRoutes"),
      $"HELIX_UUID",$"HELIX_TIMESTAMP")
  }

  /**  Method to write processed data to HDFS
    *
    *  @param out_df Dataframe to be written to HDFS
    */
  def writeData(out_df: DataFrame): Unit = {
    args.out_format match {
      case "avro" => out_df.write.format("com.databricks.spark.avro").save(args.out_path)
      case "parquet" => out_df.write.parquet(args.out_path)
    }
  }
}
