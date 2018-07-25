/*----------------------------------------------------------------------------
 * Created on  : 04/01/2018
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PlndFlightDetailsProcessor.scala
 * Description : Secondary class file for Planned Flight Details satellite construction
 * ---------------------------------------------------------------------------
 */
package com.emirates.helix.emfg.plannedflightdetails

import com.emirates.helix.emfg.model.Model._
import com.emirates.helix.emfg.util.SparkUtils
import com.emirates.helix.emfg.dfutils.DataFrameUtils._
import com.emirates.helix.emfg.lido.SatelliteFields._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Companion object for PlndFlightDetailsProcessor class
  */
object PlndFlightDetailsProcessor {
  /**
    * PlndFlightDetailsProcessor instance creation
    * @param args user parameter instance
    * @return PlndFlightDetailsProcessor instance
    */
  def apply(args: PlndFlgtArgs): PlndFlightDetailsProcessor  = {
    var processor = new PlndFlightDetailsProcessor()
    processor.args = args
    processor
  }
}

/**
  * PlndFlightDetailsProcessor class with methods to
  * read data from raw layer and move to decomposed.
  */
class PlndFlightDetailsProcessor extends SparkUtils {
  private var args: PlndFlgtArgs = _
  /** Method for performing the processing which in this case satellite construction
    *
    * @param flight_mstr Dataframe represents flight master ata
    * @param lido_flight Dataframe represents lido flight deduped data
    * @param lido_route Dataframe represents lido route deduped data
    * @param lido_weather Dataframe represents lido weather deduped data
    * @return DataFrame Spark dataframe for satellite table
    */
  def processData(flight_mstr: DataFrame, lido_flight : DataFrame, lido_route: DataFrame, lido_weather: DataFrame)(implicit spark: SparkSession): (DataFrame,Option[DataFrame]) = {
    import spark.implicits._
    var current_data: DataFrame = null
    var hist_data: Option[DataFrame] = None
    val satellite = getFlightdetals(flight_mstr.formatFlightmaster, lido_flight.formatLidoFlight, lido_route.formatLidoRoute, lido_weather.formatLidoWeather).cache()
    val max_date = satellite.select(max(to_date($"flight_date"))).first().getDate(0)
    val satellite_final = satellite.withColumn("max_flightdate",lit(max_date))

    args.init match {
      case true => {        // Logic for first day load. Data will be splitted to history and current based on months parameter
        current_data = satellite_final.where(to_date($"flight_date") >=
          add_months($"max_flightdate", args.months * -1)).drop($"max_flightdate")
        hist_data = Some(satellite_final.where(to_date($"flight_date") <
          add_months($"max_flightdate", args.months * -1)).drop($"max_flightdate"))
      }
      case false => current_data = satellite_final.drop($"max_flightdate")
    }
    (current_data,hist_data)
  }

  /**
    *  Overwritten method for super class
    */
  override def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String] = Some("overwrite"),
                           coalesceValue: Option[Int] = Some(args.part) ): Unit = super.writeToHDFS(output, hdfsPath,
    outputFormat,saveMode,coalesceValue)
}

