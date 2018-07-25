/*----------------------------------------------------------------------------
 * Created on  : 01/Mar/2018
 * Author      : Pratik Vyas(S795358)
 * Email       : pratik.vyas@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : PayoadLoadsheetSchemaSyncArguments.scala
 * Description : Model class
 * ----------------------------------------------------------
 */

package com.emirates.helix.model


/**
  * Model class with different case classes for supporting operations
  */
object Model {

  case class Payload(
                      flightNumber	: String = null,
                      dayOfMovement	: String = null,
                      departurePort	: String = null,
                      arrivalPort	: String = null,
                      aircraftRegistration	: String = null,
                      flightDate	: String = null,
                      totalPassengersOnBoardIncludingInfants	: String = null,
                      crewIdentifier	: String = null,
                      crewCompliment	: String = null,
                      passengerIdentifier	: String = null,
                      totalPassengerNumbersPerClassValue	: String = null,
                      totalPassengerIdentifier	: String = null,
                      zeroFuelWeightIdentifier	: String = null,
                      zeroFuelWeight: String = null,
                      zeroFuelWeightMaximumIdentifier	: String = null,
                      maximumZeroFuelWeight: String = null,
                      takeOffFuelIdentifier	: String = null,
                      takeOffFuel	: String = null,
                      takeOffWeightIdentifier	: String = null,
                      takeOffWeight: String = null,
                      landingWeight: String = null,
                      dryOperatingWeightValue: String = null,
                      dryOperatingIndexvalue: String = null,
                      loadedIndexZFWvalue: String = null,
                      totalPassengerWeightIdentifier: String = null,
                      totalPassengerWeightValue	: String = null,
                      loadsheetIdentifier	: String = null,
                      finalLoadsheetIdentifier	: String = null,
                      finalLoadsheetEditionNumberIdentifier	: String = null,
                      maximumTakeOffWeight: String = null,
                      tripFuelIdentifier	: String = null,
                      tripFuel	: String = null,
                      landingWeightIdentifier	: String = null,
                      landingWeightMaximumIdentifier	: String = null,
                      maximumLandingWeight	: String = null,
                      limitingFactorIdentifier	: String = null,
                      balanceandSeatingIdentifier	: String = null,
                      dryOperatingWeightLabel	: String = null,
                      DOILabel	: String = null,
                      LIZFWLabel	: String = null,
                      aerodynamicChordZFWLabel	: String = null,
                      aerodynamicChordZFWValue : String = null,
                      LITOWLabel	: String = null,
                      loadedIndexTOWvalue: String = null,
                      aerodynamicChordTOWvalue	: String = null,
                      cabinSectionTotals	: String = null,
                      loadInCompartment: String = null,
                      totalWeightInCompartmentIdentifier	: String = null,
                      totalWeightOfAllCompartmentsValue	: String = null,
                      underloadIdentifier	: String = null,
                      underloadValue	: String = null,
                      LMCIdentifier	: String = null,
                      destinationIdentifier	: String = null,
                      specificationIdentifier	: String = null,
                      plusMinusIndicator	: String = null,
                      WTIndicator	: String = null,
                      indexIndicator	: String = null,
                      supplementaryInformationIdentifier	: String = null,
                      DC_HELIX_UUID	: String = null,
                      DC_HELIX_TIMESTAMP	: String = null
  )


}
