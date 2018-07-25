/*----------------------------------------------------------
 * Created on  : 12/11/2017
 * Author      : Manu Mukundan(S795217)
 * Email       : manu.mukundan@dnata.com
 * Version     : 1.0
 * Project     : Helix-OpsEfficnecy
 * Filename    : UDF.scala
 * Description : Scala file to keep all UDF functions
 * ----------------------------------------------------------
 */
package com.emirates.helix.udf

import java.io.Serializable

import org.apache.spark.sql.functions.udf


object DMISUDF extends Serializable {

  case class act_bay_details(act_dep_bay_number: String = null, act_arr_bay_number: String = null)

  case class act_dep_gate_details(act_dep_gate_number: String = null, act_dep_concourse_number: String = null,
                                  act_dep_terminal_number: String = null)

  case class act_arr_gate_details(act_arr_gate_number: String = null, act_arr_concourse_number: String = null,
                                  act_arr_terminal_number: String = null)

  case class pln_dep_bay_details(pln_dep_old_bay_number: String = null, pln_dep_new_bay_number: String = null,
                                 pln_dep_bay_change_reason_code: String = null, pln_dep_bay_change_reason_desc: String = null,
                                 dep_bay_recorded_datetime: String = null)

  case class pln_arr_bay_details(pln_arr_old_bay_number: String = null, pln_arr_new_bay_number: String = null,
                                 pln_arr_bay_change_reason_code: String = null, pln_arr_bay_change_reason_desc: String = null,
                                 arr_bay_recorded_datetime: String = null)

  case class pln_dep_gate_details(pln_dep_gate_number: String = null, pln_dep_concourse_number: String = null,
                                  pln_dep_terminal_number: String = null, pln_dep_datetime_utc: String = null,
                                  pln_dep_datetime_local: String = null, dep_gate_recorded_datetime: String = null)

  case class pln_arr_gate_details(pln_arr_gate_number: String = null, pln_arr_concourse_number: String = null,
                                  pln_arr_terminal_number: String = null, pln_arr_datetime_utc: String = null,
                                  pln_arr_datetime_local: String = null, arr_gate_recorded_datetime: String = null)

  val getActBayDetails = udf((arrivalDepartureFlag: String, bayNumber: String) => {
    if (arrivalDepartureFlag.toLowerCase == "a") act_bay_details(act_arr_bay_number = bayNumber) else
      act_bay_details(act_dep_bay_number = bayNumber)
  })

  val getActDepGateDetails = udf((gate: String, concourse: String, terminal: String) => act_dep_gate_details(gate, concourse, terminal))

  val getActArrGateDetails = udf((gate: String, concourse: String, terminal: String) => act_arr_gate_details(gate, concourse, terminal))

  val getPlnDepBayDetails = udf((oldbay: String, newbay: String, reasonid: String, reasondesc: String, datetime: String) =>
    pln_dep_bay_details(oldbay, newbay, reasonid, reasondesc, datetime))

  val getPlnArrBayDetails = udf((oldbay: String, newbay: String, reasonid: String, reasondesc: String, datetime: String) =>
    pln_arr_bay_details(oldbay, newbay, reasonid, reasondesc, datetime))

  val getPlnDepGateDetails = udf(() => pln_dep_gate_details())

  val getPlnArrGateDetails = udf(() => pln_arr_gate_details())
}

