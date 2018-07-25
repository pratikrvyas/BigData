## README

This is a spark application to build the flight master

##### Build the application and package it : 
`mvn clean package`
This step will compile, run unit tests (if any) and generate the output jar which will be copied to HDFS cluster and invoked by oozie spark action

##### Applicatoin usage and input parameter help:

CoreMasterDriver
Usage: spark-submit <spark-options> --class com.emirates.helix.CoreMasterDriver  CoreMaster-<jar version>.jar [options]

  -c, --core_input <value>
                           Required parameter : CORE input data location
  -a, --ags_input <value>  Required parameter : AGS input data location
  -d, --dmis_input <value>
                           Required parameter : DMIS input data location
  -l, --lido_input <value>
                           Required parameter : LIDO input data location
  -q, --aircraft_country_region_path <value>
                           Required parameter : Aircraft_country_region_path input data location
  -t, --flight_service_path <value>
                           Required parameter : Flight_service_path input data location
  -f, --flight_status_path <value>
                           Required parameter : Flight_status_path input data location
  -u, --flight_delay_path <value>
                           Required parameter : Flight_delay_path input data location
  -m, --look_back_months <value>
                           Required parameter : look_back_months
  -h, --output_history_location <value>
                           Required parameter : Output history data location
  -o, --output_current_location <value>
                           Required parameter : Output current data location
  -s, --compression snappy OR deflate
                           Required parameter : Output data compression format, snappy or deflate
  -p, --coalesce_value <value>
                           Required parameter : Coalesce value
  -b, --broadcast_size <value>
                           Required parameter : Auto Broadcast Join Threshold
  -r, --data_load_mode <value>
                           Required parameter : Data load mode location
  -x, --dmis_baydetail_input <value>
                           Required parameter : DMIS bay detail input
  -y, --baychange_lookup <value>
                           Required parameter : Bay change lookup
  -z, --concourse_lookup <value>
                           Required parameter : Concourse lookup
  -k, --aircraft_details_lookup <value> 
							Required parameter : Aircraft Details lookup
  --help                   Print this usage message
