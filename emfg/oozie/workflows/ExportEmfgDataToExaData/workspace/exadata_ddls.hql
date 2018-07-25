create database if not EXISTS ops_eff;

--------------------------------------------------------------------------------------------------------------------------------
-- External HIVE tables creation for Control Table
--------------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_INTERFACE_CTRL (
  MODULE_ID                   string,
  EDH_PROCESS_START_DATE_TIME string,
  EDH_PROCESS_END_DATE_TIME   string,
  EDH_LOAD_STATUS             string,
  EDH_LOAD_COMMENTS           string,
  EDH_LOAD_TYPE               string,
  EDH_RECORD_COUNT            string,
  LOAD_FREQUENCY              string,
  OPSBI_START_DATE_TIME       string,
  OPSBI_END_DATE_TIME         string,
  OPSBI_LOAD_STATUS           string,
  OPSBI_LOAD_COMMENTS         string,
  EDH_BATCH_NO                string
)
PARTITIONED BY (INTERFACE_TABLE string, HX_SNAPSHOT_DATE date)
LOCATION '/data/helix/uat/modelled/emfg/audit';

--------------------------------------------------------------------------------------------------------------------------------
-- External HIVE tables creation for flatting
--------------------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLIGHT_DELAY_TRANS (
flight_identifier string,
delay_code string,
delay_reason string,
delay_dept_code string,
delay_duration_mins string,
delay_iata_station string,
delay_icao_station string,
delay_type string,
delay_posted_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLIGHT_DELAY_TRANS'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLIGHT_HUB_MASTER (
flight_identifier string,
pln_dep_runway string,
pln_arr_runway string,
act_dep_runway string,
act_arr_runway string,
act_dep_bay_number string,
act_arr_bay_number string,
act_dep_gate_number string,
act_dep_concourse_number string,
act_dep_terminal_number string,
act_arr_gate_number string,
act_arr_concourse_number string,
act_arr_terminal_number string,
cost_index string,
pln_alt_apt1_iata string,
pln_alt_apt1_icao string,
pln_alt_apt2_iata string,
pln_alt_apt2_icao string,
pln_alt_apt3_iata string,
pln_alt_apt3_icao string,
pln_alt_apt4_iata string,
pln_alt_apt4_icao string,
flight_number string,
actual_reg_number string,
flight_date string,
pln_dep_iata_station string,
pln_dep_icao_station string,
pln_arr_iata_station string,
pln_arr_icao_station string,
act_dep_iata_station string,
act_dep_icao_station string,
act_arr_iata_station string,
act_arr_icao_station string,
sch_dep_datetime_utc string,
sch_dep_datetime_local string,
sch_arr_datetime_utc string,
sch_arr_datetime_local string,
act_dep_datetime_utc string,
act_dep_datetime_local string,
act_arr_datetime_utc string,
act_arr_datetime_local string,
flight_service_type string,
flight_service_desc string,
airline_code string,
airline_name string,
airline_country_name string,
flight_call_sign string,
arr_bag_carousel_number string,
flight_suffix string,
flight_leg_number string,
flight_dep_number string,
flight_via_route string,
flight_cabin_door_closure_date string,
flight_cargo_door_closure_date string,
flight_blocktime_orig string,
flight_blocktime_act string,
act_flight_out_datetime_utc string,
act_flight_out_datetime_local string,
act_flight_off_datetime_utc string,
act_flight_off_datetime_local string,
act_flight_in_datetime_utc string,
act_flight_in_datetime_local string,
act_flight_on_datetime_utc string,
act_flight_on_datetime_local string,
aircraft_mfr string,
aircraft_type string,
aircraft_subtype string,
aircraft_config string,
aircraft_tot_capcity string,
aircraft_f_capacity string,
aircraft_j_capacity string,
aircraft_y_capacity string,
aircraft_lease_ind string,
etops_ind string,
dow string,
aircraft_version_code string,
aircraft_owner string,
potal_water_tank_capacity string,
no_engines string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLIGHT_HUB_MASTER'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLIGHT_PLN_REG (
flight_identifier string,
pln_reg_number string,
pln_posted_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLIGHT_PLN_REG'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLIGHT_STAT_TRANS (
flight_identifier string,
flight_status string,
flight_status_desc string,
flt_status_recorded_datetime string 
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLIGHT_STAT_TRANS'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_PLN_ALT_APT (
flight_identifier string,
pln_alt_apt1_iata string,
pln_alt_apt1_icao string,
pln_alt_apt2_iata string,
pln_alt_apt2_icao string,
pln_alt_apt3_iata string,
pln_alt_apt3_icao string,
pln_alt_apt4_iata string,
pln_alt_apt4_icao string,
lido_version_num string,
alt_msg_received_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLT_PLN_ALT_APT'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_PLN_RUNWAY (
flight_identifier string,
pln_dep_runway string,
pln_arr_runway string,
lido_version_num string,
runway_msg_created_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLT_PLN_RUNWAY'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_OUT_TIME_EST(
flight_identifier string,
est_flight_out_datetime_local string,
est_flight_out_datetime_utc string,
est_out_posted_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLT_OUT_TIME_EST'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_OFF_TIME_EST(
flight_identifier string,
est_flight_off_datetime_local string,
est_flight_off_datetime_utc string,
est_off_posted_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLT_OFF_TIME_EST'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_ON_TIME_EST(
flight_identifier string,
est_flight_on_datetime_local string,
est_flight_on_datetime_utc string,
est_on_posted_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLT_ON_TIME_EST'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_IN_TIME_EST(
flight_identifier string,
est_flight_in_datetime_local string,
est_flight_in_datetime_utc string,
est_in_posted_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLT_IN_TIME_EST'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_COST_INDEX (
flight_identifier string,
cost_index string,
lido_version_num string,
ci_posted_datetime string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/OPS_STG_EDH_FLT_COST_INDEX'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

----------------------------------------------------------------------------------------------------------------------------------------
--Adding partitions to metastore
----------------------------------------------------------------------------------------------------------------------------------------
msck repair table ops_eff.OPS_STG_EDH_INTERFACE_CTRL;

msck repair table ops_eff.OPS_STG_EDH_FLIGHT_DELAY_TRANS;

msck repair table ops_eff.OPS_STG_EDH_FLIGHT_HUB_MASTER;

msck repair table ops_eff.OPS_STG_EDH_FLIGHT_PLN_REG;
 
msck repair table ops_eff.OPS_STG_EDH_FLIGHT_STAT_TRANS;

msck repair table ops_eff.OPS_STG_EDH_FLT_PLN_ALT_APT;

msck repair table ops_eff.OPS_STG_EDH_FLT_PLN_RUNWAY;

msck repair table ops_eff.OPS_STG_EDH_FLT_OUT_TIME_EST;

msck repair table ops_eff.OPS_STG_EDH_FLT_OFF_TIME_EST;

msck repair table ops_eff.OPS_STG_EDH_FLT_ON_TIME_EST;

msck repair table ops_eff.OPS_STG_EDH_FLT_IN_TIME_EST;

msck repair table ops_eff.OPS_STG_EDH_FLT_COST_INDEX;
;;