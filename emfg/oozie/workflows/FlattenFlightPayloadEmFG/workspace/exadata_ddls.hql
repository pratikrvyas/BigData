create database if not EXISTS ops_eff;

--------------------------------------------------------------------------------------------------------------------------------
-- External HIVE tables creation for Control Table
--------------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_INTERFACE_CTRL(
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
LOCATION '/data/helix/uat/modelled/emfg/data/helix/modelled/emfg/audit';

--------------------------------------------------------------------------------------------------------------------------------
-- External HIVE tables creation for flatting
--------------------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.ops_stg_edh_flt_pln_payload(
FLIGHT_IDENTIFIER string, 
EST_ZFW string,
MAX_ZFW string,
EST_TOW string,
MAX_TOW string,
EST_LWT string,
MAX_LWT string,
MAX_TAXIWT string,
LIDO_VERSION_NUM string,
MSG_RECEIVED_DATE string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/flightpayload/flight_pln_payload_details'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.ops_stg_edh_flt_act_payload(
FLIGHT_IDENTIFIER string, 
ZERO_FUEL_WT string,
MAX_ZERO_FUEL_WT string,
TAKE_OFF_WT string,
REGULATED_TAKE_OFF_WT string,
LANDING_WT string,
MAX_LANDING_WT string,
DRY_OP_WT string,
DRY_OP_INDEX string,
LIZFW string,
MACLIZFW string,
LITOW string,
MACTOW string,
TOTAL_COMPARTMENT_WEIGHT string,
UNDERLOAD string,
LOADSHEET_VERSION_NUM string,
MSG_RECEIVED_DATE string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/flightpayload/flight_act_payload_details'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.ops_stg_edh_flt_act_deadload(
FLIGHT_IDENTIFIER string, 
BAGGAGE_WEIGHT string, 
TOTAL_EXCESS_BAG_WEIGHT string, 
CARGO_WEIGHT string, 
MAIL_WEIGHT string, 
TRANSIT_BAG_WEIGHT string, 
MISCL_WEIGHT string, 
TOTAL_PAYLOAD string,
MSG_RECEIVED_DATE string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/flightpayload/flight_deadload_details'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.ops_stg_edh_flt_act_pax(
FLIGHT_IDENTIFIER string, 
TOTAL_PAX_COUNT string,
F_PAX_LOADSHEET_COUNT string,
J_PAX_LOADSHEET_COUNT string,
Y_PAX_LOADSHEET_COUNT string,
INFANTS_COUNT string,
PAX_WEIGHT string,
LOADSHEET_VERSION_NUM string,
MSG_RECEIVED_DATE string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/flightpayload/flight_pax_payload_details'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.ops_stg_edh_flt_act_comp_wt(
FLIGHT_IDENTIFIER string, 
COMP_NO string,
COMP_WEIGHT string,
LOADSHEET_VERSION_NUM string,
MSG_RECEIVED_DATE string
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/flightpayload/flight_act_compartment_details'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_LOADSHEET(
FLIGHT_IDENTIFIER string, 
LOADSHEET_MESSAGE string,
MSG_RECEIVED_DATE string 
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/flightpayload/flight_act_loadsheet_message'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

----------------------------------------------------------------------------------------------------------------------------------------
--Adding partitions to metastore
----------------------------------------------------------------------------------------------------------------------------------------
msck repair table ops_eff.ops_stg_edh_flt_pln_payload;

msck repair table ops_eff.ops_stg_edh_flt_act_payload;

msck repair table ops_eff.ops_stg_edh_flt_act_deadload;

msck repair table ops_eff.ops_stg_edh_flt_act_pax;

msck repair table ops_eff.ops_stg_edh_flt_act_comp_wt;

msck repair table ops_eff.ops_stg_edh_flt_loadsheet;
