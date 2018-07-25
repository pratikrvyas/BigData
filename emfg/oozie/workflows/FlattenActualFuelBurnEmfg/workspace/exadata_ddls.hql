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
PARTITIONED BY (INTERFACE_TABLE string, HX_SNAPSHOT_DATE string)
LOCATION '/data/helix/uat/modelled/emfg/data/helix/modelled/emfg/audit';

--------------------------------------------------------------------------------------------------------------------------------
-- External HIVE tables creation for flatting
--------------------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_ACT_FLT_FUELWAYPNT(
flight_identifier string,
file_name String,
seq_no String,
latitude String,
longitude String,
latitude_received String,
longitude_received String,
baro_setting String,
gross_weight String,
fuel_burned String,
fob String,
fob_t1 String,
fob_t2 String,
fob_t3 String,
fob_t4 String,
fob_t5 String,
fob_t6 String,
fob_t7 String,
fob_t8 String,
fob_t9 String,
fob_t10 String,
fob_t11 String,
fob_t12 String
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/fuelburnactual/flight_act_waypnt_fuel_details'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_ACT_FLT_DISTWAYPNT(
flight_identifier string,
file_name String,
seq_no String,
latitude String,
longitude String,
latitude_received String,
longitude_received String,
true_heading String,
alt_std String,
alt_qnh String,
CAS String,
TAS String,
mach String,
phase String,
track_dist String,
N11 String,
N12 String,
N13 String,
N14 String,
IVV String,
air_grnd_discrete String,
tran_alt_discrete String,
CG String,
isa_deviation_actuals String,
wind_direction_actuals String,
wind_speed_actuals String,
TAT String,
SAT String,
cr_wind_comp String,
cr_wind_vector String,
wind_comp_actuals String,
wind_comp_navigation String,
altitude_actuals String,
height_actuals String,
gnd_speed_actuals String,
air_distance_actuals String,
acc_dist_nm_actuals String,
acc_air_dist_nm_actuals String,
acc_time_mins String
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/fuelburnactual/flight_act_waypnt_distance_speed_details'
TBLPROPERTIES ("parquet.compression"="SNAPPY");


----------------------------------------------------------------------------------------------------------------------------------------
--Adding partitions to metastore
----------------------------------------------------------------------------------------------------------------------------------------
msck repair table ops_eff.OPS_STG_EDH_ACT_FLT_FUELWAYPNT;

msck repair table ops_eff.OPS_STG_EDH_ACT_FLT_DISTWAYPNT;
