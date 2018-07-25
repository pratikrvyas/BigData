create database if not EXISTS ops_eff;

--------------------------------------------------------------------------------------------------------------------------------
-- External HIVE tables creation for flatting
--------------------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_PLN_FLT_DISTWAYPNT(
flight_identifier String,
waypnt_seq_no String,
waypnt_name String,
altitude String,
grnd_dist String,
acc_dist String,
wind String,
wind_comp String,
tas String,
cas String,
mach_no String,
isa_dev String,
sr_tdev String,
mora String,
seg_temp String,
cross_wind String,
air_dist String,
trop String,
flight_level String,
acc_air_dist String,
msg_received_date String,
lido_version_num String
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/fuelburnplanned/ops_stg_edh_pln_flt_distwaypnt'
TBLPROPERTIES ("parquet.compression"="SNAPPY");



CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_PLN_FLT_FUELWAYPNT(
flight_identifier String,
waypnt_seq_no String,
waypnt_name String,
latitude String,
longitude String,
remaining_fuel String,
est_fob String,
est_fbu String,
stm String,
atm String,
msg_received_date String,
lido_version_num String,
true_heading String
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/fuelburnplanned/ops_stg_edh_pln_flt_fuelwaypnt'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

----------------------------------------------------------------------------------------------------------------------------------------
--Adding partitions to metastore
----------------------------------------------------------------------------------------------------------------------------------------
msck repair table ops_eff.OPS_STG_EDH_PLN_FLT_DISTWAYPNT;

msck repair table ops_eff.OPS_STG_EDH_PLN_FLT_FUELWAYPNT;