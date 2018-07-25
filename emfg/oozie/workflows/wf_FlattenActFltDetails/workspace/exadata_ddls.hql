create database if not EXISTS ops_eff;

--------------------------------------------------------------------------------------------------------------------------------
-- External HIVE tables creation for flatting
--------------------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ops_eff.OPS_STG_EDH_FLT_ACT_DTLS(
flight_identifier String,
block_time_mins String,
trip_time_mins String,
taxi_out_mins String,
taxi_in_mins String,
fuel_out_kgs String,
fuel_off_kgs String,
fuel_on_kgs String,
fuel_in_kgs String,
block_fuel_kgs String,
trip_fuel_kgs String,
fuel_populated_datetime String,
oooi_azfw_kgs String,
oooi_fuel_out_kgs String,
oooi_fuel_off_kgs String,
oooi_fuel_on_kgs String,
oooi_fuel_in_kgs String,
egds_populated_datetime String,
contingency_fuel_used_kgs String,
captain_extra_fuel_kgs String,
pct_contingency_used_kgs String,
zfw_correction_kgs String,
wabi_takeoff_fuel_kgs String,
adj_extra_fuel String,
excess_arrival_fuel String,
adj_extra_arr_fuel String,
extra_fuel_penalty String,
fuel_burn_saved String,
fm_recorded_datetime String,
mfp_file_name String,
mfp_file_path String,
mfp_page_count String,
mfp_source_folder String,
mfp_folder_index String,
mfp_valid_ind String,
mfp_fuel_reason_code String,
mfp_fuel_reason_comment String,
captain_staff_id String,
captain_staff_name String,
mfp_published_datetime String,
mfp_source_sequence_id String,
grnd_distance String,
esad String,
windspeed String,
file_name String,
hold_entry_time String,
hold_exit_time String,
hold_area String,
event_type String,
nearest_star String,
hold_time_mins String,
epic_recorded_datetime String,
act_fuel_uplifted_gal String,
act_fuel_uplifted_litres String,
fuel_specific_gravity String,
act_potable_water_aft_litres String,
act_potable_water_pdaft_litres String,
um_recorded_datetime String
)
PARTITIONED BY (HX_SNAPSHOT_DATE string)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
LOCATION '/data/helix/uat/modelled/emfg/serving/flightdetailsactual/OPS_STG_EDH_FLT_ACT_DTLS'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

----------------------------------------------------------------------------------------------------------------------------------------
--Adding partitions to metastore
----------------------------------------------------------------------------------------------------------------------------------------
msck repair table ops_eff.OPS_STG_EDH_FLT_ACT_DTLS;
