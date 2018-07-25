-- -----------------------------
--ENABLING DYNAMIC PARTITION
-- -----------------------------
set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict; 

-- -----------------------------
--INSERTING ERROR STATUS
-- -----------------------------

insert overwrite table ${DATABASE}.${CTRL_TABLE_NAME} partition(interface_table,hx_snapshot_date)  
select "FLIGHT" as module_id,edh_process_start_date_time,current_timestamp as edh_process_end_date_time ,"E" as edh_load_status,"Process failed at Helix" as edh_load_comments,'${LOAD_TYPE}' as edh_load_type,edh_record_count,"D" as load_frequency,"" as opsbi_start_date_time,"" as opsbi_end_date_time,"" as opsbi_load_status,"" as opsbi_load_comments,"" as edh_batch_no,'${TABLE_NAME}' as interface_table,'${HX_SNAPSHOT_DATE}' as hx_snapshot_date from ${DATABASE}.${CTRL_TABLE_NAME} where hx_snapshot_date = '${HX_SNAPSHOT_DATE}' and interface_table='${TABLE_NAME}';;