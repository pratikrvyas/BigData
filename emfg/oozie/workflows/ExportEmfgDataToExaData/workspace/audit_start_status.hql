-- -----------------------------
--ENABLING DYNAMIC PARTITION
-- -----------------------------
set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict; 

-- -----------------------------
--INSERTING START STATUS
-- -----------------------------

insert overwrite table ${DATABASE}.${CTRL_TABLE_NAME} partition(interface_table,hx_snapshot_date)  
select "FLIGHT" as module_id,current_timestamp as edh_process_start_date_time,"" as edh_process_end_date_time ,"R" as edh_load_status,"Load Running at Helix" as edh_load_comments,'${LOAD_TYPE}' as edh_load_type,edh_record_count,"D" as load_frequency,"" as opsbi_start_date_time,"" as opsbi_end_date_time,"" as opsbi_load_status,"" as opsbi_load_comments,"" as edh_batch_no,'${TABLE_NAME}' as interface_table,'${HX_SNAPSHOT_DATE}' as hx_snapshot_date from (select count(1) as edh_record_count from ${DATABASE}.${TABLE_NAME} where hx_snapshot_date = '${HX_SNAPSHOT_DATE}') src;;