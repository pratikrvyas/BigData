-- RAW TO DECOMPOSED
-- register required libraries
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

REGISTER helix-pig-udfs-0.0.1.jar;

-- load the data
sms_logs_hist = LOAD '${SOURCE_DIR}' USING PigStorage( ',' );
-- sms_logs_hist = LOAD '${SOURCE_DIR}' USING PigStorage( ',' ) AS (mobile_number:chararray, sms_message:chararray, airline:chararray, created_date:chararray, action_time:chararray);

-- check for bad records and put them in rejected raw directory
row_with_col_count =  FOREACH sms_logs_hist GENERATE COUNT_STAR(TOBAG(*)),$0..;
SPLIT row_with_col_count INTO good_records_with_col_count IF (int)$0 == 5 , bad_records_with_col_count IF (int)$0 != 5;

good_records = FOREACH good_records_with_col_count GENERATE $1 as mobile_number, $2 as sms_message, $3 as airline, $4 as created_date, $5 as action_time;

bad_records = FOREACH bad_records_with_col_count GENERATE $1..;

-- store data in decomposed layer
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;
 
STORE good_records INTO '${TEMP_TARGET_DIR}' using PigStorage(',');
// store bad records to rejected directory
STORE bad_records INTO '${REJECTED_DIR}' using PigStorage(','); 


sms_logs_data = LOAD '${TEMP_TARGET_DIR}' USING PigStorage( ',' ) AS (mobile_number:chararray, sms_message:chararray, airline:chararray, created_date:chararray, action_time:chararray);

-- add uuid and timestamp
sms_logs_data_withid = FOREACH sms_logs_data GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid, (chararray)ToUnixTime(CurrentTime()) as timestamp, mobile_number, sms_message, airline, created_date, action_time;

STORE sms_logs_hist_withid INTO '${TARGET_DIR}'  USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${AVRO_SCHEMA}' );


