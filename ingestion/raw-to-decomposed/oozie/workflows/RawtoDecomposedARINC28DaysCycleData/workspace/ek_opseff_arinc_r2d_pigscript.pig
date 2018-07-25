-- #######################################################################
-- ek_opseff_arinc_r2d_pigscript.pig
-- CREATED BY RAVINDRA CHELLUBANI, PRATIK VYAS
-- DATE : 30-10-2017
-- MODIFIED BY : 
-- MODIFIED DATE :
-- DESCRIPTION : This pig script will move data from raw to decomposed
-- ######################################################################

-- SETTING MAP REDUCE QUEUE
SET mapred.job.queuename '${JOB_QUEUE}';

REGISTER 'pigudf.jar'

-- LOADING DATA FROM RAW LAYER
rawdata = load '${SOURCE_DIR}' using PigStorage(',');

-- DESCRIBE SCHEMA
%declare PIG_SCHEMA `hdfs dfs -cat '${PIG_SCHEMA_FILE}'`

rawdata_with_schema = FOREACH rawdata GENERATE $PIG_SCHEMA;

describe rawdata_with_schema;

-- ADDING UUID AND TIMESTAMP
rawdata_uuid_timestamp = FOREACH rawdata_with_schema GENERATE *, (chararray) com.emirates.helix.pig.GenerateUUID('') AS uuid, (chararray) ToUnixTime(CurrentTime()) AS timestamp;

-- CLEANING UP 
--fs -rm -r ${TARGET_DIR};

-- SETTING SNAPPY COMPRESSION
SET mapreduce.output.fileoutputformat.compress true
SET mapreduce.output.fileoutputformat.compress.codec org.apache.hadoop.io.compress.SnappyCodec
SET avro.output.codec snappy

-- WRITING TO DECOMPOSED LAYER
STORE rawdata_uuid_timestamp INTO '${TARGET_DIR}' USING AvroStorage('schema_file','${AVRO_SCHEMA}');
--STORE rawdata_uuid_timestamp INTO '${TARGET_DIR}' USING AvroStorage('schema','${AVRO_SCHEMA}');