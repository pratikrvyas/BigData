--	Pig script to move Sales Channel data from Raw to Decomposed layer
--		Loads data from raw directory
--		Splits data into columns
--		Adds UUID and timestamp as per standard
--		Stores data in decomposed directory

--	Register required libraries
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

--	Register EK jar
REGISTER pigudf.jar;

--	Load data
rawdata = LOAD '${SOURCE_DIRECTORY}' USING PigStorage('\t') AS (record:chararray); 

--	Split tab-delimited data by column index
rawdata_column = FOREACH rawdata GENERATE 
--	Add helix_uuid and helix_timestamp
	com.emirates.helix.pig.GenerateUUID('') AS HELIX_UUID,
	(chararray)ToUnixTime(CurrentTime()) AS HELIX_TIMESTAMP,
	$0 AS SALES_CHANNEL_TYPE,
	$1 AS SALES_CHANNEL_CODE,
	$2 AS SALES_CHANNEL_DESCRIPTION,
	$3 AS SALES_CHANNEL_SUB_TYPE,
	$4 AS SALES_CHANNEL_SUB_TYPE_CODE,
	$5 AS SALES_CHANNEL_SUB_TYPE_DESC,
	$6 AS CREATED_DATE,
	$7 AS CHANGED_DATE,
	$8 AS SOURCE;

--	Split flat-file data by character index
--rawdata_column = FOREACH rawdata GENERATE 
-- --	Add helix_uuid and helix_timestamp
-- 	com.emirates.helix.pig.GenerateUUID('') AS HELIX_UUID,
-- 	(chararray)ToUnixTime(CurrentTime()) AS HELIX_TIMESTAMP,
-- 	(chararray)SUBSTRING(record,0,10) AS SALES_CHANNEL_TYPE,
-- 	(chararray)SUBSTRING(record,10,40) AS SALES_CHANNEL_CODE,
-- 	(chararray)SUBSTRING(record,40,100) AS SALES_CHANNEL_DESCRIPTION,
-- 	(chararray)SUBSTRING(record,140,170) AS SALES_CHANNEL_SUB_TYPE,
-- 	(chararray)SUBSTRING(record,170,200) AS SALES_CHANNEL_SUB_TYPE_CODE,
-- 	(chararray)SUBSTRING(record,200,300) AS SALES_CHANNEL_SUB_TYPE_DESC,
-- 	(chararray)SUBSTRING(record,300,320) AS CREATED_DATE,
-- 	(chararray)SUBSTRING(record,320,340) AS CHANGED_DATE,
-- 	(chararray)SUBSTRING(record,340,370) AS SOURCE;

--	Enable compression
--	NB - possible change to snappy?
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;

--	Store data
STORE rawdata_column INTO '${TARGET_DIRECTORY}' 
	USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );
