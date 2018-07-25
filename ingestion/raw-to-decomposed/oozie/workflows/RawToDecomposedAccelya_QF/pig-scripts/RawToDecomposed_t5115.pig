-- pig script from raw to decomposed with uuid and timestamp 
-- register required libraries 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

REGISTER pigudf.jar;

--load data
rawdata = LOAD '${SOURCE_DIRECTORY}' AS (record:chararray); 

rawdatadelimited = foreach rawdata generate (chararray)SUBSTRING(record,0,4) AS MONTH,(chararray)SUBSTRING(record,4,7) AS AIRLINE_CODE,(chararray)SUBSTRING(record,7,14) AS AGENT_CODE,(chararray)SUBSTRING(record,14,15) AS STAT,(chararray)SUBSTRING(record,15,18) AS ORIGIN_AIRPORT,(chararray)SUBSTRING(record,18,21) AS DESTINATION_AIRPORT,(chararray)SUBSTRING(record,21,22) AS CLASS_CATEGORY,(chararray)SUBSTRING(record,22,26) AS FLIGHT_MONTH,(chararray)SUBSTRING(record,26,42) AS GROSS_INDUSTRY_AMOUNT,(chararray)SUBSTRING(record,42,58) AS GROSS_AIRLINE_AMOUNT,(chararray)SUBSTRING(record,58,74) AS NET_INDUSTRY_AMOUNT,(chararray)SUBSTRING(record,74,90) AS NET_AIRLINE_AMOUNT,(chararray)SUBSTRING(record,90,97) AS INDUSTRY_DOCUMENTS,(chararray)SUBSTRING(record,97,104) AS AIRLINE_DOCUMENTS,(chararray)SUBSTRING(record,104,120) AS YQ_TAX_INDUSTRY,(chararray)SUBSTRING(record,120,136) AS YR_TAX_INDUSTRY,(chararray)SUBSTRING(record,136,152) AS YQ_TAX,(chararray)SUBSTRING(record,152,168) AS YR_TAX,(chararray)SUBSTRING(record,168,172) AS CUTP,(chararray)SUBSTRING(record,172,174) AS COUNTRY,(chararray)SUBSTRING(record,174,180) AS PROCESSING_MONTH;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;

 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );