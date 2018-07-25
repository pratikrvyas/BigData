-- pig script from raw to decomposed with uuid and timestamp 
-- register required libraries 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

REGISTER  pigudf.jar;

--load data
rawdata = LOAD '${SOURCE_DIRECTORY}' USING PigStorage('\n') AS (record:chararray);  

rawdatadelimited = foreach rawdata generate (chararray)SUBSTRING(record,0,6) AS PNR,(chararray)SUBSTRING(record,6, 14) AS BOOKING_DATE,(chararray)SUBSTRING(record,14,22) AS PNR_CRT_DATE,(chararray)SUBSTRING(record,22,30) AS AGENT_NO,(chararray)SUBSTRING(record,30,32) AS CRS,(chararray)SUBSTRING(record,32,41) AS PCC,(chararray)SUBSTRING(record,41,43) AS AGENT_COUNTRY_CODE,(chararray)SUBSTRING(record,43, 46) AS AGENT_CITY_CODE,(chararray)SUBSTRING(record,46,86) AS AGENT_CITY_NAME,(chararray)SUBSTRING(record,86,93) AS FLOWN_SECTOR,(chararray)SUBSTRING(record,93,101) AS FLIGHT_LOC_DATE,(chararray)SUBSTRING(record,101,105) AS FLIGHT_LOC_TIME, (chararray)SUBSTRING(record,105,113) AS FLIGHT_GMT_DATE,(chararray)SUBSTRING(record,113,117) AS FLIGHT_GMT_TIME,(chararray)SUBSTRING(record,117,123) AS STOPOVER_DURATION,(chararray)SUBSTRING(record,123,127) AS FLYING_DURATION,(chararray)SUBSTRING(record,127,135) AS ARRIVAL_LOC_DATE,(chararray)SUBSTRING(record,135,139) AS ARRIVAL_LOC_TIME,(chararray)SUBSTRING(record,139,147) AS ARRIVAL_GMT_DATE,(chararray)SUBSTRING(record,147,151) AS ARRIVAL_GMT_TIME,(chararray)SUBSTRING(record,151,154) AS MARKETING_CARRIER,(chararray)SUBSTRING(record,154,158) AS MARKETING_CARRIER_NO,(chararray)SUBSTRING(record,158,161) AS OPERATING_CARRIER,(chararray)SUBSTRING(record,161,165) AS OPERATING_CARRIER_NO,(chararray)SUBSTRING(record,165,167) AS BOOKING_CLASS,(chararray)SUBSTRING(record,167,168) AS COMPARTMENT,(chararray)SUBSTRING(record,168,170) AS BOOKING_STATUS,(chararray)SUBSTRING(record,170,173) AS AIRCRAFT_TYPE,(chararray)SUBSTRING(record,173,176) AS PAX,(chararray)SUBSTRING(record,176,179) AS SECTOR_ORIGIN,(chararray)SUBSTRING(record,179,182) AS SECTOR_DESTINATION,(chararray)SUBSTRING(record,182,183) AS ACTION,(chararray)SUBSTRING(record,183,187) AS SEQUENCE_NUMBER,(chararray)SUBSTRING(record,187,189) AS BATCH_NUMBER,(chararray)SUBSTRING(record,189,195) AS PROCESSING_DATE;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );