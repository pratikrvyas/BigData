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
rawdata = LOAD '${SOURCE_DIRECTORY}'   USING org.apache.pig.piggybank.storage.CSVExcelStorage('|', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER');


rawdatadelimited =  FOREACH rawdata  GENERATE (chararray)$0 AS Customer,(chararray)$1 AS EK_Company_Code,(chararray)$2 AS Ticket_Number,(chararray)$3 AS Validating_Carrier_Code,(chararray)$4 AS Validating_Carrier,(chararray)$5 AS  Issue_Date,(chararray)$6 AS  Departure_Date,(chararray)$7 AS  Credit_Card,(chararray)$8 AS  Base_Amount,(chararray)$9 AS  Tax,(chararray)$10 AS  Commission_Amount,(chararray)$11 AS Commission_Amount_percentage,(chararray)$12 AS  Ticket_Amount,(chararray)$13 AS  Currency,(chararray)$14 AS  Month;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );