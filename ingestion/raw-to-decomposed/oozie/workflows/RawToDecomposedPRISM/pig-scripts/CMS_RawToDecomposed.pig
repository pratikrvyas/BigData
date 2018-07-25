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


rawdatadelimited =  FOREACH rawdata  GENERATE (chararray)$0 AS Selected_Code1,(chararray)$1 AS Selected_Name1,(chararray)$2 AS Selected_Code2,(chararray)$3 AS Selected_Name2,(chararray)$4 AS Selected_Code3,(chararray)$5 AS  Issue_Month,(chararray)$6 AS  Selected_Name3,(chararray)$7 AS  Selected_Code4,(chararray)$8 AS  Selected_Name4,(chararray)$9 AS  Selected_Code5,(chararray)$10 AS  Selected_Name5,(chararray)$11 AS  Selected_Code6,(chararray)$12 AS  Selected_Name6,(chararray)$13 AS  Flights,(chararray)$14 AS  Flights_percentage,(chararray)$15 AS  Base_Amount,(chararray)$16 AS  Base_Amount_perentage,(chararray)$17 AS  Tax_Amount,(chararray)$18 AS  Commission_Amount,(chararray)$19 AS  Average_Flight_Amount,(chararray)$20 AS  Amount_Per_KM,(chararray)$21 AS  Distance_KM,(chararray)$22 AS  Currency,(chararray)$23 AS  Month;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdatadelimited GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, *;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );