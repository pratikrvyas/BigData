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
rawdata = LOAD  '${SOURCE_DIRECTORY}'  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

rawdata_column = FOREACH rawdata  GENERATE (chararray)$0 AS Sector_Code,(chararray)$1 AS Sector_board_Point,(chararray)$2 AS Sector_Off_Point,(chararray)$3 AS Combined_Sector,(chararray)$4 AS EK_Online_Offline_Flag,(chararray)$5 AS  EK_Online_Offline_Start_Date,(chararray)$6 AS EK_Online_Offline_end_Date,(chararray)$7 AS Sector_Distance,(chararray)$8 AS GCD,(chararray)$9 AS MPM_Distance,(chararray)$10 AS Sector_Distance_Effective_Start_Date,(chararray)$11 AS Sector_Distance_Effective_End_Date,(chararray)$12 AS Source,(chararray)$13 AS Effective_Start_Date,(chararray)$14 AS Effective_End_Date,(chararray)$15 AS  Last_Updated_Date;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );