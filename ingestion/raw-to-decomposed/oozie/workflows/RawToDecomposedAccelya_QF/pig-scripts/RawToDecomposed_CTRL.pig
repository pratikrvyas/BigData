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
rawdata = LOAD  '${SOURCE_DIRECTORY2}'  USING org.apache.pig.piggybank.storage.CSVExcelStorage(';');

rawdata_column = FOREACH rawdata  GENERATE (chararray)$0 AS FILENAME,(chararray)$1 AS CRT_DT,(chararray)$2 AS UPD_DT,(chararray)$3 AS NO_OF_RECORDS,(chararray)$4 AS IND_NET_AMNT,(chararray)$5 AS AIRLINE_NET_AMNT,(chararray)$6 AS IND_GROSS_AMNT,(chararray)$7 AS AIRLINE_GROSS_AMNT,(chararray)$8 AS IND_DOC_CNT,(chararray)$9 AS AIRLINE_DOC_CNT,(chararray)$10 AS IND_YQ,(chararray)$11 AS IND_YR,(chararray)$12 AS AIRLINE_YQ,(chararray)$13 AS AIRLINE_YR,(chararray)$16 AS COUNTRY,(chararray)$17 AS PROCESSING_MONTH;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;

 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY2}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA2}' );