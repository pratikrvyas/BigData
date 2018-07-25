-- register required libraries 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';

-- load data 
avrodata = LOAD '${SOURCE_DIRECTORY}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_FILE}' );

-- store data in decomposed layer 
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;

STORE avrodata INTO '${TARGET_DIRECTORY}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${AVRO_SCHEMA}' );
