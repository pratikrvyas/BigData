-- register required libraries 
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/avro-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-core-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/jackson-mapper-asl-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/json-simple-*.jar';
REGISTER '/opt/cloudera/parcels/CDH/lib/pig/lib/snappy-java-*.jar';

data_in = LOAD '${PATH_IN}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_FILE}' );
data_out = LOAD '${PATH_OUT}' USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'no_schema_check', 'schema_file', '${AVRO_SCHEMA_FILE}' );

in_uuid = FOREACH data_in GENERATE HELIX_UUID;
out_uuid = FOREACH data_out GENERATE HELIX_UUID;

cogrp = COGROUP in_uuid BY HELIX_UUID, out_uuid BY HELIX_UUID;
minus1 = FILTER cogrp BY IsEmpty( in_uuid );
minus2 = FILTER cogrp BY IsEmpty( out_uuid );

final = UNION minus1, minus2;
STORE final INTO '${PATH_DIFF}';
