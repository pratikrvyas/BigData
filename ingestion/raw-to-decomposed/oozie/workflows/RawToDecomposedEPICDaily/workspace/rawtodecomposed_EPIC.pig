-- Author      : Manu Mukundan
-- Date         : 10/17/2017
-- Project      : HELIX-OpsEfficiency
-- Location   : BAS 20th floor

-- pig script from raw to decomposed with uuid and timestamp 
-- register required libraries
REGISTER pigudf.jar;
		
-- enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;
SET 

-- load raw epic_hold_snapshot data
avrodata_hs = LOAD  '${HS_SOURCE}'  USING AvroStorage('schema_file','${HS_SOURCE_SCHEMA}');

-- add uuid and timestamp
avrodatawithid_hs = FOREACH avrodata_hs GENERATE com.emirates.helix.pig.GenerateUUID('') as UUID,  (chararray)ToUnixTime(CurrentTime()) as TIMESTAMP, *;


-- load raw epic_calc_eta data
avrodata_ce = LOAD  '${CE_SOURCE}'  USING AvroStorage('schema_file','${CE_SOURCE_SCHEMA}');

-- add uuid and timestamp
avrodatawithid_ce = FOREACH avrodata_ce GENERATE com.emirates.helix.pig.GenerateUUID('') as UUID,  (chararray)ToUnixTime(CurrentTime()) as TIMESTAMP, *;

 -- store epic_hold_snapshot data in decomposed layer 
STORE avrodatawithid_hs INTO '${HS_TARGET_DIRECTORY}' USING AvroStorage('schema','${HS_TARGET_SCHEMA}');

 -- store epic_calc_eta data in decomposed layer 
STORE avrodatawithid_ce INTO '${CE_TARGET_DIRECTORY}' USING AvroStorage('schema','${CE_TARGET_SCHEMA}');