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

rawdata_column = FOREACH rawdata  GENERATE (chararray)$0 AS Countrycode,(chararray)$1 AS Country,(chararray)$2 AS Fin_ID,(chararray)$3 AS LC,(chararray)$4 AS Compartment,(chararray)$5 AS Year,(chararray)$6 AS APR_OTA_REV_TGT_AED,(chararray)$7 AS  MAY_OTA_REV_TGT_AED,(chararray)$8 AS  JUN_OTA_REV_TGT_AED,(chararray)$9 AS JUL_OTA_REV_TGT_AED,(chararray)$10 AS AUG_OTA_REV_TGT_AED,(chararray)$11 AS SEP_OTA_REV_TGT_AED,(chararray)$12 AS  OCT_OTA_REV_TGT_AED,(chararray)$13 AS  NOV_OTA_REV_TGT_AED,(chararray)$14 AS DEC_OTA_REV_TGT_AED,(chararray)$15 AS JAN_OTA_REV_TGT_AED,(chararray)$16 AS FEB_OTA_REV_TGT_AED,(chararray)$17 AS  MAR_OTA_REV_TGT_AED,(chararray)$18 AS  APR_CTY_PAX_TGT,(chararray)$19 AS MAY_CTY_PAX_TGT,(chararray)$20 AS JUN_CTY_PAX_TGT,(chararray)$21 AS JUL_CTY_PAX_TGT,(chararray)$22 AS  AUG_CTY_PAX_TGT,(chararray)$23 AS  SEP_CTY_PAX_TGT,(chararray)$24 AS OCT_CTY_PAX_TGT,(chararray)$25 AS NOV_CTY_PAX_TGT,(chararray)$26 AS DEC_CTY_PAX_TGT,(chararray)$27 AS  JAN_CTY_PAX_TGT,(chararray)$28 AS  FEB_CTY_PAX_TGT,(chararray)$29 AS MAR_CTY_PAX_TGT,(chararray)$30 AS APR_OTA_REV_TGT_LC,(chararray)$31 AS MAY_OTA_REV_TGT_LC,(chararray)$32 AS  JUN_OTA_REV_TGT_LC,(chararray)$33 AS  JUL_OTA_REV_TGT_LC,(chararray)$34 AS AUG_OTA_REV_TGT_LC,(chararray)$35 AS SEP_OTA_REV_TGT_LC,(chararray)$36 AS OCT_OTA_REV_TGT_LC,(chararray)$37 AS  NOV_OTA_REV_TGT_LC,(chararray)$38 AS  DEC_OTA_REV_TGT_LC,(chararray)$39 AS JAN_OTA_REV_TGT_LC,(chararray)$40 AS FEB_OTA_REV_TGT_LC,(chararray)$41 AS MAR_OTA_REV_TGT_LC,(chararray)$42 AS  APR_CTY_REV_TGT_AED,(chararray)$43 AS  MAY_CTY_REV_TGT_AED,(chararray)$44 AS JUN_CTY_REV_TGT_AED,(chararray)$45 AS JUL_CTY_REV_TGT_AED,(chararray)$46 AS AUG_CTY_REV_TGT_AED,(chararray)$47 AS  SEP_CTY_REV_TGT_AED,(chararray)$48 AS  OCT_CTY_REV_TGT_AED,(chararray)$49 AS NOV_CTY_REV_TGT_AED,(chararray)$50 AS DEC_CTY_REV_TGT_AED,(chararray)$51 AS JAN_CTY_REV_TGT_AED,(chararray)$52 AS  FEB_CTY_REV_TGT_AED,(chararray)$53 AS  MAR_CTY_REV_TGT_AED,(chararray)$54 AS APR_CTY_REV_TGT_LC,(chararray)$55 AS MAY_CTY_REV_TGT_LC,(chararray)$56 AS JUN_CTY_REV_TGT_LC,(chararray)$57 AS  JUL_CTY_REV_TGT_LC,(chararray)$58 AS  AUG_CTY_REV_TGT_LC,(chararray)$59 AS SEP_CTY_REV_TGT_LC,(chararray)$60 AS OCT_CTY_REV_TGT_LC,(chararray)$61 AS  NOV_CTY_REV_TGT_LC,(chararray)$62 AS  DEC_CTY_REV_TGT_LC,(chararray)$63 AS JAN_CTY_REV_TGT_LC,(chararray)$64 AS FEB_CTY_REV_TGT_LC,(chararray)$65 AS MAR_CTY_REV_TGT_LC;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );