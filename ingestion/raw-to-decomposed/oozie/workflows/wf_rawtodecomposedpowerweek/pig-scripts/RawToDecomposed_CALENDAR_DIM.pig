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
rawdata = LOAD  '${SOURCE_DIRECTORY}'  USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

rawdata_column = FOREACH rawdata  GENERATE (chararray)$0 AS CALENDAR_DATE,(chararray)$1 AS DAY_OF_WEEK,(chararray)$2 AS DAY_OF_MONTH,(chararray)$3 AS DAY_OF_YEAR,(chararray)$4 AS DAY_ABBREVIATION,(chararray)$5 AS  SAME_DAY_IN_LAST_YEAR,(chararray)$6 AS WEEK_DAY_FLAG,(chararray)$7 AS SEASONALITY_INDICATOR,(chararray)$8 AS WEEK_OF_MONTH,(chararray)$9 AS WEEK_OF_YEAR,(chararray)$10 AS WEEK_BEGIN_DATE,(chararray)$11 AS WEEK_END_DATE,(chararray)$12 AS POWER_WEEK,(chararray)$13 AS POWER_WEEK_ST_DATE,(chararray)$14 AS POWER_WEEK_END_DATE,(chararray)$15 AS POWER_WEEK_YEAR_ST_DATE,(chararray)$16 AS POWER_WEEK_YEAR_END_DATE,(chararray)$17 AS MONTH_NUMBER,(chararray)$18 AS MONTH_OF_QUARTER,(chararray)$19 AS MONTH_BEGIN_DATE,(chararray)$20 AS MONTH_END_DATE,(chararray)$21 AS MONTH_END_FLAG,(chararray)$22 AS MONTH_NAME,(chararray)$23 AS MONTH_ABBREVIATION,(chararray)$24 AS QUARTER_NUMBER,(chararray)$25 AS QUARTER_NAME,(chararray)$26 AS QUARTER_BEGIN_DATE,(chararray)$27 AS QUARTER_END_DATE,(chararray)$28 AS YEAR_BEGIN_DATE,(chararray)$29 AS YEAR_END_DATE,(chararray)$30 AS YEAR,(chararray)$31 AS YEAR_MONTH,(chararray)$32 AS YEAR_FISCAL_CODE,(chararray)$33 AS YEAR_FISCAL,(chararray)$34 AS FISCAL_YEAR_BEGIN_DATE,(chararray)$35 AS FISCAL_YEAR_END_DATE,(chararray)$36 AS FISCAL_MONTH,(chararray)$37 AS FISCAL_QUARTER,(chararray)$38 AS MON_YY,(chararray)$39 AS LAST_DAY,(chararray)$40 AS POWER_YEAR_WEEK,(chararray)$41 AS POWER_YEAR_LAST_WEEK,(chararray)$42 AS POWER_YEAR_LAST_2_WEEK,(chararray)$43 AS POWER_YEAR_NEXT_WEEK,(chararray)$44 AS POWER_LAST_YEAR_WEEK,(chararray)$45 AS POWER_LAST_YEAR_LAST_WEEK,(chararray)$46 AS POWER_LAST_YEAR_NEXT_WEEK,(chararray)$47 AS YEAR_WEEK,(chararray)$48 AS LAST_YEAR_WEEK,(chararray)$49 AS YEAR_LAST_WEEK,(chararray)$50 AS YEAR_LAST_2_WEEK,(chararray)$51 AS YEAR_LAST_3_WEEK,(chararray)$52 AS YEAR_LAST_4_WEEK,(chararray)$53 AS YEAR_NEXT_QUARTER,(chararray)$54 AS LAST_YEAR,(chararray)$55 AS YEAR_QUARTER_NUMBER,(chararray)$56 AS LAST_YEAR_QUARTER,(chararray)$57 AS YEAR_LAST_QUARTER,(chararray)$58 AS YEAR_LAST_MONTH,(chararray)$59 AS LAST_YEAR_MONTH,(chararray)$60 AS YEAR_NEXT_MONTH,(chararray)$61 AS YEAR_NEXT_2_MONTH,(chararray)$62 AS YEAR_NEXT_3_MONTH,(chararray)$63 AS LAST_YEAR_NEXT_MONTH,(chararray)$64 AS LAST_YEAR_FISCAL_CODE,(chararray)$65 AS POWER_YEAR_LAST_3_WEEK,(chararray)$66 AS POWER_YEAR_LAST_4_WEEK,(chararray)$67 AS YEAR_NEXT_4_MONTH,(chararray)$68 AS YEAR_NEXT_5_MONTH,(chararray)$69 AS YEAR_NEXT_WEEK,(chararray)$70 AS LAST_YEAR_SAME_DAY_OF_WEEK,(chararray)$71 AS POWER_YEAR_CODE,(chararray)$72 AS POWER_YEAR_DESC ;

-- add uuid and timestamp
rawdatawithid = FOREACH rawdata_column GENERATE com.emirates.helix.pig.GenerateUUID('') as uuid,  (chararray)ToUnixTime(CurrentTime()) as timestamp, * ;

--enabling deflate compression
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.DeflateCodec;
SET avro.output.codec deflate;


 -- store data in decomposed layer 
STORE rawdatawithid INTO '${TARGET_DIRECTORY}' 
		USING org.apache.pig.piggybank.storage.avro.AvroStorage( 'schema', '${TARGET_SCHEMA}' );