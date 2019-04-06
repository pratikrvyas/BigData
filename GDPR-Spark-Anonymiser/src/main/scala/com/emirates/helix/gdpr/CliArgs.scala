// CMD LINE ARGUMENTS PARSING CLASS
package com.emirates.helix.gdpr

import org.kohsuke.args4j.Option

object CliArgs {

  @Option(name = "--GDPR_REQUEST_TYPE", required = true,
    usage = "[OPTIONAL] GDRP REQUEST TYPE (anonymize - Anonymization | delete - delete [default]) ")
  var gdpr_request_type: String = "delete"

  @Option(name = "--EMCG_UUID_FILE", required = true,
    usage = "EMCG_UUID_FILE PATH IS REQUIRED")
  var emcg_uuid_file: String = null

  @Option(name = "--COL_MASKING_FILE", required = false,
    usage = "COL_MASKING_FILE PATH IS REQUIRED")
  var col_mask_file: String = null

  // depends = Array("GDPR_REQUEST_TYPE")
  @Option(name = "--MASKING_CONFIG_FILE", required = false,
    usage = "MASKING_CONFIG_FILE PATH IS REQUIRED")
  var mask_config_file: String = null

  @Option(name = "--INPUT_PATH", required = false,
    usage = "[OPTIONAL] SPECIFY INPUT PATH IF INPUT TYPE IS NOT A TABLE")
  var input_path: String = ""

  @Option(name = "--INPUT_FORMAT", required = false,
    usage = "[OPTIONAL] SPECIFY INPUT FORMAT (parquet, avro, sequencefile, orc, textfile ...")
  var input_format: String = "parquet"

  @Option(name = "--INPUT_TABLE_NAME", required = true,
    usage = "INPUT TABLE NAME IS REQUIRED (<DB_NAME>.<TABLE_NAME>) || <TABLE_NAME>")
  var input_tbl_name: String = null

  @Option(name = "--INPUT_JOIN_KEY", required = true,
    usage = "INPUT TABLE JOIN KEY COLUMN NAME IS REQUIRED")
  var ip_join_key: String = null

  @Option(name = "--EXPLODE_JOIN_KEY", required = false,
    usage = "EXPLODE JOINS KEYS ARE REQUIRED FOR ARRAY OF MAP OR ARRAY OF STRUCT IS REQUIRED")
  var expld_join_key: String = ""

  @Option(name = "--NON_ANONYM_OUTPUT_PATH", required = true,
    usage = "OUTPUT PATH IS REQUIRED")
  var non_anon_output_path: String = null

  @Option(name = "--ANONYM_OUTPUT_PATH", required = false,
    usage = "OUTPUT PATH IS REQUIRED")
  var anon_output_path: String = null

  @Option(name = "--ANONYM_REJECTED_OUTPUT_PATH", required = false,
    usage = "OUTPUT PATH IS REQUIRED")
  var anon_rejected_output_path: String = null

  @Option(name = "--OUTPUT_FORMAT", required = false,
    usage = "OUTPUT FORMAT IS REQUIRED (sequencefile, rcfile, orc, parquet, textfile and avro)")
  var output_format: String = "parquet"

  @Option(name = "--WRITE_MODE", required = false,
    usage = "OUTPUT FILE WRITE MODE IS REQUIRED (append,overwrite)")
  var write_mode: String = "overwrite"

  @Option(name = "--COMPRESSION", required = false,
    usage = "COMPRESSION IS REQUIRED ( none, uncompressed, snappy, gzip, lzo)")
  var compression: String = "snappy"

  @Option(name = "--SPARK-PARTITIONS", required = false,
    usage = "NO.OF SPARK PARTITIONS IS REQUIRED")
  var spark_partitions: Int = 2

  @Option(name = "--VALIDATE-COUNTS", required = false,
    usage = "VALIDATE COUNTS FLAG")
  var validate_counts: String = "true"

  @Option(name = "--ENVR", required = false,
    usage = "PLEASE SPECIFY THE ENVIRONMENT")
  var envr: String = "true"

}