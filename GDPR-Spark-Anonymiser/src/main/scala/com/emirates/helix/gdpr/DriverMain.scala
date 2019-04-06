package com.emirates.helix.gdpr

import com.emirates.helix.gdpr.GdprGenericAnonymizer._
import org.kohsuke.args4j.{CmdLineException, CmdLineParser}

object DriverMain {

  def main(args:Array[String]) : Unit = {

    // PARSING CMD LINE ARGUMENTS
    val parser = new CmdLineParser(CliArgs)
    try {
      parser.parseArgument(args: _*)
    } catch {
      case e: CmdLineException =>
        print(s"[ERROR] Parameters Missing or Wrong \n:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }


    log.info("[INFO] ANONYMIZATION PROCESS STARTED ...")
    // GETTING OR CREATING SPARK SESSION
    val spark = getSparkSession("GdprGenericAnonymizer")

    // INITIALIZING UDFS
    setUpdateMapUDF(spark)
    removeStringArrayDuplicates(spark)

    // READING EMCG UUID FILE
    log.info("[INFO] READING EMCG UUID FILE ...")
    val ip_emcg_uuids = readEmcgUuidFile(CliArgs.emcg_uuid_file,spark)
    log.info("[INFO] READING EMCG UUID FILE SUCCESSFUL")

    // CREATING TABLE OR VIEW
    ip_emcg_uuids.createOrReplaceTempView("A_UUIDS_TBL")

    if(CliArgs.gdpr_request_type.equalsIgnoreCase("anonymize") || CliArgs.gdpr_request_type.equalsIgnoreCase("delete+anonymize")) {

      // SOME PRE CHECKS
      if((CliArgs.col_mask_file == null || CliArgs.col_mask_file.isEmpty) && (CliArgs.mask_config_file == null ||
        CliArgs.mask_config_file.isEmpty)){
        log.info("[ERROR] MISSING PARAMETERS IN SPARK-SUBMIT CMD (COLUMN MASKING FILE OR MASKING CONFIG FILE)")
        System.exit(1)
      }

      // READING MASKING PROPERTY FILE
      log.info("[INFO] READING MASKING PROPERTY FILE ...")
      val (default_masking_value, gdpr_restricted_flag_col_name, gdpr_restricted_flag_col_value,
      gdpr_reason_code_col_name, gdpr_reason_code_col_value) = readMaskConfigFile(CliArgs.mask_config_file, spark)
      log.info("[INFO] READING MASKING PROPERTY FILE SUCCESSFUL")

      // READING COLUMNS TO ANONYMIZATION
      log.info("[INFO] READING COLUMN MASKING FILE ...")
      val (m_cols_list,m_col_type_list,m_col_key_list,m_start_ind_list,m_len_list,m_mask_val_list) = readColMaskFile(CliArgs.col_mask_file,spark)
      log.info("[INFO] READING COLUMN MASKING FILE SUCCESSFUL")

      // READING AND FILTERING NON ANONYMIZED RECORDS FROM INPUT DATA
      log.info("[INFO] READING AND FILTERING NON ANONYMIZED RECORDS FROM INPUT DATA ...")
      if (!CliArgs.input_path.isEmpty) {
        val ip_df = spark.read.format(CliArgs.input_format).load(CliArgs.input_path)
        ip_df.createOrReplaceGlobalTempView(CliArgs.input_tbl_name)
      }
      val ip_data_na_uuids = filterNonAnonymData(CliArgs.input_tbl_name, CliArgs.ip_join_key, spark)
      log.info("[INFO] READING AND FILTERING NON ANONYMIZED RECORDS FROM INPUT DATA SUCCESSFUL")

      // GETTING COLUMN NAMES FROM INPUT DATA
      val ip_cols_list = getCols(ip_data_na_uuids)

      // CHECK IF SPECIFIED ANONYMIZATION COLUMNS ARE PRESENT IN INPUT DATA OR NOT
      if (!validateColMapping(ip_cols_list, m_cols_list)) System.exit(1)

      // BUILDING DYNAMIC QUERY FOR ANONYMIZATION
      log.info("[INFO] BUILDING DYNAMIC QUERY FOR ANONYMIZATION ...")
      // BUILDING DYNAMIC QUERY FOR ANONYMIZATION
      log.info("[INFO] BUILDING DYNAMIC QUERY FOR ANONYMIZATION ...")
      val (anon_sql_qry,array_of_map,array_of_struct) = buildAnonymizedQry(ip_cols_list,m_cols_list,m_col_type_list,m_col_key_list,m_start_ind_list,m_len_list,m_mask_val_list
        ,default_masking_value,gdpr_restricted_flag_col_value,gdpr_restricted_flag_col_name,gdpr_reason_code_col_value,gdpr_reason_code_col_name
        ,CliArgs.input_tbl_name,CliArgs.ip_join_key,"emcg_uuid",ip_data_na_uuids)

      log.info("[INFO] ANONYMIZATION SQL QUERY : " + anon_sql_qry)
      log.info("[INFO] BUILDING DYNAMIC QUERY FOR ANONYMIZATION SUCCESSFUL")

      // RUNNING ANONYMIZATION SQL QUERY
      log.info("[INFO] RUNNING ANONYMIZATION SQL QUERY ...")
      val anon_df = if(array_of_map.size >0) spark.sql(anon_sql_qry).cache() else spark.sql(anon_sql_qry).dropDuplicates().cache()
      log.info("[INFO] RUNNING ANONYMIZATION SQL QUERY SUCCESSFUL")

      // CHECKING JOINKEYS FOR NULL

      // val dummy_schema = StructType(
      //  StructField("Emp_Salary", LongType, false) :: Nil)

      // CHANGES WHEN JOIN RELATION IS ADDED IN CMD LINE PARAMS - REMEMBER
      val anon_rejected_df = anonymizeFullColumns(joinKeyNullRec(anon_df,CliArgs.ip_join_key,CliArgs.expld_join_key,spark),array_of_map,default_masking_value) // spark.createDataFrame(spark.sparkContext.emptyRDD[Row],dummy_schema) -- create emtpy dataframe
      // log.info("[INFO] TOTAL COUNT OF REJECTED RECORDS : " + anon_rejected_df.count)

      // ARRAY MAP COMPLEX COLUMNS ANONYMIZATION
      // Need add support to array of struct
      val array_map_df = if(array_of_map.size > 0) recursiveMapComplexTypes(anon_df,null,CliArgs.ip_join_key,CliArgs.expld_join_key,array_of_map,m_cols_list,m_col_key_list,
        m_col_type_list,m_start_ind_list,m_len_list,m_mask_val_list,default_masking_value,"",spark) else anon_df

      // CHECKPOINT
      // spark.sparkContext.setCheckpointDir("/tmp/gdpr/checkpoints/" +CliArgs.input_tbl_name )
      // array_map_df.checkpoint(false)

      // TEMP WRITING
      log.info("[INFO] WRITING TO TEMP DIRECTORY FOR NEXT STEPS")
      if(array_of_map.size > 0) anon_df.write.mode("overwrite").parquet("/tmp/"+ CliArgs.envr + "/gdpr/anon_simple_type/" + CliArgs.input_tbl_name)
      if(array_of_map.size > 0) array_map_df.write.mode("overwrite").parquet("/tmp/"+ CliArgs.envr + "/gdpr/anon_map_type/" + CliArgs.input_tbl_name)

      val anon_df_new = if(array_of_map.size > 0) spark.read.parquet("/tmp/"+ CliArgs.envr + "/gdpr/anon_simple_type/" + CliArgs.input_tbl_name) else anon_df
      val array_map_df_new = if(array_of_map.size > 0) spark.read.parquet("/tmp/"+ CliArgs.envr + "/gdpr/anon_map_type/" + CliArgs.input_tbl_name) else anon_df

      // Need add support to array of struct
      val cmplx_join_df = if(array_of_map.size > 0) explodeJoin(anon_df_new,array_map_df_new,CliArgs.ip_join_key,CliArgs.expld_join_key) else anon_df

      // UNION THE ANONYMIZED DATAFRAME WITH NON ANONYMIZED DATAFRAME
      log.info("[INFO] UNION THE ANONYMIZED DATAFRAME WITH NON ANONYMIZED DATAFRAME ...")
      val op_df = if(CliArgs.gdpr_request_type.equals("anonymize")) unionByName(ip_data_na_uuids, cmplx_join_df) else cmplx_join_df
      log.info("[INFO] UNION THE ANONYMIZED DATAFRAME WITH NON ANONYMIZED DATAFRAME SUCCESSFUL")

      if(CliArgs.validate_counts.equalsIgnoreCase("true")) {

        // TEST THE COUNTS IF MATCHING WRITE OTHERWISE ABORT
        log.info("[INFO] VALIDATING INPUT DATA COUNT AND OUTPUT DATA COUNT ...")
        // log.info("[INFO] ip_data_na_uuids schema : " + ip_data_na_uuids.printSchema())
        // log.info("[INFO] anon_df schema : " + anon_df.printSchema())
        // log.info("[INFO] array_map_df  : " + array_map_df.printSchema())

        val op_df_cnt = if (CliArgs.gdpr_request_type.equals("anonymize")) op_df.count() else ip_data_na_uuids.count() + cmplx_join_df.count() + anon_rejected_df.count()
        if (!validateCounts(op_df_cnt, CliArgs.input_tbl_name, spark)) System.exit(1)

        // FINAL SUMMARY OF GDPR ANONYMIZATION
        log.info("################################################################################")
        log.info("------------------FINAL SUMMARY OF GDPR ANONYMIZATION---------------------------")
        log.info("################################################################################")
        log.info(s"TOTAL RECORD COUNT : $op_df_cnt")
        log.info("NON-ANONYMIZED RECORD COUNT : " + ip_data_na_uuids.count)
        log.info(s"ANONYMIZED RECORD COUNT : " + cmplx_join_df.count)
        log.info(s"REJECTED RECORD COUNT : " + anon_rejected_df.count())
        log.info("--------------------------------------------------------------------------------")
        log.info("################################################################################")
        log.info("---------------------------COMPLETED SUCCESSFULLY-------------------------------")
        log.info("################################################################################")
        log.info("[INFO] VALIDATING INPUT DATA COUNT AND OUTPUT DATA COUNT SUCCESSFUL")

      }
      val spark_part = if(CliArgs.gdpr_request_type.equals("anonymize")) CliArgs.spark_partitions else 4

      // WRITING DATA TO HDFS
      log.info("[INFO] WRITING DATA TO OUTPUT PATH ...")
      op_df.select(ip_cols_list.map(org.apache.spark.sql.functions.col):_*).repartition(spark_part).write.format(CliArgs.output_format)
        .option("compression", CliArgs.compression)
        .mode(CliArgs.write_mode)
        .save(CliArgs.anon_output_path)
      log.info("[INFO] WRITING DATA TO OUTPUT PATH SUCCESSFUL")

      // WRITING REJECTED DATA TO HDFS
      if(array_of_map.size > 0 && anon_rejected_df.count > 0) {
        log.info("[INFO] WRITING REJECTED DATA TO REJECTED OUTPUT PATH ... " + anon_rejected_df.count)
        anon_rejected_df.repartition(1).write.mode("overwrite") parquet (CliArgs.anon_rejected_output_path)
        log.info("[INFO] WRITING REJECTED DATA TO REJECTED OUTPUT PATH SUCCESSFUL")
      }

    }
    if(CliArgs.gdpr_request_type.equalsIgnoreCase("delete") || CliArgs.gdpr_request_type.equalsIgnoreCase("delete+anonymize")){
      // READING AND FILTERING NON ANONYMIZED RECORDS FROM INPUT DATA
      log.info("[INFO] READING AND FILTERING NON ANONYMIZED RECORDS FROM INPUT DATA ...")
      if (!CliArgs.input_path.isEmpty) {
        val ip_df = spark.read.format(CliArgs.input_format).load(CliArgs.input_path)
        ip_df.createOrReplaceGlobalTempView(CliArgs.input_tbl_name)
      }
      val ip_data_na_uuids = filterNonAnonymData(CliArgs.input_tbl_name, CliArgs.ip_join_key, spark)
      log.info("[INFO] READING AND FILTERING NON ANONYMIZED RECORDS FROM INPUT DATA SUCCESSFUL")

      // WRITING DATA TO HDFS
      log.info("[INFO] WRITING DATA TO OUTPUT PATH ...")
      ip_data_na_uuids.coalesce(CliArgs.spark_partitions).write.format(CliArgs.output_format)
        .option("compression", CliArgs.compression)
        .mode(CliArgs.write_mode)
        .save(CliArgs.non_anon_output_path)
      log.info("[INFO] WRITING DATA TO OUTPUT PATH SUCCESSFUL")
    }

  }

}