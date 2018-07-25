package com.emirates.helix

import org.kohsuke.args4j.Option

object CliArgs {

  @Option(name = "--epic_t_int_core-hist", required = true,
    usage = "epic_t_int_core history path is missing")
  var epic_t_int_core_hist: String = null

  @Option(name = "--epic_calc_eta-hist", required = true,
    usage = "--epic_calc_eta history path is missing")
  var epic_calc_eta_hist: String = null

  @Option(name = "--epic_hold_snapshot-hist", required = true,
    usage = "epic_hold_snapshot path is missing")
  var epic_hold_snapshot_hist: String = null

  @Option(name = "--epic_t_int_core-incr", required = true,
    usage = "epic_t_int_core increment path is missing")
  var epic_t_int_core_incr: String = null

  @Option(name = "--epic_calc_eta-incr", required = true,
    usage = "--epic_calc_eta increment path is missing")
  var epic_calc_eta_incr: String = null

  @Option(name = "--epic_hold_snapshot-incr", required = true,
    usage = "epic_hold_snapshot increment path is missing")
  var epic_hold_snapshot_incr: String = null

  @Option(name = "--hist-output", required = true,
    usage = "hdfs history output path is missing")
  var hist_output_path: String = null

  @Option(name = "--incr-output", required = true,
    usage = "hdfs increment output path is missing")
  var incr_output_path: String = null

  @Option(name = "--lookback-months", required = true,
    usage = "lookback-months is missing")
  var lookback_months: Int = 3

  @Option(name = "--compression", required = true,
    usage = "compression is required")
  var compression: String = null

  @Option(name = "--spark-partitions", required = true,
    usage = "spark-partitions is required")
  var spark_partitions: Int = 2

}