/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import com.mozilla.telemetry.timeseries._
import org.apache.spark.sql.types.StructType

object ErrorAggregator extends ErrorAggregatorBase {
  override val queryName = "error_aggregator"
  override val outputPrefix = "error_aggregator/v2"

  override val countHistogramErrorsSchema: StructType = new SchemaBuilder()
    .add[Int]("BROWSER_SHIM_USAGE_BLOCKED")
    .add[Int]("PERMISSIONS_SQL_CORRUPTED")
    .add[Int]("DEFECTIVE_PERMISSIONS_SQL_REMOVED")
    .add[Int]("SLOW_SCRIPT_NOTICE_COUNT")
    .add[Int]("SLOW_SCRIPT_PAGE_COUNT")
    .build

  override val dimensionsSchema: StructType = new SchemaBuilder()
    .add[Timestamp]("timestamp")  // Windowed
    .add[String]("submission_date_s3")
    .add[String]("channel")
    .add[String]("version")
    .add[String]("display_version")
    .add[String]("build_id")
    .add[String]("application")
    .add[String]("os_name")
    .add[String]("os_version")
    .add[String]("architecture")
    .add[String]("country")
    .add[String]("experiment_id")
    .add[String]("experiment_branch")
    .build

  override val metricsSchema: StructType = new SchemaBuilder()
    .add[Float]("usage_hours")
    .add[Int]("count")
    .add[Int]("main_crashes")
    .add[Int]("startup_crashes")
    .add[Int]("content_crashes")
    .add[Int]("gpu_crashes")
    .add[Int]("plugin_crashes")
    .add[Int]("gmplugin_crashes")
    .add[Int]("content_shutdown_crashes")
    .build
}
