/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import com.mozilla.telemetry.timeseries.SchemaBuilder
import org.apache.spark.sql.types.StructType

object ExperimentsErrorAggregator extends ErrorAggregatorBase {
  override val outputPrefix = "experiment_error_aggregates/v1"
  override val queryName = "experiment_error_aggregates"

  override val countHistogramErrorsSchema: StructType = new SchemaBuilder().build

  override val dimensionsSchema: StructType = new SchemaBuilder()
    .add[Timestamp]("timestamp") // Windowed
    .add[String]("submission_date_s3")
    .add[String]("channel")
    .add[String]("version")
    .add[String]("os_name")
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
