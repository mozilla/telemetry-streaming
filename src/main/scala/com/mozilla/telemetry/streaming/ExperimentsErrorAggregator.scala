// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.{Date, Timestamp}
import com.mozilla.telemetry.timeseries.SchemaBuilder

class ExperimentErrorAggregator extends ErrorAggregator {
  override val outputPrefix = "experiment_error_aggregates/v1"
  override val queryName = "experiment_error_aggregates"
  override val defaultNumFiles = 20

  override val countHistogramErrorsSchema = (new SchemaBuilder()).build
  override val thresholdHistogramsSchema = (new SchemaBuilder()).build

  override val dimensionsSchema = new SchemaBuilder()
    .add[Timestamp]("timestamp")  // Windowed
    .add[Date]("submission_date")
    .add[String]("channel")
    .add[String]("version")
    .add[String]("os_name")
    .add[String]("country")
    .add[String]("experiment_id")
    .add[String]("experiment_branch")
    .build

  override val metricsSchema = new SchemaBuilder()
    .add[Float]("usage_hours")
    .add[Int]("count")
    .add[Int]("subsession_count")
    .add[Int]("main_crashes")
    .add[Int]("content_crashes")
    .add[Int]("gpu_crashes")
    .add[Int]("plugin_crashes")
    .add[Int]("gmplugin_crashes")
    .add[Int]("content_shutdown_crashes")
    .build

  override val statsSchema = metricsSchema
}
