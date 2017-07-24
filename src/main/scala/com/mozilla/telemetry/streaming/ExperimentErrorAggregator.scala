// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.{Date, Timestamp}

import com.mozilla.telemetry.pings._
import com.mozilla.telemetry.timeseries._
import org.apache.spark.sql.Row

class ExperimentErrorAggregator extends ErrorAggregator {
  override val dimensionsSchema = new SchemaBuilder()
    .add[Timestamp]("timestamp")  // Windowed
    .add[Date]("submission_date")
    .add[String]("channel")
    .add[String]("version")
    .add[String]("build_id")
    .add[String]("application")
    .add[String]("os_name")
    .add[String]("os_version")
    .add[String]("architecture")
    .add[String]("country")
    .add[Boolean]("e10s_enabled")
    .add[String]("e10s_cohort")
    .add[String]("gfx_compositor")
    .add[Boolean]("quantum_ready")
    .add[String]("experiment_id")
    .add[String]("experiment_branch")
    .build

  override def buildDimensions(meta: Meta): Array[Row] = {
    meta.experiments.map{ case (experiment_id, experiment_branch) =>
      val dimensions = new RowBuilder(dimensionsSchema)
      dimensions("timestamp") = Some(meta.normalizedTimestamp())
      dimensions("submission_date") = Some(new Date(meta.normalizedTimestamp().getTime))
      dimensions("channel") = Some(meta.normalizedChannel)
      dimensions("version") = meta.`environment.build`.flatMap(_.version)
      dimensions("build_id") = meta.`environment.build`.flatMap(_.buildId)
      dimensions("application") = Some(meta.appName)
      dimensions("os_name") = meta.`environment.system`.map(_.os.name)
      dimensions("os_version") = meta.`environment.system`.map(_.os.version)
      dimensions("architecture") = meta.`environment.build`.flatMap(_.architecture)
      dimensions("country") = Some(meta.geoCountry)
      dimensions("e10s_enabled") = meta.`environment.settings`.flatMap(_.e10sEnabled)
      dimensions("e10s_cohort") = meta.`environment.settings`.flatMap(_.e10sCohort)
      dimensions("gfx_compositor") = for {
        system <- meta.`environment.system`
        gfx <- system.gfx
        features <- gfx.features
        compositor <- features.compositor
      } yield compositor
      dimensions("quantum_ready") = meta.isQuantumReady
      dimensions("experiment_id") = Some(experiment_id)
      dimensions("experiment_branch") = Some(experiment_branch)
      dimensions.build
    }
  }
}
