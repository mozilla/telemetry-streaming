// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.scalatest._

class TestAggregator extends FlatSpec with Matchers{

  implicit val formats = DefaultFormats
  val k = TestUtils.scalarValue
  val app = TestUtils.application

  "The aggregator" should "sum metrics over a set of dimensions" in {
    val spark = SparkSession.builder()
      .appName("Error Aggregates")
      .master("local[1]")
      .getOrCreate()
    import spark.implicits._
    val messages =
      (TestUtils.generateCrashMessages(k)
        ++ TestUtils.generateMainMessages(k)).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)
    df.count() should be (1)
    val inspectedFields = List(
      "submission_date",
      "channel",
      "version",
      "build_id",
      "application",
      "os_name",
      "os_version",
      "architecture",
      "country",
      "quantum_ready",
      "main_crashes",
      "content_crashes",
      "gpu_crashes",
      "plugin_crashes",
      "gmplugin_crashes",
      "content_shutdown_crashes",
      "count",
      "usage_hours",
      "browser_shim_usage_blocked",
      "experiment_id",
      "experiment_branch",
      "e10s_enabled",
      "e10s_cohort",
      "gfx_compositor",
      "input_event_response_coalesced_ms_main_above_150",
      "input_event_response_coalesced_ms_main_above_250",
      "input_event_response_coalesced_ms_main_above_2500",
      "input_event_response_coalesced_ms_content_above_150",
      "input_event_response_coalesced_ms_content_above_250",
      "input_event_response_coalesced_ms_content_above_2500"
    )
    val row = df.select(inspectedFields(0), inspectedFields.drop(1):_*).first()
    val results = 0.to(inspectedFields.length-1).map(x => (inspectedFields(x), row(x))).toMap

    results("submission_date").toString should be ("2016-04-07")
    results("channel") should be (app.channel)
    results("version") should be (app.version)
    results("build_id") should be (app.buildId)
    results("application") should be (app.name)
    results("os_name") should be ("Linux")
    results("os_version") should be (s"${k}")
    results("architecture") should be (app.architecture)
    results("country") should be ("IT")
    results("quantum_ready") should equal (true)
    results("main_crashes") should be (k)
    results("content_crashes") should be (k)
    results("gpu_crashes") should be (k)
    results("plugin_crashes") should be (k)
    results("gmplugin_crashes") should be (k)
    results("content_shutdown_crashes") should be (k)
    results("count") should be (k * 2)
    results("usage_hours") should be (k.toFloat)
    results("browser_shim_usage_blocked") should be (k)
    results("experiment_id") should be ("experiment1")
    results("experiment_branch") should be ("control")
    results("e10s_enabled") should equal (true)
    results("e10s_cohort") should be ("test")
    results("gfx_compositor") should be ("opengl")
    results("input_event_response_coalesced_ms_main_above_150") should be (42 * 14)
    results("input_event_response_coalesced_ms_main_above_250") should be (42 * 12)
    results("input_event_response_coalesced_ms_main_above_2500") should be (42 * 9)
    results("input_event_response_coalesced_ms_content_above_150") should be (42 * 4)
    results("input_event_response_coalesced_ms_content_above_250") should be (42 * 3)
    results("input_event_response_coalesced_ms_content_above_2500") should be (42 * 2)

    df.where("window is null").count() should be (0)
  }
}
