// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TestAggregator extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val formats = DefaultFormats
  val k = TestUtils.scalarValue
  val app = TestUtils.application

  val spark = SparkSession.builder()
    .appName("Error Aggregates")
    .master("local[1]")
    .getOrCreate()

  override def afterAll() {
    spark.stop()
  }

  "The aggregator" should "sum metrics over a set of dimensions" in {
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
      "input_event_response_coalesced_ms_content_above_2500",
      "first_paint",
      "first_subsession_count",
      "window_start",
      "window_end"
    )
    val row = df.select(inspectedFields(0), inspectedFields.drop(1):_*).first()
    val results = inspectedFields.zip(row.toSeq).toMap
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
    results("first_paint") should be (42 * 1200)
    results("first_subsession_count") should be (42)

    results("window_start").asInstanceOf[Timestamp].getTime should be <= (TestUtils.testTimestampMillis)
    results("window_end").asInstanceOf[Timestamp].getTime should be >= (TestUtils.testTimestampMillis)
  }

  "The aggregator" should "handle new style experiments" in {
    import spark.implicits._
    val crashMessage = TestUtils.generateCrashMessages(
      k,
      Some(Map(
        "environment.addons" ->
          """
            |{
            | "activeAddons": {"my-addon": {"isSystem": true}},
            | "theme": {"id": "firefox-compact-dark@mozilla.org"}
            |}""".stripMargin,
        "environment.experiments" ->
          """
            |{
            |  "new-experiment-1": {"branch": "control"},
            |  "new-experiment-2": {"branch": "chaos"}
            |}""".stripMargin
      )))
    val mainMessage = TestUtils.generateMainMessages(
      k,
      Some(Map(
        "environment.addons" ->
          """
            |{
            | "activeAddons": {"my-addon": {"isSystem": true}},
            | "theme": {"id": "firefox-compact-dark@mozilla.org"}
            |}""".stripMargin,
        "environment.experiments" ->
          """
            |{
            |  "new-experiment-1": {"branch": "control"},
            |  "new-experiment-2": {"branch": "chaos"}
            |}""".stripMargin
      )))
    val messages = (crashMessage ++ mainMessage).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)
    df.count() should be (1)
    val inspectedFields = List(
      "experiment_id",
      "experiment_branch"
    )
    val row = df.select(inspectedFields(0), inspectedFields.drop(1):_*).first()
    val results = inspectedFields.zip(row.toSeq).toMap
    results("experiment_id") should be ("new-experiment-1")
    results("experiment_branch") should be ("control")
  }
}