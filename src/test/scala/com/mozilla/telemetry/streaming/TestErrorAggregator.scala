// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.Timestamp
import com.mozilla.spark.sql.hyperloglog.functions.{hllCreate, hllCardinality}
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TestErrorAggregator extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val formats = DefaultFormats
  val k = TestUtils.scalarValue
  val app = TestUtils.application

  val spark = SparkSession.builder()
    .appName("Error Aggregates")
    .master("local[1]")
    .getOrCreate()

  spark.udf.register("HllCreate", hllCreate _)
  spark.udf.register("HllCardinality", hllCardinality _)

  override def afterAll() {
    spark.stop()
  }

  "The aggregator" should "sum metrics over a set of dimensions" in {
    import spark.implicits._
    val messages =
      (TestUtils.generateCrashMessages(k)
        ++ TestUtils.generateMainMessages(k)).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)

    // 1 for each experiment (there are 2), and one for a null experiment
    df.count() should be (3)
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
      "window_end",
      "HllCardinality(client_count) as client_count",
      "HllCardinality(long_main_input_latency_client_count) as long_main_input_latency_client_count"
    )

    val query = df.selectExpr(inspectedFields:_*)
    val columns = query.columns
    val results = columns.zip(columns.map(field => query.collect().map(row => row.getAs[Any](field)).toSet) ).toMap


    results("submission_date").map(_.toString) should be (Set("2016-04-07"))
    results("channel") should be (Set(app.channel))
    results("version") should be (Set(app.version))
    results("build_id") should be (Set(app.buildId))
    results("application") should be (Set(app.name))
    results("os_name") should be (Set("Linux"))
    results("os_version") should be (Set(s"${k}"))
    results("architecture") should be (Set(app.architecture))
    results("country") should be (Set("IT"))
    results("quantum_ready") should equal (Set(true))
    results("main_crashes") should be (Set(k))
    results("content_crashes") should be (Set(k))
    results("gpu_crashes") should be (Set(k))
    results("plugin_crashes") should be (Set(k))
    results("gmplugin_crashes") should be (Set(k))
    results("content_shutdown_crashes") should be (Set(k))
    results("count") should be (Set(k * 2))
    results("usage_hours") should be (Set(k.toFloat))
    results("browser_shim_usage_blocked") should be (Set(k))
    results("experiment_id") should be (Set("experiment1", "experiment2", null))
    results("experiment_branch") should be (Set("control", "chaos", null))
    results("e10s_enabled") should equal (Set(true))
    results("e10s_cohort") should be (Set("test"))
    results("gfx_compositor") should be (Set("opengl"))
    results("input_event_response_coalesced_ms_main_above_150") should be (Set(42 * 14))
    results("input_event_response_coalesced_ms_main_above_250") should be (Set(42 * 12))
    results("input_event_response_coalesced_ms_main_above_2500") should be (Set(42 * 9))
    results("input_event_response_coalesced_ms_content_above_150") should be (Set(42 * 4))
    results("input_event_response_coalesced_ms_content_above_250") should be (Set(42 * 3))
    results("input_event_response_coalesced_ms_content_above_2500") should be (Set(42 * 2))
    results("first_paint") should be (Set(42 * 1200))
    results("first_subsession_count") should be (Set(42))
    results("window_start").head.asInstanceOf[Timestamp].getTime should be <= (TestUtils.testTimestampMillis)
    results("window_end").head.asInstanceOf[Timestamp].getTime should be >= (TestUtils.testTimestampMillis)
    results("client_count") should be (Set(1))
    results("long_main_input_latency_client_count") should be (Set(1))
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

    //one count for each experiment-branch, and one for null-null
    df.count() should be (3)

    val inspectedFields = List(
      "experiment_id",
      "experiment_branch"
    )
    val rows = df.select(inspectedFields(0), inspectedFields.drop(1):_*).collect()
    val results = inspectedFields.zip( inspectedFields.map(field => rows.map(row => row.getAs[Any](field))) ).toMap

    results("experiment_id").toSet should be (Set("new-experiment-1", "new-experiment-2", null))
    results("experiment_branch").toSet should be (Set("control", "chaos", null))
  }

  "The aggregator" should "correctly compute client counts" in {
    import spark.implicits._
    val crashMessages = 1 to 10 flatMap (i =>
      TestUtils.generateCrashMessages(
        2, Some(Map("clientId" -> s"client${i}")))
      )

    val mainMessages = 1 to 10 flatMap (i =>
      TestUtils.generateMainMessages(
        2, Some(Map("clientId" -> s"client${i}")))
      )

    val messages = (crashMessages ++ mainMessages).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)
    val client_count = df.selectExpr("HllCardinality(client_count) as client_count").collect()(0).getAs[Any]("client_count")
    client_count should be (10)
  }

  "The aggregator" should "correctly compute filtered client counts" in {
    import spark.implicits._

    val mainMessagesAffected = 1 to 5 flatMap (i =>
      TestUtils.generateMainMessages(
        2, Some(
          Map(
            "clientId" -> s"client${i}",
            "payload.histograms" ->
              """{
                |  "INPUT_EVENT_RESPONSE_COALESCED_MS": {
                |    "values": {
                |      "1": 1,
                |      "150": 2,
                |      "250": 3,
                |      "2500": 4,
                |      "10000": 5
                |    }
                |  }
                |}""".stripMargin
          )))
      )

    val mainMessagesNotAffected = 6 to 10 flatMap (i =>
      TestUtils.generateMainMessages(
        2, Some(
          Map(
            "clientId" -> s"client${i}",
            "payload.histograms" ->
              """{
                |  "INPUT_EVENT_RESPONSE_COALESCED_MS": {
                |    "values": {
                |      "1": 1,
                |      "150": 2,
                |      "250": 3,
                |      "2500": 0,
                |      "10000": 0
                |    }
                |  }
                |}""".stripMargin
          )))
      )

    val messages = (mainMessagesAffected ++ mainMessagesNotAffected).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)
    val client_count = df.selectExpr(
      "HllCardinality(long_main_input_latency_client_count) as long_main_input_latency_client_count"
    ).collect()(0).getAs[Any]("long_main_input_latency_client_count")
    client_count should be (5)
  }
}
