package com.mozilla.telemetry.streaming

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.scalatest._

class TestAggregator extends FlatSpec with Matchers{

  implicit val formats = DefaultFormats

  "The aggregator" should "sum metrics over a set of dimensions" in {
    val spark = SparkSession.builder()
      .appName("Error Aggregates")
      .master("local[1]")
      .getOrCreate()
    import spark.implicits._
    val messages = (TestUtils.generateCrashMessages(42) ++ TestUtils.generateMainMessages(42)).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)
    df.count() should be (1)
    df.select("submission_date").first()(0).toString should be ("2016-04-07")
    df.select("channel").first()(0) should be (TestUtils.application.channel)
    df.select("version").first()(0) should be (TestUtils.application.version)
    df.select("build_id").first()(0) should be (TestUtils.application.buildId)
    df.select("application").first()(0) should be (TestUtils.application.name)
    df.select("os_name").first()(0) should be ("Linux")
    df.select("os_version").first()(0) should be ("42")
    df.select("architecture").first()(0) should be (TestUtils.application.architecture)
    df.select("country").first()(0) should be ("IT")
    df.select("main_crashes").first()(0) should be (42)
    df.select("content_crashes").first()(0) should be (42)
    df.select("gpu_crashes").first()(0) should be (42)
    df.select("plugin_crashes").first()(0) should be (42)
    df.select("gmplugin_crashes").first()(0) should be (42)
    df.select("content_shutdown_crashes").first()(0) should be (42)
    df.select("count").first()(0) should be (84)
    df.select("usage_hours").first()(0) should be (42.0)
    df.select("browser_shim_usage_blocked").first()(0) should be (42)
    df.select("experiment_id").first()(0) should be ("experiment1")
    df.select("experiment_branch").first()(0) should be ("control")
    df.select("e10s_enabled").first()(0) should equal (true)
    df.select("e10s_cohort").first()(0) should be ("test")
    df.select("gfx_compositor").first()(0) should be ("opengl")
    df.where("window is null").count() should be (0)
  }
}
