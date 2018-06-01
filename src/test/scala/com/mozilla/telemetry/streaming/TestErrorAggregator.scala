/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.io.File
import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.streaming.TestUtils.Fennec
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.json4s.DefaultFormats
import org.scalatest.{FlatSpec, Matchers, Tag}

class TestErrorAggregator extends FlatSpec with Matchers with DataFrameSuiteBase {

  object DockerErrorAggregatorTag extends Tag("DockerErrorAggregatorTag")

  implicit val formats = DefaultFormats
  val k = TestUtils.scalarValue
  val app = TestUtils.defaultFirefoxApplication

  // 2016-04-07T02:01:56.000Z
  val earlierTimestamp = 1459994516000000000L

  // 2016-04-07T02:35:16.000Z
  val laterTimestamp = 1459996516000000000L

  val streamingOutputPath = "/tmp/parquet"
  val streamingCheckpointPath = "/tmp/checkpoint"

  override def beforeAll(): Unit = {
    super.beforeAll()
    cleanupTestDirectories()
  }

  override def afterAll(): Unit = {
    super.beforeAll()
    cleanupTestDirectories()
  }

  private def cleanupTestDirectories() = {
    FileUtils.deleteDirectory(new File(streamingOutputPath))
    FileUtils.deleteDirectory(new File(streamingCheckpointPath))
  }

  "The aggregator" should "sum metrics over a set of dimensions" in {
    import spark.implicits._

    val mainCrashes =
      TestUtils.generateCrashMessages(k - 2) ++
        TestUtils.generateCrashMessages(1, customMetadata = Some(""""StartupCrash": "0"""")) ++
        TestUtils.generateCrashMessages(1, customMetadata = Some(""""StartupCrash": "1""""))
    val contentCrashes =
      TestUtils.generateCrashMessages(1, customMetadata = Some(""""ipc_channel_error": "ShutDownKill""""),
        customPayload = Some(""""processType": "content"""")) ++
        TestUtils.generateCrashMessages(1, customPayload = Some(""""processType": "content""""))

    val messages =
      (mainCrashes
        ++ contentCrashes
        ++ TestUtils.generateMainMessages(k)).map(_.toByteArray).seq


    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true)

    // 1 for each experiment (there are 2), and one for a null experiment
    df.count() should be (3)
    val inspectedFields = List(
      "submission_date_s3",
      "channel",
      "version",
      "display_version",
      "build_id",
      "application",
      "os_name",
      "os_version",
      "architecture",
      "country",
      "main_crashes",
      "content_crashes",
      "gpu_crashes",
      "plugin_crashes",
      "gmplugin_crashes",
      "content_shutdown_crashes",
      "startup_crashes",
      "count",
      "usage_hours",
      "browser_shim_usage_blocked",
      "experiment_id",
      "experiment_branch",
      "window_start",
      "window_end"
    )

    val query = df.selectExpr(inspectedFields:_*)
    val columns = query.columns
    val results = columns.zip(columns.map(field => query.collect().map(row => row.getAs[Any](field)).toSet) ).toMap


    results("submission_date_s3").map(_.toString) should be (Set("20160407"))
    results("channel") should be (Set(app.channel))
    results("version") should be (Set(app.version))
    results("display_version") should be (Set(app.displayVersion.getOrElse(null)))
    results("build_id") should be (Set(app.buildId))
    results("application") should be (Set(app.name))
    results("os_name") should be (Set("Linux"))
    results("os_version") should be (Set(s"${k}"))
    results("architecture") should be (Set(app.architecture))
    results("country") should be (Set("IT"))
    results("main_crashes") should be (Set(k))
    results("content_crashes") should be (Set(1))
    results("gpu_crashes") should be (Set(k))
    results("plugin_crashes") should be (Set(k))
    results("gmplugin_crashes") should be (Set(k))
    results("content_shutdown_crashes") should be (Set(1))
    results("startup_crashes") should be(Set(1))
    results("count") should be (Set(k * 2 + 2))
    results("usage_hours") should be (Set(k.toFloat))
    results("browser_shim_usage_blocked") should be (Set(k))
    results("experiment_id") should be (Set("experiment1", "experiment2", null))
    results("experiment_branch") should be (Set("control", "chaos", null))
    results("window_start").head.asInstanceOf[Timestamp].getTime should be <= (TestUtils.testTimestampMillis)
    results("window_end").head.asInstanceOf[Timestamp].getTime should be >= (TestUtils.testTimestampMillis)
  }

  it should "normalize os_version" in {
    import spark.implicits._
    val fieldsOverride = Some(Map("environment.system" -> """{"os": {"name": "linux", "version": "10.2.42-hello"}}"""))
    val messages =
      (TestUtils.generateCrashMessages(k, fieldsOverride=fieldsOverride)
        ++ TestUtils.generateMainMessages(k, fieldsOverride=fieldsOverride)).map(_.toByteArray).seq

    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true)

    // 1 for each experiment (there are 2), and one for a null experiment
    df.count() should be (3)
    val inspectedFields = List(
      "os_version"
    )

    val query = df.selectExpr(inspectedFields:_*)
    val columns = query.columns
    val results = columns.zip(columns.map(field => query.collect().map(row => row.getAs[Any](field)).toSet) ).toMap

    results("os_version") should be (Set(s"10.2.42"))
  }

  it should "handle new style experiments" in {
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

    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF)


    // one count for each experiment-branch, and one for null-null
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

  it should "handle Fennec pings" in {
    import spark.implicits._

    // Ignore Fennec experiments for now as these are different than desktop ones
    val fieldsOverride = Some(Map("environment.addons" -> "{}", "environment.experiments" -> "{}"))

    val messages =
      (TestUtils.generateCrashMessages(k, appType = Fennec, fieldsOverride = fieldsOverride)
        ++ TestUtils.generateFennecCoreMessages(k)
        ).map(_.toByteArray).seq

    val aggregates = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF)

    val expectedNumberOfAggregatedRows = 1

    aggregates.count() shouldBe expectedNumberOfAggregatedRows
    aggregates.filter($"application" === "Fennec").count() shouldBe expectedNumberOfAggregatedRows

    val inspectedFields = List(
      "build_id",
      "usage_hours",
      "os_name"
    )
    val rows = aggregates.select(inspectedFields(0), inspectedFields.drop(1): _*).collect()
    val results = inspectedFields.zip(inspectedFields.map(field => rows.map(row => row.getAs[Any](field)).toSet)).toMap
    results("usage_hours") should be(Set(k.toFloat))
    results("os_name") should be(Set("Android"))
    results("build_id") should be(Set(TestUtils.defaultFennecApplication.buildId))
  }

  it should "correctly extract displayVersion from Fennec pings" in {
    import spark.implicits._

    val messages =
      (TestUtils.generateFennecCoreMessages(k)
        ++ TestUtils.generateFennecCoreMessages(k, app = TestUtils.defaultFennecApplication.copy(displayVersion = None))
        ).map(_.toByteArray).seq

    val aggregates = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF)

    val expectedNumberOfAggregatedRows = 2
    aggregates.count() shouldBe expectedNumberOfAggregatedRows

    val inspectedFields = List(
      "display_version"
    )
    val rows = aggregates.select(inspectedFields(0), inspectedFields.drop(1): _*).collect()
    val results = inspectedFields.zip(inspectedFields.map(field => rows.map(row => row.getAs[Any](field)).toSet)).toMap
    results("display_version") should be(Set(TestUtils.defaultFennecApplication.displayVersion.get, TestUtils.defaultFennecApplication.version))
  }

  it should "discard non-Firefox pings" in {
    import spark.implicits._
    val fxCrashMessages = TestUtils.generateCrashMessages(k)
    val fxMainMessages = TestUtils.generateMainMessages(k)
    val otherCrashMessages = TestUtils.generateCrashMessages(k, Some(Map("appName" -> "Icefox")))
    val otherMainMessages = TestUtils.generateMainMessages(k, Some(Map("appName" -> "Icefox")))
    val fennecCoreMessages = TestUtils.generateFennecCoreMessages(k)

    val messages =
      (fxCrashMessages ++ fxMainMessages ++ otherCrashMessages ++ otherMainMessages ++ fennecCoreMessages).map(_.toByteArray).seq

    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = false)

    df.where("application not in ('Firefox','Fennec')").count() should be(0)
  }

  it should "correctly read from kafka" taggedAs(Kafka.DockerComposeTag, DockerErrorAggregatorTag) in {
    spark.sparkContext.setLogLevel("WARN")

    Kafka.createTopic(ErrorAggregator.kafkaTopic)
    val kafkaProducer = Kafka.makeProducer(ErrorAggregator.kafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val earlier = (
      TestUtils.generateMainMessages(k, timestamp = Some(earlierTimestamp)) ++
        TestUtils.generateCrashMessages(k, timestamp = Some(earlierTimestamp)) ++
        TestUtils.generateFennecCoreMessages(k, timestamp = Some(earlierTimestamp))
      ).map(_.toByteArray)

    val later = TestUtils.generateMainMessages(1, timestamp = Some(laterTimestamp)).map(_.toByteArray)

    val expectedTotalMsgs = 3 * k

    val listener = new StreamingQueryListener {
      val DefaultWatermark = "1970-01-01T00:00:00.000Z"

      var messagesSeen = 0L
      var sentMessages = false
      var watermarks: Set[String] = Set(DefaultWatermark)

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        messagesSeen += event.progress.numInputRows

        if(!sentMessages){
          send(earlier)
          sentMessages = true
        }

        // If we only send this message once (i.e. set a flag that we've sent it), Spark will recieve
        // it and process the new rows (should be 3: 1 per experiment), and will update the eventTime["max"]
        // to be this message's time -- but it will not update the watermark, and thus will not write
        // the old rows (from earlier) to disk. You can follow this by reading the QueryProgress log events.
        // If we send more than one, however, it eventually updates the value.
        if(messagesSeen >= expectedTotalMsgs){
          send(later)
        }

        val watermark = event.progress.eventTime.getOrDefault("watermark", DefaultWatermark)
        watermarks = watermarks | Set(watermark)

        // We're done when we've gone through 3 watermarks -- the default, the earlier, and the later
        // when we're on the later watermark, the data from the earlier window is written to disk
        if(watermarks.size == 3){
          spark.streams.active.foreach(_.processAllAvailable)
          spark.streams.active.foreach(_.stop)
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    }

    spark.streams.addListener(listener)

    val args = "--kafkaBroker" :: Kafka.kafkaBrokers ::
      "--outputPath" :: streamingOutputPath ::
      "--checkpointPath" :: streamingCheckpointPath ::
      "--startingOffsets" :: "latest" ::
      "--raiseOnError" :: Nil

    ErrorAggregator.main(args.toArray)

    assert(spark.read.parquet(s"$streamingOutputPath/${ErrorAggregator.outputPrefix}").count() == 3 + 1) // 3 from desktop + 1 from Fennec

    kafkaProducer.close
    spark.streams.removeListener(listener)
  }

  "The resulting schema" should "not have fields belonging to the tempSchema" in {
    import spark.implicits._
    val messages = TestUtils.generateCrashMessages(10).map(_.toByteArray).seq

    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = false)

    df.schema.fields.map(_.name) should not contain ("client_id")
  }

  "BuildId older than 6 months" should "not be aggregated" in {
    import spark.implicits._
    val messages = TestUtils.generateMainMessages(
      1, Some(Map(
        "environment.build" -> """{"buildId": "20170102"}""",
        "submissionDate" -> "20170810"
        )
      )
    ).map(_.toByteArray).seq

    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = false)

    df.count() should be (0)

    val messages2 = TestUtils.generateMainMessages(
      1, Some(Map(
        "environment.build" -> """{"buildId": "20170101"}""",
        "submissionDate" -> "20170601"
        )
      )
    ).map(_.toByteArray).seq

    val df2 = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages2).toDF, raiseOnError = false)

    df2.where("build_id IS NULL").collect().length should be (0)
    df2.count() should be (3)
  }

  "display version" can "be undefined" in {
    import spark.implicits._
    val messages = TestUtils.generateMainMessages(
      1, None, None, "displayVersion" :: Nil
    ).map(_.toByteArray).seq

    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = false)

    // 1 for each experiment (there are 2), and one for a null experiment
    df.where("display_version IS NULL").collect().length should be (3)
    // should be no cases where displayVersion is null for this test case
    df.where("display_version <> NULL").collect().length should be (0)
  }
}
