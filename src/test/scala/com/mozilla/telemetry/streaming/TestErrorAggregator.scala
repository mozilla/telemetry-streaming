// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.Timestamp
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils

import com.mozilla.spark.sql.hyperloglog.functions.{hllCreate, hllCardinality}
import com.mozilla.telemetry.streaming.TestUtils.todayDays
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.joda.time.{Duration, DateTime}
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, AsyncFlatSpec, Matchers, Tag}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.process._

class TestErrorAggregator extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  object DockerComposeTag extends Tag("DockerComposeTag")

  val zkHostInfo = "localhost:2181"
  val kafkaTopicPartitions = 1
  val kafkaBrokers = "localhost:9092"

  implicit val formats = DefaultFormats
  val k = TestUtils.scalarValue
  val app = TestUtils.application

  // 2016-04-07T02:01:56.000Z
  val earlierTimestamp = 1459994516000000000L

  // 2016-04-07T02:35:16.000Z
  val laterTimestamp = 1459996516000000000L

  val spark = SparkSession.builder()
    .appName("Error Aggregates")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .master("local[1]")
    .getOrCreate()

  spark.udf.register("HllCreate", hllCreate _)
  spark.udf.register("HllCardinality", hllCardinality _)

  override def afterAll() {
    spark.stop()
  }

  def topicExists(zkUtils: ZkUtils, topic: String): Boolean = {
    // taken from
    // https://github.com/apache/spark/blob/master/external/kafka-0-10-sql +
    // src/test/scala/org/apache/spark/sql/kafka010/KafkaTestUtils.scala#L350
    return zkUtils.getAllTopics().contains(topic)
  }

  def createTopic(topic: String, numPartitions: Int) = {
    val timeoutMs = 10000
    val isSecureKafkaCluster = false
    val replicationFactor = 1
    val topicConfig = new Properties
    val zkUtils = ZkUtils.apply(zkHostInfo, timeoutMs, timeoutMs, isSecureKafkaCluster)

    if(!topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, topicConfig)
    }

    zkUtils.close()
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
      "subsession_count",
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
      "profile_age_days"
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
    results("subsession_count") should be (Set(k))
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
    results("profile_age_days") should be (Set(70))
  }

  it should "normalize os_version" in {
    import spark.implicits._
    val fieldsOverride = Some(Map("environment.system" -> """{"os": {"name": "linux", "version": "10.2.42-hello"}}"""))
    val messages =
      (TestUtils.generateCrashMessages(k, fieldsOverride=fieldsOverride)
        ++ TestUtils.generateMainMessages(k, fieldsOverride=fieldsOverride)).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)

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

  "The aggregator" should "correctly compute subsession counts" in {
    import spark.implicits._
    val mainMessages = 1 to 10 flatMap (i =>
      TestUtils.generateMainMessages(
        1, Some(Map("payload.info" -> s"""{"subsessionLength": 3600, "sessionId": "session${i%5}"}""")))
      )

    val messages = mainMessages.map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)
    val subsession_count = df.selectExpr("subsession_count").collect()(0).getAs[Any]("subsession_count")
    subsession_count should be (10)
  }

  "The aggregator" should "use proper bins for profile age" in {
    import spark.implicits._
    val crashMessagesNewProfile = TestUtils.generateCrashMessages(
      k,
      Some(Map(
        "environment.profile" ->
        s"""
           |{
           | "creationDate": ${todayDays-41}
           | }""".stripMargin
      )))

    val crashMessagesYoungProfile = TestUtils.generateCrashMessages(
      k,
      Some(Map(
        "environment.profile" ->
          s"""
             |{
             | "creationDate": ${todayDays-50}
             | }""".stripMargin
      )))

    val crashMessagesOldProfile = TestUtils.generateCrashMessages(
      k,
      Some(Map(
        "environment.profile" ->
        s"""
           |{
           | "creationDate": ${todayDays-3000}
           | }""".stripMargin
      )))


    val messages =
      (crashMessagesNewProfile ++ crashMessagesYoungProfile ++ crashMessagesOldProfile).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false)

    // one count for each age, limited to 60
    // multiplied by the number of experiments (chaos, control, null)
    df.count() should be (9)

    val rows = df.select("profile_age_days").collect()
    val results = rows.map(row => row.getAs[Any]("profile_age_days")).toSet

    results should be (Set(41, 56, 365))
  }

  "The aggregator" should "discard non-Firefox pings" in {
    import spark.implicits._
    val fxCrashMessage = TestUtils.generateCrashMessages(k)
    val fxMainMessage = TestUtils.generateMainMessages(k)
    val otherCrashMessage = TestUtils.generateCrashMessages(k, Some(Map("appName" -> "Icefox")))
    val otherMainMessage = TestUtils.generateMainMessages(k, Some(Map("appName" -> "Icefox")))

    val messages =
      (fxCrashMessage ++ fxMainMessage ++ otherCrashMessage ++ otherMainMessage).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = false, online = false)

    df.where("application <> 'Firefox'").count() should be (0)
  }

  "the aggregator" should "correctly read from kafka" taggedAs(DockerComposeTag) in {
    spark.sparkContext.setLogLevel("WARN")

    val conf = new Properties()
    conf.put("bootstrap.servers", kafkaBrokers)
    conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    conf.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val kafkaProducer = new KafkaProducer[String, Array[Byte]](conf)
    createTopic(ErrorAggregator.kafkaTopic, kafkaTopicPartitions)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ v =>
        val record = new ProducerRecord[String, Array[Byte]](ErrorAggregator.kafkaTopic, v)
        kafkaProducer.send(record)
        kafkaProducer.flush()
      }
    }

    val earlier = (TestUtils.generateMainMessages(k, timestamp=Some(earlierTimestamp)) ++
      TestUtils.generateCrashMessages(k, timestamp=Some(earlierTimestamp))).map(_.toByteArray)

    val later = TestUtils.generateMainMessages(1, timestamp=Some(laterTimestamp)).map(_.toByteArray)

    val expectedTotalMsgs = 2 * k

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

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        if(messagesSeen < expectedTotalMsgs){
          println(s"Terminated Early: Expected $expectedTotalMsgs messages, saw $messagesSeen")
        }
      }
    }

    spark.streams.addListener(listener)

    val outputPath = "/tmp/parquet"
    val args = "--kafkaBroker" :: kafkaBrokers ::
      "--outputPath" :: outputPath ::
      "--startingOffsets" :: "latest" ::
      "--raiseOnError" :: Nil

    val mainRes: Future[Unit] = Future {
      ErrorAggregator.main(args.toArray)
    }

    mainRes map {_ => assert(spark.read.parquet(s"$outputPath/${ErrorAggregator.outputPrefix}").count() == 3)}
  }

  "The resulting schema" should "not have fields belonging to the tempSchema" in {
    import spark.implicits._
    val messages = TestUtils.generateCrashMessages(10).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = false, online = false)
    df.schema.fields.map(_.name) should not contain ("client_id")
  }

  "BuildId" should "not be older than 6 months" in {
    import spark.implicits._
    val messages = TestUtils.generateMainMessages(
      1, Some(Map(
        "environment.build" -> """{"buildId": "20170102"""",
        "submissionDate" -> "2017-06-01"
        )
      )
    ).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = false, online = false)
    df.where("build_id IS NOT NULL").collect().length should be (0)


    val messages2 = TestUtils.generateMainMessages(
      1, Some(Map(
        "environment.build" -> """{"buildId": "20170101"""",
        "submissionDate" -> "2017-06-01"
        )
      )
    ).map(_.toByteArray).seq
    val df2 = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = false, online = false)
    df2.where("build_id IS NULL").collect().length should be (0)
  }
}
