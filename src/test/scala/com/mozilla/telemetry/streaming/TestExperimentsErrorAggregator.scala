// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.Timestamp
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils

import com.mozilla.spark.sql.hyperloglog.functions.{hllCreate, hllCardinality}
import com.mozilla.telemetry.streaming.TestUtils.{deleteRecursively, todayDays}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.joda.time.{Duration, DateTime}
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Tag}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.process._

import java.io.File

class TestExperimentsErrorAggregator extends FlatSpec with Matchers with BeforeAndAfterAll {

  object DockerExperimentsErrorAggregatorTag extends Tag("DockerExperimentsErrorAggregatorTag")

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

  override def beforeAll() = {
    ExperimentsErrorAggregator.prepare
  }

  "The aggregator" should "sum metrics over a set of dimensions" in {
    import spark.implicits._

    val messages = (TestUtils.generateCrashMessages(k)
        ++ TestUtils.generateMainMessages(k)).map(_.toByteArray).seq

    val df = ErrorAggregator.aggregate(spark.sqlContext.createDataset(messages).toDF, raiseOnError = true, online = false, 
      ExperimentsErrorAggregator.defaultDimensionsSchema, ExperimentsErrorAggregator.defaultMetricsSchema,
      ExperimentsErrorAggregator.defaultCountHistogramErrorsSchema,
      ExperimentsErrorAggregator.defaultThresholdHistograms)

    // 1 for each experiment (there are 2), and one for a null experiment
    df.count() should be (3)
    val inspectedFields = List(
      "submission_date",
      "channel",
      "version",
      "os_name",
      "country",
      "main_crashes",
      "content_crashes",
      "gpu_crashes",
      "plugin_crashes",
      "gmplugin_crashes",
      "content_shutdown_crashes",
      "count",
      "subsession_count",
      "usage_hours",
      "experiment_id",
      "experiment_branch",
      "window_start",
      "window_end",
      "HllCardinality(client_count) as client_count"
    )

    val query = df.selectExpr(inspectedFields:_*)
    val columns = query.columns
    val results = columns.zip(columns.map(field => query.collect().map(row => row.getAs[Any](field)).toSet) ).toMap

    results("submission_date").map(_.toString) should be (Set("2016-04-07"))
    results("channel") should be (Set(app.channel))
    results("os_name") should be (Set("Linux"))
    results("country") should be (Set("IT"))
    results("main_crashes") should be (Set(k))
    results("content_crashes") should be (Set(k))
    results("gpu_crashes") should be (Set(k))
    results("plugin_crashes") should be (Set(k))
    results("gmplugin_crashes") should be (Set(k))
    results("content_shutdown_crashes") should be (Set(k))
    results("count") should be (Set(k * 2))
    results("subsession_count") should be (Set(k))
    results("usage_hours") should be (Set(k.toFloat))
    results("experiment_id") should be (Set("experiment1", "experiment2", null))
    results("experiment_branch") should be (Set("control", "chaos", null))
    results("window_start").head.asInstanceOf[Timestamp].getTime should be <= (TestUtils.testTimestampMillis)
    results("window_end").head.asInstanceOf[Timestamp].getTime should be >= (TestUtils.testTimestampMillis)
    results("client_count") should be (Set(1))
  }

  "the aggregator" should "correctly read from kafka" taggedAs(Kafka.DockerComposeTag, DockerExperimentsErrorAggregatorTag) in {
    spark.sparkContext.setLogLevel("WARN")

    Kafka.createTopic(ExperimentsErrorAggregator.experimentTopic)
    val kafkaProducer = Kafka.makeProducer(ExperimentsErrorAggregator.experimentTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val earlier = (TestUtils.generateMainMessages(k, timestamp = Some(earlierTimestamp)) ++
      TestUtils.generateCrashMessages(k, timestamp = Some(earlierTimestamp))).map(_.toByteArray)

    val later = TestUtils.generateMainMessages(1, timestamp = Some(laterTimestamp)).map(_.toByteArray)

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
    val checkpointPath = "/tmp/experiment_error_aggregates_checkpoint"
    deleteRecursively(new File(checkpointPath))

    val args = "--kafkaBroker" :: Kafka.kafkaBrokers ::
      "--outputPath" :: outputPath ::
      "--startingOffsets" :: "latest" ::
      "--checkpointPath" :: checkpointPath ::
      "--raiseOnError" :: Nil

    ExperimentsErrorAggregator.main(args.toArray)

    assert(spark.read.parquet(s"$outputPath/${ExperimentsErrorAggregator.outputPrefix}").count() == 3)

    kafkaProducer.close
    spark.streams.removeListener(listener)
  }
}
