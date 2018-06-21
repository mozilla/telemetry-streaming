/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.MainPing
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import com.mozilla.telemetry.streaming.sinks.HttpSink
import org.apache.spark
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.jackson.Serialization
import org.rogach.scallop.ScallopOption
import scalaj.http.HttpRequest

object ExperimentEnrollmentsToTestTube extends StreamingJobBase {
  val MaxParalellRequests = 10
  val httpSendMethod: (HttpRequest, String) => HttpRequest = (req: HttpRequest, data: String) =>
    req.postData(s"""{"enrollment":[$data]}""").header("content-type", "application/json")


  val kafkaCacheMaxCapacity = 100

  private val allowedDocTypes = List("main")
  private val allowedAppNames = List("Firefox")

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Experiment Enrollments to TestTube")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()


    val httpSink = new HttpSink(opts.url(), Map())(httpSendMethod)

    val pings = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", opts.kafkaBroker())
      .option("failOnDataLoss", opts.failOnDataLoss())
      .option("kafka.max.partition.fetch.bytes", 8 * 1024 * 1024) // 8MB
      .option("spark.streaming.kafka.consumer.cache.maxCapacity", kafkaCacheMaxCapacity)
      .option("subscribe", TelemetryKafkaTopic)
      .option("startingOffsets", opts.startingOffsets())
      .load()
      .select("value")

    aggregateAndSend(pings, httpSink)
      .awaitTermination()
  }

  def aggregateAndSend(pings: DataFrame, httpSink: HttpSink[String]): StreamingQuery = {
    aggregate(pings)
      .coalesce(MaxParalellRequests)
      .writeStream
      .queryName(QueryName)
      .foreach(httpSink)
      .start()
  }

  private[streaming] def aggregate(messages: DataFrame): Dataset[String] = {

    import messages.sparkSession.implicits._
    import spark.sql.functions._

    val events: Dataset[ExperimentEnrollmentEvent] = messages.flatMap(v => {
      try {
        val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
        val fields = m.fieldsAsMap
        val docType = fields.getOrElse("docType", "").asInstanceOf[String]
        if (!allowedDocTypes.contains(docType)) {
          Array.empty[ExperimentEnrollmentEvent]
        } else {
          val mainPing = MainPing(m)
          mainPing.getNormandyEvents.map { e =>
            val timestamp = mainPing.meta.normalizedTimestamp()
            val submissionDate = timestampToDateString(mainPing.meta.normalizedTimestamp())
            ExperimentEnrollmentEvent(e.method, e.value, e.extra.flatMap(m => m.get("branch")), e.`object`, timestamp, submissionDate)
          }
        }
      } catch {
        // TODO: track parse errors
        case _: Throwable => Array.empty[ExperimentEnrollmentEvent]
      }
    })

    events
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window($"timestamp", "5 minute").as("window"),
        $"object", $"experiment_id", $"branch_id", $"submission_date_s3")
      .agg(
        count(when($"method" === "enroll", 1)).alias("enroll_count"),
        count(when($"method" === "unenroll", 1)).alias("unenroll_count"))
      .withColumn("window_start", $"window.start")
      .withColumn("window_end", $"window.end")
      .drop($"window")
      .map { r =>
        val m = r.getValuesMap[Any](r.schema.fieldNames).map {
          case ("object", o) => "type" -> o
          case ("window_start", t: Timestamp) => "window_start" -> t.getTime
          case ("window_end", t: Timestamp) => "window_end" -> t.getTime
          case (k: String, v) => k -> v
        }
        implicit val formats = org.json4s.DefaultFormats
        Serialization.write(m)
      }
  }

  private def shouldStopContextAtEnd(spark: SparkSession): Boolean = {
    !spark.conf.get("spark.home").startsWith("/databricks")
  }

  case class ExperimentEnrollmentEvent(method: String, // enroll/unenroll
                                       experiment_id: Option[String],
                                       branch_id: Option[String],
                                       `object`: String,
                                       timestamp: Timestamp,
                                       submission_date_s3: String)

  private class Opts(args: Array[String]) extends BaseOpts(args) {
    val failOnDataLoss: ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val url: ScallopOption[String] = opt[String](
      descr = "Endpoint to send data to",
      required = true)

    requireOne(kafkaBroker)
    verify()
  }

}
