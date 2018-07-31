/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.{EventPing, MainPing}
import com.mozilla.telemetry.monitoring.DogStatsDCounter
import com.mozilla.telemetry.sinks.DogStatsDCounterSink
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption


object ExperimentsEnrollmentsToDatadog extends StreamingJobBase {
  val kafkaCacheMaxCapacity = 100

  private val allowedDocTypes = List("main", "event")

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Experiment Enrollments to Datadog")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

      writeStreamingCounter(spark, opts)
  }


  def writeStreamingCounter(spark: SparkSession, opts: Opts): Unit = {
    val pings = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", opts.kafkaBroker())
      .option("kafka.max.partition.fetch.bytes", 8 * 1024 * 1024) // 8MB
      .option("spark.streaming.kafka.consumer.cache.maxCapacity", kafkaCacheMaxCapacity)
      .option("subscribe", TelemetryKafkaTopic)
      .option("startingOffsets", opts.startingOffsets())
      .load()

    val writer = new DogStatsDCounterSink("localhost", 8125)

    eventsToCounter(pings.select("value"), opts.raiseOnError())
      .writeStream
      .queryName(QueryName)
      .foreach(writer)
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  private[streaming] def eventsToCounter(messages: DataFrame, raiseOnError: Boolean): Dataset[DogStatsDCounter] = {
    import messages.sparkSession.implicits._

    val empty = Array.empty[DogStatsDCounter]

    messages.flatMap(v => {
      try {
        val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
        val fields = m.fieldsAsMap
        // TODO: looks like we can get a double here
        val docType = fields.getOrElse("docType", "").asInstanceOf[String]
        if (!allowedDocTypes.contains(docType)) {
          empty
        } else {
          val normandyEvents = {
            if (docType == "main") {
              val mainPing = MainPing(m)
              mainPing.getNormandyEvents
            } else {
              val eventPing = EventPing(m)
              eventPing.getNormandyEvents
            }
          }

          normandyEvents.map { e =>
            val tags = Map("experiment" -> e.value.getOrElse(""), "branch" -> e.extra.flatMap(_.get("branch")).getOrElse(""))
            DogStatsDCounter(s"telemetry.${e.category}.${e.`object`}.${e.method}", kvTags = Some(tags))
          }
        }
      } catch {
        // TODO: track parse errors
        case _: Throwable if !raiseOnError => empty
      }
    })
  }

  private class Opts(args: Array[String]) extends BaseOpts(args) {
    val raiseOnError: ScallopOption[Boolean] = opt[Boolean](
      name = "raiseOnError",
      descr = "Whether the program should exit on a data processing error or not.",
      default = Some(false))

    verify()
  }
}
