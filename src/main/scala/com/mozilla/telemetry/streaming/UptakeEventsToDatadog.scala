/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.{EventPing, MainPing}
import com.mozilla.telemetry.monitoring.{DogStatsDMetric}
import com.mozilla.telemetry.sinks.{DogStatsDMetricSink}
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption

import scala.util.Try


object UptakeEventsToDatadog extends StreamingJobBase {
  override val JobName: String = "uptake_events_enrollments_to_datadog"

  val kafkaCacheMaxCapacity = 100

  private val allowedDocTypes = List("main", "event")

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Uptake Events to Datadog")
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
      .option("failOnDataLoss", false)
      .load()

    val driverAddress = spark.conf.get("spark.driver.host", "localhost")
    val writer = new DogStatsDMetricSink(driverAddress, 8125)

    eventsToMetrics(pings.select("value"), opts.raiseOnError())
      .writeStream
      .queryName(QueryName)
      .foreach(writer)
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  private[streaming] def eventsToMetrics(messages: DataFrame, raiseOnError: Boolean): Dataset[DogStatsDMetric] = {
    import messages.sparkSession.implicits._

    val empty = Array.empty[DogStatsDMetric]

    messages.flatMap(v => {
      try {
        val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
        val fields = m.fieldsAsMap
        // TODO: looks like we can get a double here
        val docType = fields.getOrElse("docType", "").asInstanceOf[String]
        if (!allowedDocTypes.contains(docType)) {
          empty
        } else {
          val uptakeEvents = {
            if (docType == "main") {
              val mainPing = MainPing(m)
              mainPing.getNormandyEvents
            } else {
              val eventPing = EventPing(m)
              eventPing.getUptakeEvents
            }
          }

          val normandyCounters = uptakeEvents.filter(_.category == "normandy").map { e =>
            val tags = Map("experiment" -> e.value.getOrElse(""), "branch" -> e.extra.flatMap(_.get("branch")).getOrElse(""))
            DogStatsDMetric.makeCounter(s"telemetry.${e.category}.${e.`object`}.${e.method}", kvTags = Some(tags))
          }

          val uptakeMetrics = uptakeEvents.filter(_.category == "uptake.remotecontent.result").flatMap { e =>
            // Split the "source" key in the extra field, if it exists, to tag uptake events (bug 1539249)
            val source = e.extra.flatMap(_.get("source").map(_.split("/")))
            val source_type = source.flatMap(_.lift(0)).map(s => Seq("source_type" -> s)).getOrElse(Nil)
            val source_subtype = source.flatMap(_.lift(1)).map(s => Seq("source_subtype" -> s)).getOrElse((Nil))
            val source_details = source.flatMap(_.lift(2)).map(s => Seq("source_details" -> s)).getOrElse(Nil)
            val tags = Map(source_type ++ source_subtype ++ source_details:_*)
            val metricName = s"telemetry.uptake.${e.`object`}.${e.method}.${e.value.getOrElse("null")}"

            // Uptake events all create a counter, and if there's a "duration" or "age" key in the event map field,
            // those are sent as timer metrics
            val counter = Seq(DogStatsDMetric.makeCounter(metricName, kvTags = Some(tags)))
            val duration = e.extra.flatMap(_.get("duration").flatMap(d => Try(d.toInt).toOption)).map(d =>
              Seq(DogStatsDMetric.makeTimer(metricName + ".duration", metricValue = d, kvTags = Some(tags)))).getOrElse(Nil)
            val age = e.extra.flatMap(_.get("age").flatMap(d => Try(d.toInt).toOption)).map(d =>
              Seq(DogStatsDMetric.makeTimer(metricName + ".age", metricValue = d, kvTags = Some(tags)))).getOrElse(Nil)
            counter ++ duration ++ age
          }

          normandyCounters ++ uptakeMetrics
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
