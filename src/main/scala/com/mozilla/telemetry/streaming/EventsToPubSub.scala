package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.EventPing
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption
import com.mozilla.telemetry.sinks.{GCPMapValue, PubSubTopicSink}

object EventsToPubSub extends StreamingJobBase {
  override val outputPrefix = "events/v1"

  val kafkaCacheMaxCapacity = 10

  private val allowedDocTypes = List("event")


  protected[streaming] case class Experiments(experiment_id: String, branch: String)

  case class EventMessage(ping_timestamp: Timestamp, document_id: String, client_id: String, normalized_channel: String,
                      country: String, locale: Option[String], app_name: String, app_version: String,
                      os: Option[String], os_version: Option[String], session_id: String, subsession_id: String,
                      session_start_time: Timestamp,
                      experiments: Seq[Experiments], event_timestamp: Long, event_category: String,
                      event_method: String, event_object: String, event_string_value: Option[String],
                      event_map_values: Seq[GCPMapValue], event_process: String)

  class EventPubSubTopicSink(projectName: String, topicId: String) extends PubSubTopicSink[EventMessage](projectName, topicId)

  private class Opts(args: Array[String]) extends BaseOpts(args) {
    val failOnDataLoss: ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val pubsubTopicName = opt[String](
      "pubsubTopicName",
      descr = "The pubsub topic name to publish to",
      required = true)
    val gcpProjectName = opt[String](
      "gcpProjectName",
      descr = "The project the pubsub topic is located in",
      required = true)

    conflicts(kafkaBroker, List(from, to))
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Event Ping Events")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    writeStreamingEvents(spark, opts)
  }

  def writeStreamingEvents(spark: SparkSession, opts: EventsToPubSub.Opts): Unit = {
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

    val writer = new EventPubSubTopicSink(opts.gcpProjectName(), opts.pubsubTopicName())

    explodeEvents(pings.select("value"))
      .repartition(1)
      .writeStream
      .queryName(QueryName)
      .foreach(writer)
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  private[streaming] def explodeEvents(messages: DataFrame): Dataset[EventMessage] = {
    import messages.sparkSession.implicits._

    val empty = Array.empty[EventMessage]

    messages.flatMap(v => {
      try {
        val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
        val fields = m.fieldsAsMap
        val docType = fields.getOrElse("docType", "").asInstanceOf[String]
        if (!allowedDocTypes.contains(docType)) {
          empty
        } else {
          val ping = EventPing(m)
          ping.processEventMap.flatMap { case (process, events) =>
            val experiments = ping.getExperiments.map { case (k, v) => Experiments(k.getOrElse(""), v.getOrElse("")) }.toSeq

            events.map { e =>
              // Serializing a Map[String, Any] causes problems: https://issues.apache.org/jira/browse/SPARK-23251
              val extra: Option[Map[String, String]] =
                e.extra.map(
                  _.map { case (k, v) =>
                    k -> v.toString
                  }.toMap
                )

              EventMessage(ping.meta.normalizedTimestamp, ping.meta.documentId.get, ping.meta.clientId.get, ping.meta.normalizedChannel,
                ping.meta.geoCountry, ping.getLocale, ping.meta.appName, ping.meta.appVersion, ping.getOsName,
                ping.getOsVersion, ping.payload.sessionId, ping.payload.subsessionId, ping.sessionStartAsTimestamp,
                experiments, e.timestamp, e.category, e.method, e.`object`, e.value,
                GCPMapValue.mapToGCP(extra.getOrElse(Map.empty[String, String])), process)
            }
          }
        }
      } catch {
        case _: Throwable => empty
      }
    })
  }
}
