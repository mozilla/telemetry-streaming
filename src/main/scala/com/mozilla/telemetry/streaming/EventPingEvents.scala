/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.{Message, Dataset => MozDataset}
import com.mozilla.telemetry.pings.EventPing
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption

object EventPingEvents extends StreamingJobBase {
  override val outputPrefix = "events/v1"

  val kafkaCacheMaxCapacity = 10

  private val allowedDocTypes = List("event")
  private val allowedAppNames = List("Firefox")

  private class Opts(args: Array[String]) extends BaseOpts(args) {
    val outputPath: ScallopOption[String] = opt[String](
      "outputPath",
      descr = "Output path",
      required = true)
    val failOnDataLoss: ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val maxRecordsPerFile = opt[Int]("max-records-per-file",
      descr = "Max number of rows to write to output files before splitting",
      required = false, default = Some(10000000))

    conflicts(kafkaBroker, List(from, to, maxRecordsPerFile))
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Event Ping Events")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    opts.kafkaBroker.get match {
      case Some(_) => writeStreamingEvents(spark, opts)
      case None => writeBatchEvents(spark, opts)
    }
  }

  def writeStreamingEvents(spark: SparkSession, opts: EventPingEvents.Opts): Unit = {
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

    val outputPath = opts.outputPath()

    explodeEvents(pings.select("value"))
      .repartition(1)
      .writeStream
      .queryName(QueryName)
      .format("parquet")
      .option("path", s"${outputPath}/${outputPrefix}")
      .option("checkpointLocation", opts.checkpointPath())
      .partitionBy("submission_date_s3", "docType")
      .start()
      .awaitTermination()

  }

  def writeBatchEvents(spark: SparkSession, opts: EventPingEvents.Opts): Unit = {
    implicit val sc = spark.sparkContext
    import spark.implicits._

    datesBetween(opts.from(), opts.to.get).foreach { currentDate =>
      val pings = MozDataset("telemetry")
        .where("sourceName") { case "telemetry" => true }
        .where("docType") { case docType if allowedDocTypes.contains(docType) => true }
        .where("appName") { case appName if allowedAppNames.contains(appName) => true }
        .where("submissionDate") { case date if date == currentDate => true }
        .records(opts.fileLimit.get)
        .map(_.toByteArray)

      val pingsDataframe = pings.toDF("value")

      val outputPath = s"${opts.outputPath()}/${outputPrefix}/submission_date_s3=$currentDate/doc_type=event"

      explodeEvents(pingsDataframe)
        .write
        .option("maxRecordsPerFile", opts.maxRecordsPerFile())
        .mode("overwrite")
        .parquet(outputPath)
    }

    spark.stop()
  }

  private[streaming] def explodeEvents(messages: DataFrame): Dataset[EventRow] = {
    import messages.sparkSession.implicits._

    messages.flatMap(v => {
      try {
        val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
        val fields = m.fieldsAsMap
        val docType = fields.getOrElse("docType", "").asInstanceOf[String]
        if (!allowedDocTypes.contains(docType)) {
          None
        } else {
          val ping = EventPing(m)
          ping.processEventMap.flatMap { case (process, events) =>
            events.map { e =>
              EventRow(ping.meta.documentId.get, ping.meta.clientId.get, ping.meta.normalizedChannel, ping.meta.geoCountry,
                ping.getLocale, ping.meta.appName, ping.meta.appVersion, ping.getOsName, ping.getOsVersion,
                ping.payload.sessionId, ping.payload.subsessionId, ping.sessionStart,
                (ping.meta.Timestamp / 1e9).toLong, ping.meta.sampleId.map(_.toString), ping.getMSStyleExperiments,
                e.timestamp, e.category, e.method, e.`object`, e.value, e.extra, process)

            }
          }
        }
      } catch {
        case _: Throwable => None
      }
    })
  }

  case class EventRow(document_id: String, client_id: String, normalized_channel: String,
                      country: String, locale: Option[String], app_name: String, app_version: String,
                      os: Option[String], os_version: Option[String], session_id: String, subsession_id: String,
                      session_start_time: Long, timestamp: Long, sample_id: Option[String],
                      experiments: Option[Map[String, String]], event_timestamp: Long, event_category: String,
                      event_method: String, event_object: String, event_string_value: Option[String],
                      event_map_values: Option[Map[String, String]], event_process: String)

}
