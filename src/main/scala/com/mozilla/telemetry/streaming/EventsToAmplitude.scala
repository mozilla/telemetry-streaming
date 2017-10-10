// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings._
import com.mozilla.telemetry.timeseries._
import com.mozilla.telemetry.streaming.sinks.HttpSink
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}
import org.json4s._
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.joda.time.{DateTime, Days, format}

import java.net.URLEncoder

// TODO:
// - incorporate event schema - DEPENDS ON EVENT SCHEMA

/**
 * Stream events to amplitude. More generally, stream
 * events to an HTTP Endpoint.
 *
 * Backfill is done per-day. There are two options given
 * for limiting backfill: Max Parallelism and Min Delay.
 * Max Parallelism is the max number of parallel requests,
 * and Min Delay is the minimum delay per request, in ms.
 *
 * Max requests/second = 10^3 * MaxParallelism / MinDelay
 *
 * Sampling is limited to the hundredths place; anything
 * more will be truncated (e.g. .355 will be .35, so 35%).
 */
object EventsToAmplitude {

  val AMPLITUDE_API_KEY_KEY = "AMPLITUDE_API_KEY"

  val allowedDocTypes = List("focus-event")
  val allowedAppNames = List("Focus")
  val kafkaCacheMaxCapacity = 1000
  val kafkaTopic = "telemetry"
  val queryName = "EventsToAmplitude"
  val writeMode = "error"

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val kafkaBroker: ScallopOption[String] = opt[String](
      "kafkaBroker",
      descr = "Kafka broker (streaming mode only)",
      required = false)
    val from: ScallopOption[String] = opt[String](
      "from",
      descr = "Start submission date (batch mode only). Format: YYYYMMDD",
      required = false)
    val to: ScallopOption[String] = opt[String](
      "to",
      descr = "End submission date (batch mode only). Default: yesterday. Format: YYYYMMDD",
      required = false)
    val fileLimit: ScallopOption[Int] = opt[Int](
      "fileLimit",
      descr = "Max number of files to retrieve (batch mode only). Default: All files",
      required = false)
    val raiseOnError:ScallopOption[Boolean] = opt[Boolean](
      "raiseOnError",
      descr = "Whether the program should exit on a data processing error or not.")
    val failOnDataLoss:ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val checkpointPath:ScallopOption[String] = opt[String](
      "checkpointPath",
      descr = "Checkpoint path (streaming mode only)",
      required = false,
      default = Some("/tmp/checkpoint"))
    val startingOffsets:ScallopOption[String] = opt[String](
      "startingOffsets",
      descr = "Starting offsets (streaming mode only)",
      required = false,
      default = Some("latest"))
    val url: ScallopOption[String] = opt[String](
      "url",
      descr = "Endpoint to send data to",
      required = true)
    val sample: ScallopOption[Double] = opt[Double](
      "sample",
      descr = "Fraction of clients to use",
      required = false,
      default = Some(1.0))
    val minDelay: ScallopOption[Long] = opt[Long](
      "minDelay",
      descr = "Amount of delay between requesets in batch mode, in ms",
      required = false,
      default = Some(0))
    val maxParallelRequests: ScallopOption[Int] = opt[Int](
      "maxParallelRequests",
      descr = "Max number of parallel requests in batch mode",
      required = false,
      default = Some(100))
    requireOne(kafkaBroker, from)
    conflicts(kafkaBroker, List(from, to, fileLimit, minDelay, maxParallelRequests))
    validateOpt (sample) {
      case Some(s) if 0.0 < s && s <= 1.0 => Right(Unit)
      case Some(o) => Left(s"Sample out of range. Expected 0.0 < Sample <= 1.0, Found $o")
    }

    verify()
  }

  def parsePing(message: Message, sample: Double): Array[String] = {
    implicit val formats = DefaultFormats

    val fields = message.fieldsAsMap
    val docType = fields.getOrElse("docType", "").asInstanceOf[String]
    val appName = fields.getOrElse("appName", "").asInstanceOf[String]

    (allowedDocTypes.contains(docType) && allowedAppNames.contains(appName)) match {
      case false => Array[String]()
      case true =>
        val ping = FocusEventPing(message)
        ping.includeClient(sample) match {
          case false => Array[String]()
          case true =>
            (ping.getEvents :: Nil)
              .map{ URLEncoder.encode(_, "UTF-8") }
              .toArray
        }
    }
  }

  def getEvents(pings: DataFrame, sample: Double, raiseOnError: Boolean): Dataset[String] = {
    import pings.sparkSession.implicits._

    pings.flatMap( v => {
        try {
          parsePing(Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]]), sample)
        } catch {
          case _: Throwable if !raiseOnError => Array[String]()
        }
      }).as[String]
  }

  def sendStreamingEvents(spark: SparkSession, opts: Opts): Unit = {
    val apiKey = sys.env(AMPLITUDE_API_KEY_KEY)
    val httpSink = new HttpSink(opts.url(), Map("api_key" -> apiKey))

    val pings = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", opts.kafkaBroker())
      .option("failOnDataLoss", opts.failOnDataLoss())
      .option("kafka.max.partition.fetch.bytes", 8 * 1024 * 1024) // 8MB
      .option("spark.streaming.kafka.consumer.cache.maxCapacity", kafkaCacheMaxCapacity)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", opts.startingOffsets())
      .load()

    getEvents(pings.select("value"), opts.sample(), opts.raiseOnError())
      .writeStream
      .queryName(queryName)
      .foreach(httpSink)
      .start()
      .awaitTermination()
  }

  def sendBatchEvents(spark: SparkSession, opts: Opts): Unit = {
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")

    val from = fmt.parseDateTime(opts.from())
    val to = opts.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }

    implicit val sc = spark.sparkContext
    val underscoreDocTypes = allowedDocTypes.map(_.replace("-", "_"))

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset).toString("yyyyMMdd")
      val pings = com.mozilla.telemetry.heka.Dataset("telemetry")
        .where("sourceName") {
          case "telemetry" => true
        }.where("docType") {
          case docType if underscoreDocTypes.contains(docType) => true
        }.where("appName") {
          case appName if allowedAppNames.contains(appName) | appName == "OTHER" => true
        }.where("submissionDate") {
          case date if date == currentDate => true
        }.records(opts.fileLimit.get)
        .map(m => Row(m.toByteArray))

      val schema = StructType(List(
          StructField("value", BinaryType, true)
      ))

      val pingsDataFrame = spark.createDataFrame(pings, schema)
      val apiKey = sys.env(AMPLITUDE_API_KEY_KEY)
      val minDelay = opts.minDelay()
      val url = opts.url()

      getEvents(pingsDataFrame, opts.sample(), opts.raiseOnError())
        .repartition(opts.maxParallelRequests())
        .foreachPartition{ it: Iterator[String] =>
          val httpSink = new HttpSink(url, Map("api_key" -> apiKey))
          it.foreach{ event =>
            httpSink.process(event)
            java.lang.Thread.sleep(minDelay)
          }
        }
    }

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName(queryName)
      .getOrCreate()

    opts.kafkaBroker.get match {
      case Some(_) => sendStreamingEvents(spark, opts)
      case None => sendBatchEvents(spark, opts)
    }
  }
}
