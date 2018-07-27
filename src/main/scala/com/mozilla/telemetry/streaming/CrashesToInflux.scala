/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.CrashPing
import com.mozilla.telemetry.sinks.BatchHttpSink
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql._
import org.rogach.scallop.ScallopOption

import scala.collection.immutable.ListMap

object CrashesToInflux extends StreamingJobBase {

  val kafkaCacheMaxCapacity = 1000

  val defaultChannels: List[String] = List[String]("release", "beta", "nightly")
  val defaultAppNames: List[String] = List[String]("Firefox", "Fennec")

  private[streaming] class Opts(args: Array[String]) extends BaseOpts(args) {
    val raiseOnError: ScallopOption[Boolean] = opt[Boolean](
      "raiseOnError",
      descr = "Whether the program should exit on a data processing error or not.")
    val failOnDataLoss: ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val url: ScallopOption[String] = opt[String](
      "url",
      descr = "Endpoint to send data to",
      required = true)
    val maxParallelRequests: ScallopOption[Int] = opt[Int](
      "maxParallelRequests",
      descr = "Max number of parallel requests in batch mode",
      required = false,
      default = Some(100))
    val acceptedChannels: ScallopOption[List[String]] = opt[List[String]](
      "acceptedChannels",
      descr = "Release channels to accept crashes from",
      required = false,
      default = Some(defaultChannels))
    val acceptedAppNames: ScallopOption[List[String]] = opt[List[String]](
      "acceptedAppNames",
      descr = "Applications to accept crashes from",
      required = false,
      default = Some(defaultAppNames))
    val measurementName: ScallopOption[String] = opt[String](
      "measurementName",
      descr = "Name of measurement in InfluxDB",
      required = true)
    val httpBatchSize: ScallopOption[Int] = opt[Int](
      "httpBatchSize",
      descr = "Max number of crashes to send to influx in one http request",
      required = false,
      default = Some(1)
    )

    conflicts(kafkaBroker, List(from, to, fileLimit, maxParallelRequests))

    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("CrashesToInflux")
      .getOrCreate()

    opts.kafkaBroker.get match {
      case Some(_) => sendStreamCrashes(spark, opts)
      case None => sendBatchCrashes(spark, opts)
    }
  }

  def sendStreamCrashes(spark: SparkSession, opts: Opts): Unit = {
    val httpSink = new BatchHttpSink(opts.url(), maxBatchSize = opts.httpBatchSize())

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

    getParsedPings(pings.select("value"), opts.raiseOnError(),
      opts.measurementName(), opts.acceptedChannels(), opts.acceptedAppNames())
      .writeStream
      .queryName(QueryName)
      .foreach(httpSink)
      .start()
      .awaitTermination()
  }

  def sendBatchCrashes(spark: SparkSession, opts: Opts): Unit = {
    val maxParallelRequests = opts.maxParallelRequests()
    val httpBatchSize = opts.httpBatchSize()

    datesBetween(opts.from(), opts.to.get).foreach { currentDate =>
      val pings = getPingsForDate(spark, opts.acceptedChannels(), opts.acceptedAppNames(),
        opts.fileLimit.get, currentDate)

      val schema = StructType(List(
        StructField("value", BinaryType, nullable = true)
      ))

      val pingsDataFrame = spark.createDataFrame(pings, schema)
      val url = opts.url()

      getParsedPings(pingsDataFrame, opts.raiseOnError(),
        opts.measurementName(), opts.acceptedChannels(), opts.acceptedAppNames())
        .repartition(maxParallelRequests)
        .foreachPartition{ it: Iterator[String] =>
          val httpSink = new BatchHttpSink(url, maxBatchSize = httpBatchSize)
          it.foreach(httpSink.process)
          httpSink.flush()
        }
    }
    if (shouldStopContextAtEnd(spark)) {
      spark.stop()
    }
  }

  // separate function to work around spark serializable exception
  def getPingsForDate(spark: SparkSession, acceptedChannels: List[String], acceptedAppNames: List[String],
                      fileLimit: Option[Int], currentDate: String): RDD[Row] = {
    implicit val sc = spark.sparkContext

    com.mozilla.telemetry.heka.Dataset("telemetry")
      .where("sourceName") {
        case "telemetry" => true
      }.where("docType") {
      case doctype if doctype == "crash" => true
    }.where("appUpdateChannel") {
      case appUpdateChannel if acceptedChannels.contains(appUpdateChannel) => true
    }.where("appName") {
      case appName if acceptedAppNames.contains(appName) => true
    }.where("submissionDate") {
      case date if date == currentDate => true
    }.records(fileLimit).map(m => Row(m.toByteArray))
  }

  def getParsedPings(pings: DataFrame, raiseOnError: Boolean, measurementName: String,
                     channels: List[String] = defaultChannels,
                     appNames: List[String] = defaultAppNames): Dataset[String] = {
    import pings.sparkSession.implicits._

    pings.flatMap( v => {
      try {
        parsePing(Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]]), channels, appNames, measurementName)
      } catch {
        case _: Throwable if !raiseOnError => None
      }
    }).as[String]
  }

  def parsePing(message: Message, channels: List[String], appNames: List[String],
                measurementName: String): Option[String] = {
    val fields = message.fieldsAsMap

    if (!fields.get("docType").contains("crash")) {
      None
    } else {
      val ping = CrashPing(message)
      val metadata = ping.meta

      if (!channels.contains(metadata.normalizedChannel) || !appNames.contains(metadata.appName)) {
        None
      } else {
        val timestamp = metadata.Timestamp

        val influxFields = ListMap(
          "buildId" -> ping.getNormalizedBuildId.getOrElse(metadata.appBuildId)
        )

        val influxTags = ListMap(
          "submissionDate" -> metadata.submissionDate,
          "appVersion" -> metadata.appVersion,
          "appName" -> metadata.appName,
          "displayVersion" -> ping.getDisplayVersion.getOrElse(""),
          "channel" -> metadata.normalizedChannel,
          "country" -> metadata.geoCountry,
          "osName" -> metadata.os.getOrElse(""),
          "osVersion" -> ping.getOsVersion.getOrElse(""),
          "architecture" -> ping.getArchitecture.getOrElse("")
        )

        Some(measurementName +
          influxTags.map { case (k, v) => s"$k=$v" }.mkString(",", ",", "") +
          influxFields.map { case (k, v) => s"$k=$v" }.mkString(" ", ",", "") +
          " " +
          timestamp)
      }
    }
  }
}
