/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.io.{BufferedWriter, File, FileWriter}

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.{CrashPayload, CrashPing}
import com.mozilla.telemetry.sinks.BatchHttpSink
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.rogach.scallop.ScallopOption

import scala.collection.immutable.ListMap
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import sys.process._

abstract class CrashPingStreamingBase extends StreamingJobBase {

  val sparkAppName: String

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
      descr = "Name of measurement in database",
      required = true)
    val httpBatchSize: ScallopOption[Int] = opt[Int](
      "httpBatchSize",
      descr = "Max number of crashes to send to influx in one http request",
      required = false,
      default = Some(1))
    val getCrashSignature: ScallopOption[Boolean] = opt[Boolean](
      "getCrashSignature",
      descr = "Use symbolication api and siggen library to get crash signature",
      required = false,
      default = Some(false))

    conflicts(kafkaBroker, List(from, to, fileLimit, maxParallelRequests))

    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName(sparkAppName)
      .getOrCreate()

    opts.kafkaBroker.get match {
      case Some(_) => sendStreamCrashes(spark, opts)
      case None => sendBatchCrashes(spark, opts)
    }
  }

  def sendStreamCrashes(spark: SparkSession, opts: Opts): Unit = {
    val httpSink = getHttpSink(opts.url(), maxBatchSize = opts.httpBatchSize())

    val usingDatabricks = !shouldStopContextAtEnd(spark)

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
      opts.measurementName(), opts.acceptedChannels(), opts.acceptedAppNames(),
      opts.getCrashSignature(), usingDatabricks)
      .writeStream
      .queryName(QueryName)
      .option("checkpointLocation", opts.checkpointPath())
      .foreach(httpSink)
      .start()
      .awaitTermination()
  }

  def sendBatchCrashes(spark: SparkSession, opts: Opts): Unit = {
    val maxParallelRequests = opts.maxParallelRequests()
    val httpBatchSize = opts.httpBatchSize()

    val usingDatabricks = !shouldStopContextAtEnd(spark)

    datesBetween(opts.from(), opts.to.get).foreach { currentDate =>
      val pings = getPingsForDate(spark, opts.acceptedChannels(), opts.acceptedAppNames(),
        opts.fileLimit.get, currentDate)

      val schema = StructType(List(
        StructField("value", BinaryType, nullable = true)
      ))

      val pingsDataFrame = spark.createDataFrame(pings, schema)
      val url = opts.url()

      getParsedPings(pingsDataFrame, opts.raiseOnError(),
        opts.measurementName(), opts.acceptedChannels(), opts.acceptedAppNames(),
        opts.getCrashSignature(), usingDatabricks)
        .repartition(maxParallelRequests)
        .foreachPartition{ it: Iterator[String] =>
          val httpSink = getHttpSink(url, httpBatchSize)
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
                     appNames: List[String] = defaultAppNames,
                     getSignature: Boolean = false, usingDatabricks: Boolean = false): Dataset[String] = {
    import pings.sparkSession.implicits._

    pings.flatMap( v => {
      try {
        parsePing(Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]]),
          channels, appNames, measurementName, getSignature, usingDatabricks)
      } catch {
        case _: Throwable if !raiseOnError => None
      }
    }).as[String]
  }

  def parsePing(message: Message, channels: List[String], appNames: List[String],
                measurementName: String, getSignature: Boolean, usingDatabricks: Boolean): Option[String] = {
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

        val isWindows = ping.getOsName.contains("Windows_NT")
        val crashSignature = if (getSignature) getCrashSignature(ping.payload, isWindows, usingDatabricks) else ""

        val buildId = ping.getNormalizedBuildId.getOrElse(metadata.appBuildId)

        val tags = ListMap(
          "submissionDate" -> metadata.submissionDate,
          "appVersion" -> metadata.appVersion,
          "appName" -> metadata.appName,
          "displayVersion" -> ping.getDisplayVersion.getOrElse(""),
          "channel" -> metadata.normalizedChannel,
          "country" -> metadata.geoCountry,
          "osName" -> ping.getOsName.getOrElse(""),
          "osVersion" -> ping.getOsVersion.getOrElse(""),
          "architecture" -> ping.getArchitecture.getOrElse(""),
          "buildIdTag" -> ping.getNormalizedBuildId.getOrElse(metadata.appBuildId),
          "crashSignature" -> crashSignature
        ).filter{ case (k, v) => v.nonEmpty }

        Some(buildOutputString(measurementName, timestamp, buildId, tags))
      }
    }
  }

  def buildOutputString(measurementName: String, timestamp: Long,
                        buildId: String, tags: Map[String, String]): String

  def getHttpSink(url: String, maxBatchSize: Int): BatchHttpSink = {
    new BatchHttpSink(url, maxBatchSize = maxBatchSize)
  }

  def formatCrashSignature(signature: String): String = signature

  def getCrashSignature(payload: CrashPayload, isWindows: Boolean, usingDatabricks: Boolean, tries: Int = 0): String = {
    try {
      implicit val formats = DefaultFormats
      val payloadJson = Serialization.write(payload)

      // TODO: Is there a more robust way of doing this?

      // attempt to get crash signature with exponential timeout
      val response = Await.result(
        Future(getSignatureFromExternalCommand(payloadJson, isWindows, usingDatabricks)),
        (Math.pow(2, tries) * 30).seconds
      )

      // TODO: may be worth adding notes from response to pings
      val signature = Serialization.read[CrashSignature](response)

      formatCrashSignature(signature.signature)
    } catch {
      case _: TimeoutException => {
        if (tries > 2) {
          "fx-crash-sig\\ timed\\ out"
        } else {
          getCrashSignature(payload, isWindows, usingDatabricks, tries + 1)
        }
      }
      case _: Throwable => "" // TODO: real error checking maybe
    }
  }

  def getSignatureFromExternalCommand(payload: String, isWindows: Boolean, usingDatabricks: Boolean): String = {
    val commandPath = if (usingDatabricks) "/databricks/python/bin/fx-crash-sig" else "fx-crash-sig"

    val file = File.createTempFile("CrashesToInflux", ".json", new File("/tmp"))

    try {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(payload)
      bw.close()
      s"cat ${file.getPath}" #| s"$commandPath -v ${if (isWindows) "-w" else ""}" !!
    } finally {
      file.delete()
    }
  }
}

case class CrashSignature(notes: Option[List[String]], proto_signature: Option[String], signature: String)
