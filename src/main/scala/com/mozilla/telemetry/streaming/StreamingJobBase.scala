/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant, LocalDate, ZoneId}

import com.mozilla.telemetry.streaming.StreamingJobBase._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
  * Base class for streaming jobs that read data from Kafka
  */
abstract class StreamingJobBase extends Serializable {

  /**
    * Generic query name for easy monitoring of production jobs
    */
  val QueryName: String = "main_query"

  /**
    * Abstract job name must be defined in concrete children
    */
  val JobName: String

  /**
    * S3 output prefix with version number
    */
  val outputPrefix: String = ""

  val clock: Clock = Clock.systemUTC()

  /**
    * Generates list of dates for querying `com.mozilla.telemetry.heka.Dataset`
    * If `to` is empty, uses yesterday as the upper bound.
    *
    * @param from start date, in "yyyyMMdd" format
    * @param to   (optional) end date, in "yyyyMMdd" format
    * @return sequence of dates formatted as "yyyyMMdd" strings
    */
  def datesBetween(from: String, to: Option[String]): Seq[String] = {
    val parsedFrom: LocalDate = LocalDate.parse(from, DateFormatter)
    val parsedTo: LocalDate = to match {
      case Some(t) => LocalDate.parse(t, DateFormatter)
      case _ => LocalDate.now(clock).minusDays(1)
    }
    (0L to ChronoUnit.DAYS.between(parsedFrom, parsedTo)).map { offset =>
      parsedFrom.plusDays(offset).format(DateFormatter)
    }
  }

  /**
    * Converts timestamp to 'yyyMMdd'-formatted date string
    */
  def timestampToDateString(ts: Timestamp): String = {
    Instant.ofEpochMilli(ts.getTime).atZone(ZoneId.of("UTC")).toLocalDate.format(DateFormatter)
  }

  private[streaming] class BaseOpts(args: Array[String]) extends ScallopConf(args) {
    val kafkaBroker: ScallopOption[String] = opt[String](
      "kafkaBroker",
      descr = "Kafka broker (streaming mode only)",
      required = false)
    val startingOffsets: ScallopOption[String] = opt[String](
      "startingOffsets",
      descr = "Starting offsets (streaming mode only)",
      required = false,
      default = Some("latest"))
    val checkpointPath: ScallopOption[String] = opt[String](
      "checkpointPath",
      descr = "Checkpoint path (streaming mode only)",
      required = false,
      default = Some(s"/tmp/checkpoints/$JobName"))
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

    requireOne(kafkaBroker, from)
  }

  protected def shouldStopContextAtEnd(spark: SparkSession): Boolean = {
    !spark.conf.get("spark.home", "").startsWith("/databricks")
  }
}

object StreamingJobBase {
  /**
    * Date format for parsing input arguments and formatting partitioning columns
    */
  val DateFormat = "yyyyMMdd"
  val DateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(DateFormat)

  val TelemetryKafkaTopic = "telemetry"
}
