/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, ZoneId}

import com.mozilla.telemetry.heka.{Message, Dataset => MozDataset}
import com.mozilla.telemetry.pings.MainPing
import com.mozilla.telemetry.streaming.ErrorAggregator._
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

object ExperimentEnrollmentsAggregator {
  val queryName = "experiment_enrollments"
  val outputPrefix = "experiment_enrollments/v1"
  val kafkaCacheMaxCapacity = 100
  val dateFormat = "yyyyMMdd"
  val dateFormatter = DateTimeFormatter.ofPattern(dateFormat)
  private val allowedDocTypes = List("main")
  private val allowedAppNames = List("Firefox")

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Experiment Enrollments Aggregates")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    require(spark.version >= "2.3", "Spark 2.3 is required due to dynamic partition overwrite mode")

    opts.kafkaBroker.get match {
      case Some(_) => writeStreamingAggregates(spark, opts)
      case None => writeBatchAggregates(spark, opts)
    }
  }

  def writeStreamingAggregates(spark: SparkSession, opts: Opts): Unit = {
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

    val outputPath = opts.outputPath()

    aggregate(pings.select("value"))
      .repartition(1)
      .writeStream
      .queryName(queryName)
      .format("parquet")
      .option("path", s"${outputPath}/${outputPrefix}")
      .option("checkpointLocation", opts.checkpointPath())
      .partitionBy("submission_date_s3")
      .start()
      .awaitTermination()
  }

  def writeBatchAggregates(spark: SparkSession, opts: Opts): Unit = {
    val from = LocalDate.parse(opts.from(), dateFormatter)
    val to = opts.to.get match {
      case Some(t) => LocalDate.parse(t, dateFormatter)
      case _ => LocalDate.now.minusDays(1)
    }

    implicit val sc = spark.sparkContext
    import spark.implicits._
    for (offset <- 0L to ChronoUnit.DAYS.between(from, to)) {
      val currentDate = from.plusDays(offset)

      val pings = MozDataset("telemetry")
        .where("sourceName") { case "telemetry" => true }
        .where("docType") { case docType if allowedDocTypes.contains(docType) => true }
        .where("appName") { case appName if allowedAppNames.contains(appName) => true }
        .where("submissionDate") { case date if date == currentDate.format(dateFormatter) => true }
        .records(opts.fileLimit.get)
        .map(_.toByteArray)

      val pingsDataframe = pings.toDF("value")

      val outputPath = opts.outputPath()

      aggregate(pingsDataframe)
        .repartition(opts.numParquetFiles())
        .write
        .mode("overwrite")
        .partitionBy("submission_date_s3")
        .parquet(s"${outputPath}/${outputPrefix}")
    }

    if (shouldStopContextAtEnd(spark)) {
      spark.stop()
    }
  }

  private[streaming] def aggregate(messages: DataFrame): DataFrame = {

    import messages.sparkSession.implicits._
    import spark.sql.functions._

    val events: Dataset[ExperimentEnrollmentEvent] = messages.flatMap(v => {
      val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
      val fields = m.fieldsAsMap
      val docType = fields.getOrElse("docType", "").asInstanceOf[String]
      if (!allowedDocTypes.contains(docType)) {
        Array.empty[ExperimentEnrollmentEvent]
      } else {
        try {
          val mainPing = MainPing(m)
          mainPing.getNormandyEvents.map { e =>
            val timestamp = mainPing.meta.normalizedTimestamp()
            val submissionDate = Instant.ofEpochMilli(timestamp.getTime).atZone(ZoneId.of("UTC")).toLocalDate.format(dateFormatter)
            ExperimentEnrollmentEvent(e.method, e.value, e.extra.flatMap(m => m.get("branch")), e.`object`, timestamp, submissionDate)
          }
        } catch {
          // TODO: track parse errors
          case _: Throwable => Array.empty[ExperimentEnrollmentEvent]
        }
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
    val outputPath: ScallopOption[String] = opt[String](
      "outputPath",
      descr = "Output path",
      required = false,
      default = Some("/tmp/parquet"))
    val failOnDataLoss: ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val checkpointPath: ScallopOption[String] = opt[String](
      "checkpointPath",
      descr = "Checkpoint path (streaming mode only)",
      required = false,
      default = Some("/tmp/checkpoint"))
    val startingOffsets: ScallopOption[String] = opt[String](
      "startingOffsets",
      descr = "Starting offsets (streaming mode only)",
      required = false,
      default = Some("latest"))
    val numParquetFiles: ScallopOption[Int] = opt[Int](
      "numParquetFiles",
      descr = "Number of parquet files per submission_date_s3 (batch mode only)",
      required = false,
      default = Some(defaultNumFiles)
    )

    requireOne(kafkaBroker, from)
    conflicts(kafkaBroker, List(from, to, numParquetFiles))
    verify()
  }

}
