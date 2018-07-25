/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import com.mozilla.telemetry.heka.{Message, Dataset => MozDataset}
import com.mozilla.telemetry.pings.{EventPing, MainPing}
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption

object ExperimentEnrollmentsAggregator extends StreamingJobBase {
  override val outputPrefix = "experiment_enrollments/v1"

  val kafkaCacheMaxCapacity = 100

  private val allowedDocTypes = List("main", "event")
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
      .option("subscribe", TelemetryKafkaTopic)
      .option("startingOffsets", opts.startingOffsets())
      .load()

    val outputPath = opts.outputPath()

    aggregate(pings.select("value"))
      .repartition(1)
      .writeStream
      .queryName(QueryName)
      .format("delta")
      .option("checkpointLocation", opts.checkpointPath())
      .partitionBy("submission_date_s3")
      .start(outputPath)
      .awaitTermination()
  }

  def writeBatchAggregates(spark: SparkSession, opts: Opts): Unit = {
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
      try {
        val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
        val fields = m.fieldsAsMap
        // TODO: looks like we can get a double here
        // https://dbc-caf9527b-e073.cloud.databricks.com/#setting/sparkui/0605-205051-amid13/driver-logs
        val docType = fields.getOrElse("docType", "").asInstanceOf[String]
        if (!allowedDocTypes.contains(docType)) {
          Array.empty[ExperimentEnrollmentEvent]
        } else {
          val (timestamp, normandyEvents) = {
            if (docType == "main") {
              val mainPing = MainPing(m)
              val timestamp = mainPing.meta.normalizedTimestamp()
              (timestamp, mainPing.getNormandyEvents)
            } else {
              val eventPing = EventPing(m)
              val timestamp = eventPing.meta.normalizedTimestamp()
              (timestamp, eventPing.getNormandyEvents)
            }
          }

          val submissionDate = timestampToDateString(timestamp)
          normandyEvents.map { e =>
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
    val outputPath: ScallopOption[String] = opt[String](
      "outputPath",
      descr = "Output path",
      required = false,
      default = Some("/tmp/parquet"))
    val failOnDataLoss: ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val numParquetFiles: ScallopOption[Int] = opt[Int](
      "numParquetFiles",
      descr = "Number of parquet files per submission_date_s3 (batch mode only)",
      required = false,
      default = Some(60)
    )

    conflicts(kafkaBroker, List(from, to, numParquetFiles))
    verify()
  }
}
