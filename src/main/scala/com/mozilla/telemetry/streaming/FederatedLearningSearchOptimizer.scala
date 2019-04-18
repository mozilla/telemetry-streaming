/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp
import java.time.Clock

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.FrecencyUpdatePing
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.rogach.scallop.ScallopOption
import com.mozilla.telemetry.util.PrettyPrint

import scala.collection.mutable

object FederatedLearningSearchOptimizer extends StreamingJobBase {
  override val JobName: String = "federated_learning_search_optimizer"

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Federated learning - awesomebar optimizer")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val pings = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", opts.kafkaBroker())
      .option("kafka.max.partition.fetch.bytes", 8 * 1024 * 1024) // 8MB
      .option("spark.streaming.kafka.consumer.cache.maxCapacity", 100)
      .option("subscribe", opts.topic())
      .option("startingOffsets", opts.startingOffsets())
      .option("failOnDataLoss", opts.failOnDataLoss())
      .load()
      .select("value")

    val query = optimize(pings,
      opts.checkpointPath(), opts.modelOutputBucket(),
      opts.modelOutputKey(), opts.modelBranch(),
      opts.stateCheckpointPath(), opts.stateBootstrapFilePath.get,
      Clock.systemUTC(), opts.windowOffsetMinutes(),
      opts.raiseOnError(), opts.s3EndpointOverride.get)

    query.awaitTermination()
  }

  def optimize(pings: DataFrame, checkpointPath: String, modelOutputBucket: String, modelOutputKey: String, // scalastyle:ignore
               modelBranch: String, stateCheckpointPath: String, stateBootstrapFilePath: Option[String] = None,
               clock: Clock, windowOffsetMin: Int, raiseOnError: Boolean = false, s3EndpointOverride: Option[String] = None): StreamingQuery = {
    val aggregates = aggregate(pings, modelBranch, clock, windowOffsetMin, raiseOnError)
    writeUpdates(aggregates, checkpointPath, modelOutputBucket, modelOutputKey, stateCheckpointPath, stateBootstrapFilePath, s3EndpointOverride)
  }

  def aggregate(pings: DataFrame, modelBranch: String, clock: Clock, windowOffsetMin: Int, raiseOnError: Boolean = false): Dataset[FrecencyUpdateAggregate] = {
    import pings.sparkSession.implicits._

    val frecencyUpdates: Dataset[FrecencyUpdate] = pings.flatMap { v =>
      try {
        val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
        val fields = m.fieldsAsMap
        val docType = fields.getOrElse("docType", "").asInstanceOf[String]
        if ("frecency-update" == docType) {
          val ping = FrecencyUpdatePing(m)
          if (
              (
               (ping.payload.study_variation startsWith modelBranch) &&
              !(ping.payload.study_variation contains "not-submitting")
              ) &&
              ( ping.payload.bookmark_and_history_num_suggestions_displayed > -1)
            ) {
            Option(FrecencyUpdate(
              new Timestamp(clock.millis()),
              ping.payload.model_version,
              ping.payload.loss,
              ping.payload.update,
              ping.meta.clientId
            )
            )
          } else {
            None
          }
        } else {
          None
        }
      } catch {
        case _: Throwable if !raiseOnError => None
      }
    }
    val NumberOfWeights = 22
    frecencyUpdates.withWatermark("ts", "0 minutes")
      .groupBy(
        window($"ts", "30 minutes", "30 minutes", s"$windowOffsetMin minutes"),
        $"modelVersion")
      .agg(
        avg($"loss").as("avgLoss"),
        count("*").as("count"),
        array((0 until NumberOfWeights) map (i => avg($"updates" (i))): _*).alias("avgUpdates"),
        approx_count_distinct($"client_id", 0.02).as("approxClientCount")
      )
      .as[FrecencyUpdateAggregate]
  }

  def writeUpdates(aggregates: Dataset[FrecencyUpdateAggregate], checkpointPath: String, modelOutputBucket: String, modelOutputKey: String,
          stateCheckpointPath: String,
          stateBootstrapFilePath: Option[String], s3EndpointOverride:
          Option[String] = None): StreamingQuery = {
    val writer = aggregates.writeStream
      .format("com.mozilla.telemetry.learning.federated.FederatedLearningSearchOptimizerS3SinkProvider")
      .option("checkpointLocation", checkpointPath)
      .option("modelOutputBucket", modelOutputBucket)
      .option("modelOutputKey", modelOutputKey)

    val writerWithStateConf = stateBootstrapFilePath match {
      case Some(path) => writer.option("stateBootstrapFilePath", path)
      case None => writer
    }

    val writerWithEndpointConf = s3EndpointOverride match {
      case Some(endpoint) => writerWithStateConf.option("s3EndpointOverride", endpoint)
      case None => writerWithStateConf
    }

    writerWithEndpointConf.option("stateCheckpointPath", stateCheckpointPath)
      .queryName(QueryName)
      .start()
  }

  private class Opts(args: Array[String]) extends BaseOpts(args) {
    val failOnDataLoss: ScallopOption[Boolean] = opt[Boolean](
      descr = "Whether to fail the query when it’s possible that data is lost.",
      default = Some(false))
    val modelOutputBucket: ScallopOption[String] = opt[String](
      name = "modelOutputBucket",
      descr = "S3 bucket to save public model iterations",
      required = true)
    val modelOutputKey: ScallopOption[String] = opt[String](
      name = "modelOutputKey",
      descr = "S3 key to save public model iterations",
      required = true)
    val modelBranch: ScallopOption[String] = opt[String](
      name = "modelBranch",
      descr = "Experiment model branch that we are going to be updating",
      required = true)
    val stateCheckpointPath: ScallopOption[String] = opt[String](
      name = "stateCheckpointPath",
      descr = "Location to save model optimizer state",
      required = true)
    val stateBootstrapFilePath: ScallopOption[String] = opt[String](
      name = "stateBootstrapFilePath",
      descr = "Path to a file with initial optimizer state",
      required = false)
    val windowOffsetMinutes: ScallopOption[Int] = opt[Int](
      name = "windowOffsetMinutes",
      descr = "Offset for processing windows",
      required = false,
      default = Some(28))
    val raiseOnError: ScallopOption[Boolean] = opt[Boolean](
      name = "raiseOnError",
      descr = "Whether the program should exit on a data processing error or not.",
      default = Some(false))
    val s3EndpointOverride: ScallopOption[String] = opt[String](
      name = "s3EndpointOverride",
      descr = "Optional endpoint override for s3 client",
      required = false)
    val topic: ScallopOption[String] = opt[String](
      name = "topic",
      descr = "Kafka topic to pull pings from",
      default = Some("frecency-update"),
      required = false
    )

    requireOne(kafkaBroker)
    verify()
  }
}

case class FrecencyUpdate(ts: Timestamp, modelVersion: Long, loss: Double, updates: Array[Double], client_id: Option[String])

case class FrecencyUpdateAggregate(window: Window, modelVersion: Long, avgLoss: Double, avgUpdates: Array[Double], count: Long, approxClientCount: Long)
  extends PrettyPrint

object FrecencyUpdateAggregate {
  def apply(row: Row): FrecencyUpdateAggregate = {
    FrecencyUpdateAggregate(
      Window(
        row.getAs[Row]("window").getAs[Timestamp]("start"),
        row.getAs[Row]("window").getAs[Timestamp]("end")),
      row.getAs[Long]("modelVersion"),
      row.getAs[Double]("avgLoss"),
      row.getAs[mutable.WrappedArray[Double]]("avgUpdates").toArray,
      row.getAs[Long]("count"),
      row.getAs[Long]("approxClientCount")
    )
  }
}

case class Window(start: java.sql.Timestamp, end: java.sql.Timestamp)
