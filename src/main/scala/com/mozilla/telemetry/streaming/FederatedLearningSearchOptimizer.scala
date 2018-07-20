/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp
import java.time.Clock

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.FrecencyUpdatePing
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.rogach.scallop.ScallopOption

import scala.collection.mutable

object FederatedLearningSearchOptimizer extends StreamingJobBase {

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
      .option("subscribe", TelemetryKafkaTopic)
      .option("startingOffsets", opts.startingOffsets())
      .load()
      .select("value")

    val query = optimize(pings,
      opts.checkpointPath(), opts.modelOutputPath(), opts.stateCheckpointPath(), opts.stateBootstrapFilePath.get,
      Clock.systemUTC(), opts.windowOffsetMinutes(), opts.raiseOnError())

    query.awaitTermination()
  }

  def optimize(pings: DataFrame, checkpointPath: String,
               modelOutputPath: String, stateCheckpointPath: String, stateBootstrapFilePath: Option[String] = None,
               clock: Clock, windowOffsetMin: Int, raiseOnError: Boolean = false): StreamingQuery = {
    val aggregates = aggregate(pings, clock, windowOffsetMin, raiseOnError)
    writeUpdates(aggregates, checkpointPath, modelOutputPath, stateCheckpointPath, stateBootstrapFilePath)
  }

  def aggregate(pings: DataFrame, clock: Clock, windowOffsetMin: Int, raiseOnError: Boolean = false): Dataset[FrecencyUpdateAggregate] = {
    import pings.sparkSession.implicits._

    val frecencyUpdates: Dataset[FrecencyUpdate] = pings.flatMap { v =>
      try {
        val m = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
        val fields = m.fieldsAsMap
        val docType = fields.getOrElse("docType", "").asInstanceOf[String]
        if ("frecency-update" == docType) {
          val ping = FrecencyUpdatePing(m)
          if (ping.payload.study_variation == "treatment") {
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

  def writeUpdates(aggregates: Dataset[FrecencyUpdateAggregate], checkpointPath: String, modelOutputPath: String,
                   stateCheckpointPath: String, stateBootstrapFilePath: Option[String]): StreamingQuery = {
    val writer = aggregates.writeStream
      .format("com.mozilla.telemetry.learning.federated.FederatedLearningSearchOptimizerS3SinkProvider")
      .option("checkpointLocation", checkpointPath)
      .option("modelOutputPath", modelOutputPath)

    val writerWithStateConf = stateBootstrapFilePath match {
      case Some(path) => writer.option("stateBootstrapFilePath", path)
      case None => writer
    }

    writerWithStateConf.option("stateCheckpointPath", stateCheckpointPath)
      .queryName(QueryName)
      .start()
  }

  private class Opts(args: Array[String]) extends BaseOpts(args) {
    val modelOutputPath: ScallopOption[String] = opt[String](
      name = "modelOutputPath",
      descr = "Location to save updated model iterations",
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

    requireOne(kafkaBroker)
    verify()
  }
}

case class FrecencyUpdate(ts: Timestamp, modelVersion: Long, loss: Double, updates: Array[Double], client_id: Option[String])

case class FrecencyUpdateAggregate(window: Window, modelVersion: Long, avgLoss: Double, avgUpdates: Array[Double], count: Long, approxClientCount: Long)

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
