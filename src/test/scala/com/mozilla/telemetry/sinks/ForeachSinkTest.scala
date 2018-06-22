/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.atomic.AtomicInteger

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class ForeachSinkTest extends FlatSpec with DataFrameSuiteBase with Matchers {

  "ForeachSink" should "receive data in batches" in {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val input = MemoryStream[Ping]

    val query = input.toDS().repartition(2)
      .withWatermark("ts", "1 minute")
      .groupBy(
        window($"ts", "30 minutes", "30 minutes", "28 minutes"),
        $"version")
      .agg(
        avg($"loss").as("avgLoss"),
        count($"version").as("count"))
      .writeStream
      .queryName("ForeachSinkTest")
      .format("com.mozilla.telemetry.sinks.CustomSinkProvider")
      .start()

    // add data to first window
    input.addData(Ping(ts("10:00:00"), "1", 1.0))
    query.processAllAvailable()

    // add data to second window, close first window
    input.addData(Ping(ts("10:30:00"), "1", 1.0))
    query.processAllAvailable()

    // add data to second window
    input.addData(Ping(ts("10:35:00"), "1", 1.0))
    query.processAllAvailable()

    // add data to third window (open), close second window
    input.addData(Ping(ts("11:00:00"), "1", 1.0))
    query.processAllAvailable()

    // trigger outputting of second window
    input.addData()
    query.processAllAvailable()

    Counters.nonEmptyBatches.get() shouldBe 2 // because two windows were finished
    Counters.data should contain theSameElementsAs Seq(
      SinkInputRow(Window("09:58:00, 10:28:00"), "1", 1.0, 1),
      SinkInputRow(Window("10:28:00, 10:58:00"), "1", 1.0, 2)
    )
    Counters.totalBatches.get() shouldBe 5 // because `query.processAllAvailable()` is called 5 times
  }

  private def ts(time: String) = {
    new Timestamp(LocalDateTime.parse(s"2018-07-03T$time").atZone(ZoneId.systemDefault()).toInstant.toEpochMilli)
  }
}

case class Ping(ts: Timestamp, version: String, loss: Double, label: String = "test")

object Counters {
  lazy val data: ArrayBuffer[SinkInputRow] = ArrayBuffer.empty[SinkInputRow]
  val nonEmptyBatches = new AtomicInteger(0)
  val totalBatches = new AtomicInteger(0)
}

class CustomSinkProvider extends ForeachSinkProvider {
  override def f(batchId: Long, df: DataFrame) {

    val batch = df.collect().map(SinkInputRow(_))
    Counters.totalBatches.addAndGet(1)

    if (batch.nonEmpty) Counters.nonEmptyBatches.addAndGet(1)

    // scalastyle:off print-ln
    batch.foreach(println)
    // scalastyle:on print-ln
    batch.foreach(Counters.data.append(_))
  }
}

case class SinkInputRow(window: Window, version: String, avgLoss: Double, count: Long)

object SinkInputRow {
  def apply(row: Row): SinkInputRow = {
    SinkInputRow(
      Window(
        row.getAs[Row]("window").getAs[Timestamp]("start"),
        row.getAs[Row]("window").getAs[Timestamp]("end")),
      row.getAs[String]("version"),
      row.getAs[Double]("avgLoss"),
      row.getAs[Long]("count")
    )
  }
}

case class Window(start: java.sql.Timestamp, end: java.sql.Timestamp)

object Window {
  def apply(windowString: String): Window = {
    val range = windowString.split(", ")
    Window(
      new Timestamp(LocalDateTime.parse(s"2018-07-03T${range(0)}").atZone(ZoneId.systemDefault()).toInstant.toEpochMilli),
      new Timestamp(LocalDateTime.parse(s"2018-07-03T${range(1)}").atZone(ZoneId.systemDefault()).toInstant.toEpochMilli)
    )
  }
}
