/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.atomic.AtomicInteger

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}

// scalastyle:off
class ForeachSinkTest extends FlatSpec with DataFrameSuiteBase with Matchers {

  "ForeachSink" should "receive data in batches" in {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val input = MemoryStream[Ping]


    val formatName = "com.mozilla.telemetry.sinks.CustomSinkProvider"

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
      .format(formatName)
      .start()

    val inputData = Seq(Ping(ts("10:00:00"), "1", 1.0))


    input.addData(inputData)
    query.processAllAvailable()

    input.addData(Ping(ts("10:30:00"), "1", 1.0))
    query.processAllAvailable()

    input.addData(Ping(ts("10:35:00"), "1", 1.0))
    query.processAllAvailable()

    input.addData(Ping(ts("11:00:00"), "1", 1.0))
    query.processAllAvailable()

    input.addData()
    query.processAllAvailable()

    Counter.c.get() shouldBe 2

  }

  private def ts(time: String) = {
    new Timestamp(LocalDateTime.parse(s"2018-07-03T$time").toInstant(ZoneOffset.UTC).toEpochMilli)
  }
}

case class Ping(ts: Timestamp, version: String, loss: Double)

object Counter {
  val c = new AtomicInteger(0)
}

class CustomSinkProvider extends ForeachSinkProvider {
  override def f(batchId: Long, df: DataFrame) {

    println("BATCH: " + batchId)
    df.printSchema()
    val batch = df.collect().map(SinkInputRow(_))

    if (batch.isEmpty) println("EMPTY") else Counter.c.addAndGet(1)
    batch.foreach(println)
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
