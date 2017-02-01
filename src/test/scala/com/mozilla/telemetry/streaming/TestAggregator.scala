package com.mozilla.telemetry.streaming

import java.nio.file.{Files, Path}

import com.google.protobuf.ByteString
import com.mozilla.telemetry.heka.{Field, Message}
import org.apache.spark.sql.SparkSession
import org.scalatest._

case class Ping(docType: String,
                normalizedChannel: String,
                appName: String,
                geoCountry: String,
                subsessionLength: Int)

class TestAggregator extends FlatSpec with Matchers{
  private val path: Path = Files.createTempDirectory("telemetry-test")

  def generateCrashPings(size: Int): Seq[Array[Byte]] = {
    1.to(size).map { _ =>
      Message(ByteString.EMPTY, 0, fields = List(
        Field("docType", valueString = List("crash")),
        Field("normalizedChannel", valueString = List("release")),
        Field("appName", valueString = List("Firefox")),
        Field("geoCountry", valueString = List("IT"))
      )).toByteArray
    }
  }

  def generateMainPings(size: Int): Seq[Array[Byte]] = {
    1.to(size).map { _ =>
      Message(ByteString.EMPTY, 0, fields = List(
        Field("docType", valueString = List("main")),
        Field("normalizedChannel", valueString = List("release")),
        Field("appName", valueString = List("Firefox")),
        Field("geoCountry", valueString = List("IT")),
        Field("payload.info", valueString = List("""{"subsessionLength": 3600}""")),
        Field("payload.histograms", valueString = List("""{"BROWSER_SHIM_USAGE_BLOCKED": {"values": {"0": 1}}}"""))
      )).toByteArray
    }
  }

  "The aggregator" should "sum metrics over a set of dimensions" in {
    val spark = SparkSession.builder()
      .appName("Error Aggregates")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val messages = generateCrashPings(42) ++ generateMainPings(42)
    val df = ErrorAggregator.aggregate(messages.toDF, raiseOnError = true, online = false)

    df.count() should be (1)
    df.select("channel").first()(0) should be ("release")
    df.select("application").first()(0) should be ("Firefox")
    df.select("country").first()(0) should be ("IT")
    df.select("crashes").first()(0) should be (42)
    df.select("count").first()(0) should be (42)
    df.select("usageHours").first()(0) should be (42.0)
    df.select("BROWSER_SHIM_USAGE_BLOCKED").first()(0) should be (42)
    df.where("submission_date is null").count() should be (0)
    df.where("window is null").count() should be (0)
  }
}