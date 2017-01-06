package com.mozilla.telemetry.streaming

import java.nio.file.{Files, Path}

import com.google.protobuf.ByteString
import com.holdenkarau.spark.testing.StreamingActionBase
import com.mozilla.telemetry.heka.{Field, Message}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SQLContext
import org.scalatest._

case class Ping(docType: String,
                normalizedChannel: String,
                appName: String,
                geoCountry: String,
                subsessionLength: Int)

class TestAggregator extends FlatSpec with Matchers with StreamingActionBase with BeforeAndAfterAll{
  private var path: Path = Files.createTempDirectory("telemetry")

  def generateCrashPings(size: Int): Seq[(String, Message)] = {
    1.to(size).map { _ =>
      ("", Message(ByteString.EMPTY, 0, fields = List(
        Field("docType", valueString = List("crash")),
        Field("normalizedChannel", valueString = List("release")),
        Field("appName", valueString = List("Firefox")),
        Field("geoCountry", valueString = List("IT"))
      )))
    }
  }

  def generateMainPings(size: Int): Seq[(String, Message)] = {
    1.to(size).map { _ =>
      ("", Message(ByteString.EMPTY, 0, fields = List(
        Field("docType", valueString = List("main")),
        Field("normalizedChannel", valueString = List("release")),
        Field("appName", valueString = List("Firefox")),
        Field("geoCountry", valueString = List("IT")),
        Field("payload.info", valueString = List("""{"subsessionLength": 3600}"""))
      )))
    }
  }

  "The aggregator" should "sum metrics over a set of dimensions" in {
    val messages = List(generateCrashPings(42) ++ generateMainPings(42))
    runAction(messages, Aggregator.process(path.toString))

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.parquet(path.toString)

    df.count() should be (1)
    df.select("channel").first()(0) should be ("release")
    df.select("application").first()(0) should be ("Firefox")
    df.select("country").first()(0) should be ("IT")
    df.select("crashes").first()(0) should be (42)
    df.select("count").first()(0) should be (42)
    df.select("usageHours").first()(0) should be (42.0)
    df.where("date is null").count() should be (0)
    df.where("timestamp is null").count() should be (0)
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(path.toFile())
  }
}