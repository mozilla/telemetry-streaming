package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.{Message, RichMessage}
import com.mozilla.telemetry.pings
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.json4s.jackson.JsonMethods._
import org.json4s.Extraction
import org.json4s.DefaultFormats


class TestAggregator extends FlatSpec with Matchers{
  implicit val formats = DefaultFormats
  private val application = pings.Application(
    "x86", "20170101000000", "release", "Firefox", "42.0", "Mozilla", "42.0", "x86-msvc"
  )
  private val applicationJson = compact(render(Extraction.decompose(application)))

  def generateCrashPings(size: Int, fieldsOverride: Option[Map[String, Any]]=None): Seq[Message] = {
    val defaultMap = Map(
      "docType" -> "crash",
      "normalizedChannel" -> application.channel,
      "appName" -> application.name,
      "appVersion" -> application.version.toDouble,
      "appBuildId" -> application.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      // Timestamp is in nanoseconds
      "Timestamp" -> 1460036116829920000L,
      "submissionDate" -> "2017-01-01",
      "environment.build" ->
        s"""
           |{
           |  "architecture": "${application.architecture}",
           |  "buildId": "${application.buildId}",
           |  "version": "${application.version}"
           |}""".stripMargin,
      "environment.system" ->"""{"os": {"name": "Linux", "version": "42"}}"""
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }
    1.to(size)map { index =>
      RichMessage(s"crash-${index}",
        outputMap,
        Some(
          s"""
             |{
             |  "payload": {
             |    "crashDate": "2017-01-01"
             |  },
             |  "application": ${applicationJson}
             |}""".stripMargin)
      )
    }
  }
  def generateMainPings(size: Int, fieldsOverride: Option[Map[String, Any]]=None): Seq[Message] = {
    val defaultMap = Map(
      "docType" -> "main",
      "normalizedChannel" -> application.channel,
      "appName" -> application.name,
      "appVersion" -> application.version.toDouble,
      "appBuildId" -> application.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      // Timestamp is in nanoseconds
      "Timestamp" -> (1460036116829920000L),
      "submissionDate" -> "2017-01-01",
      "environment.system" ->"""{"os": {"name": "Linux", "version": "42"}}""",
      "environment.build" ->
        s"""
           |{
             "architecture": "${application.architecture}",
           |  "buildId": "${application.buildId}",
           |  "version": "${application.version}"
           |}""".stripMargin,
      "payload.histograms" ->"""{"BROWSER_SHIM_USAGE_BLOCKED": {"values": {"0": 1}}}""",
      "payload.info" -> """{"subsessionLength": 3600}"""
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }
    1.to(size) map { index =>
      RichMessage(s"main-${index}",
        outputMap,
        Some(s"""{"application": ${applicationJson}}""".stripMargin)
      )
    }
  }

  "The aggregator" should "sum metrics over a set of dimensions" in {
    val spark = SparkSession.builder()
      .appName("Error Aggregates")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val messages = (generateCrashPings(42) ++ generateMainPings(42)).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(messages.toDF, raiseOnError = true, online = false)

    df.count() should be (1)

    df.select("submission_date").first()(0).toString should be ("2016-04-07")
    df.select("channel").first()(0) should be (application.channel)
    df.select("version").first()(0) should be (application.version)
    df.select("build_id").first()(0) should be (application.buildId)
    df.select("application").first()(0) should be (application.name)
    df.select("os_name").first()(0) should be ("Linux")
    df.select("os_version").first()(0) should be ("42")
    df.select("architecture").first()(0) should be (application.architecture)
    df.select("country").first()(0) should be ("IT")

    df.select("main_crashes").first()(0) should be (42)
    df.select("count").first()(0) should be (84)
    df.select("usage_hours").first()(0) should be (42.0)
    df.select("browser_shim_usage_blocked").first()(0) should be (42)
    df.where("window is null").count() should be (0)
  }
}
