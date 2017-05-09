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
    "arm", "20170101000000", "beta", "Fennec", "42.0", "Mozilla", "42.0", "arm-eabi-gcc3"
  )
  private val applicationJson = compact(render(Extraction.decompose(application)))

  def generateCrashPings(size: Int): Seq[Message] = {
    1.to(size)map { index =>
      RichMessage(s"crash-${index}",
        Map(
          "docType" -> "crash",
          "normalizedChannel" -> application.channel,
          "appName" -> application.name,
          "appVersion" -> application.version.toDouble,
          "appBuildId" -> application.buildId,
          "geoCountry" -> "IT",
          "os" -> "Android",
          // Timestamp is in nanoseconds
          "Timestamp" -> 1460036116829920000L,
          "submissionDate" -> "2017-01-01",
          "environment.system" ->"""{"os": {"name": "Android", "version": "42"}}"""
        ),
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
  def generateMainPings(size: Int): Seq[Message] = {
    1.to(size) map { index =>
      RichMessage(s"main-${index}",
        Map(
          "docType" -> "main",
          "normalizedChannel" -> application.channel,
          "appName" -> application.name,
          "appVersion" -> application.version.toDouble,
          "appBuildId" -> application.buildId,
          "geoCountry" -> "IT",
          "os" -> "Android",
          // Timestamp is in nanoseconds
          "Timestamp" -> (1460036116829920000L + 3 * scala.math.pow(10,11).toLong),
          "submissionDate" -> "2017-01-01",
          "environment.system" ->"""{"os": {"name": "Android", "version": "42"}}""",
          "payload.histograms" ->"""{"BROWSER_SHIM_USAGE_BLOCKED": {"values": {"0": 1}}}""",
          "payload.info" -> """{"subsessionLength": 3600}"""
        ),
        Some(s"""{"application": ${applicationJson}}""".stripMargin)
      )
    }
  }

  "Error aggregator" should "be able to convert a message to a CrashPing" in {
    val message = generateCrashPings(1)
    ErrorAggregator.messageToCrashPing(message(0))

  }

  "Error aggregator" should "be able to convert a message to a MainPing" in {
    val message = generateMainPings(1)
    ErrorAggregator.messageToMainPing(message(0))
  }

  "PingAggregate" should "be able to create instances from CrashPings" in {
    val message = generateCrashPings(1)
    val ping = ErrorAggregator.messageToCrashPing(message(0))
    val aggregate = new PingAggregate(ping)
    aggregate.crashes.main_crashes should be (1)
    aggregate.usage_hours should be (0.0)
    aggregate.size should be (1.0)
  }

  "PingAggregate" should "be able to create instances from MainPings" in {
    val message = generateMainPings(1)
    val ping = ErrorAggregator.messageToMainPing(message(0))
    val aggregate = new PingAggregate(ping)
    aggregate.crashes.main_crashes should be (0)
    aggregate.usage_hours should be (1.0)
    aggregate.size should be (1.0)
  }

  "The aggregator" should "sum metrics over a set of dimensions" in {
    val spark = SparkSession.builder()
      .appName("Error Aggregates")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val messages = (generateCrashPings(42) ++ generateMainPings(42)).map(_.toByteArray).seq
    val df = ErrorAggregator.aggregate(messages.toDF, raiseOnError = true, online = false)
    df.count() should be (2)
    df.select("channel").first()(0) should be ("beta")
    df.select("application").first()(0) should be ("Fennec")
    df.select("country").first()(0) should be ("IT")

    val main_df = df.orderBy($"usage_hours".desc)
    val crash_df = df.orderBy($"usage_hours".asc)

    main_df.select("usage_hours").first()(0) should be (42.0)
    main_df.select("main_crashes").first()(0) should be (0)
    main_df.select("browser_shim_usage_blocked").first()(0) should be (42.0)
    main_df.select("submission_date").first()(0).toString should be ("2016-04-07")

    crash_df.select("usage_hours").first()(0) should be (0)
    crash_df.select("main_crashes").first()(0) should be (42)
    crash_df.select("browser_shim_usage_blocked").first()(0) should be (0)
    crash_df.select("submission_date").first()(0).toString should be ("2016-04-07")

  }
  "CrashStats" should "sum up correctly" in {
    (CrashStats(1) + CrashStats(2)).main_crashes should be (3)
    (CrashStats(0) + CrashStats(1)).main_crashes should be (1)
    (CrashStats(1) + CrashStats(0)).main_crashes should be (1)
  }

  "ErrorStats" should "sum up correctly" in {
    (ErrorStats(1, 1, 1, 1, 1) + ErrorStats(1, 2, 3, 4, 5)) should be (ErrorStats(2,3,4,5,6))
    (ErrorStats(0, 0, 0, 0, 0) + ErrorStats(1, 2, 3, 4, 5)) should be (ErrorStats(1,2,3,4,5))
    (ErrorStats(1, 2, 3, 4, 5) + ErrorStats(0, 0, 0, 0, 0)) should be (ErrorStats(1,2,3,4,5))
  }
}