/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.net.URLDecoder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.http.Request
import com.github.tomakehurst.wiremock.matching.{EqualToJsonPattern, MatchResult, RequestPatternBuilder, ValueMatcher}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.pings.{Meta, SendsToAmplitude}
import com.mozilla.telemetry.sinks
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}
import org.scalatest._

class EventsToAmplitudeTest extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with DataFrameSuiteBase {

  object DockerEventsTag extends Tag("DockerEventsTag")
  object DockerFocusEvents extends Tag("DockerFocusEvents")
  object DockerMainEvents extends Tag("DockerMainEvents")

  val Port = 9876
  val Host = "localhost"
  val ConfigFileName = "/testConfigFile.json"
  val MainEventsConfigFile = "/testMainPingConfigFile.json"

  val apiKey = "test-api-key"
  private val wireMockServer = new WireMockServer(wireMockConfig().port(Port))

  implicit val formats = DefaultFormats

  val expectedTotalMsgs = TestUtils.scalarValue

  val creationDateIso8601 = Meta.epochDayToIso8601(TestUtils.creationDate)

  // mocked server pieces
  val path = "/httpapi"

  // present in all focus events
  private val focusPingJson = s"""{
    "app_version": "1.1",
    "os_name": "Android",
    "os_version": "23",
    "country": "CA",
    "city": "Victoria",
    "device_id": "client1",
    "user_properties": {
      "pref_privacy_block_ads": true,
      "pref_locale": "",
      "pref_privacy_block_social": true,
      "pref_secure": true,
      "pref_privacy_block_analytics": true,
      "pref_search_engine": "custom",
      "pref_privacy_block_other": false,
      "pref_default_browser": true,
      "pref_performance_block_webfonts": false,
      "pref_performance_block_images": false,
      "pref_autocomplete_installed": true,
      "pref_autocomplete_custom": false}
    }}"""

  private val mainPingJson = s"""
    |  {
    |    "user_id": "client1",
    |    "session_id": 1527696000000,
    |    "app_version": "42.0",
    |    "os_name": "Linux",
    |    "os_version": "42",
    |    "country": "IT",
    |    "user_properties": {
    |      "channel": "release",
    |      "sample_id": 73.0,
    |      "app_build_id": "20170101000000",
    |      "app_name": "Firefox",
    |      "locale": "it_IT",
    |      "env_build_arch":"x86",
    |      "is_default_browser": true,
    |      "is_wow64": false,
    |      "memory_mb": 4136.0,
    |      "profile_creation_date": "$creationDateIso8601",
    |      "source": "example.com",
    |      "experiments": ["experiment1_control", "experiment2_chaos"]
    |    }
    |  }
    """.stripMargin

  private val pingSentJson =
    s"""
       |{
       |  "event_type": "Meta - session split",
       |  "event_properties": {
       |    "subsession_length": "3600",
       |    "active_ticks": "271",
       |    "uri_count": "63",
       |    "search_count": "4"
       |  }
       |}
     """.stripMargin

  // these keys are present in all events, but values differ
  private val requiredKeys = "session_id" :: "insert_id" :: "time" :: Nil

  // specific events we expect to see
  private def eventsJson(eventGroup: String) =
    s"""{ "event_type": "$eventGroup - AppOpen" }""" ::
    s"""
      |{
      |   "user_properties" : {
      |      "host" : "side"
      |   },
      |   "event_properties" : {
      |      "literal_field" : "literal value",
      |      "erase_object" : "erase_home"
      |   },
      |   "event_type" : "$eventGroup - Erase"
      |}
      """.stripMargin ::
    s"""
       |{
       |   "event_type" : "second_event_group - AppClose",
       |   "event_properties" : {
       |      "session_length" : "1000"
       |   }
       |}
      """.stripMargin ::
    Nil

  private val focusEventJsonMatch = JArray(
      eventsJson("m_foc").map{
        e => parse(e) merge parse(focusPingJson)
      }
    )

  private val mainPingJsonMatch = JArray(
    parse(pingSentJson) +:
    eventsJson("main_ping").map {
      e => parse(e) merge parse(mainPingJson)
    }
  )

  private def createMatcher(jsonPattern: JValue): RequestPatternBuilder = {
    requestMadeFor(new ValueMatcher[Request] {
      // scalastyle:off methodName
      def `match`(request: Request): MatchResult = {
        val params = request
          .getBodyAsString()
          .split("&")
          .map(v => {
            val m = v.split("=")
            m(0) -> URLDecoder.decode(m(1), "UTF-8")
          }).toMap

        MatchResult.of(
          request.getUrl.startsWith(path) &&
            params("api_key") == apiKey &&
            parse(params("event")).asInstanceOf[JArray].arr.flatMap(e => requiredKeys.map(k => e.has(k))).reduce(_ & _) &&
            (new EqualToJsonPattern(compact(jsonPattern), true, true))
              .`match`(params("event"))
              .isExactMatch()
        )
      }

      // scalastyle:on methodName
    })
  }

  def setUpWireMockServer: Unit = {
    wireMockServer.start()
    WireMock.configureFor(Host, Port)

    stubFor(post(urlMatching(path))
      .willReturn(aResponse().withStatus(200)))
  }

  override def beforeEach {
    wireMockServer.start()
    WireMock.configureFor(Host, Port)

    stubFor(post(urlMatching(path))
      .willReturn(aResponse().withStatus(200)))
  }

  implicit class JValueExtended(value: JValue) {
    def has(childString: String): Boolean = (value \ childString) != JNothing
  }

  override def afterEach {
    WireMock.reset()
    wireMockServer.stop()
  }

  private def configFilePath(filename: String): String = {
    getClass.getResource(filename: String).getPath
  }

  "HTTPSink" should "send focus events correctly" in {
    val config = EventsToAmplitude.readConfigFile(configFilePath(ConfigFileName))
    val msgs = TestUtils.generateFocusEventMessages(expectedTotalMsgs)
    val sink = sinks.AmplitudeHttpSink(apiKey, s"http://$Host:$Port$path")

    msgs.foreach(m => sink.process(SendsToAmplitude(m).getAmplitudeEvents(config).get))

    verify(expectedTotalMsgs, postRequestedFor(urlMatching(path)))
    verify(expectedTotalMsgs, createMatcher(focusEventJsonMatch))
  }

  it should "send main ping events correctly" in {
    val config = EventsToAmplitude.readConfigFile(configFilePath(MainEventsConfigFile))
    val msgs = TestUtils.generateMainMessages(expectedTotalMsgs,
      customPayload=EventsToAmplitudeTest.CustomMainPingPayload)
    val sink = sinks.AmplitudeHttpSink(apiKey, s"http://$Host:$Port$path")

    msgs.foreach(m => sink.process(SendsToAmplitude(m).getAmplitudeEvents(config).get))

    verify(expectedTotalMsgs, postRequestedFor(urlMatching(path)))
    verify(expectedTotalMsgs, createMatcher(mainPingJsonMatch))
  }

  "Events to Amplitude" should "send focus events via HTTP request" taggedAs(Kafka.DockerComposeTag, DockerEventsTag, DockerFocusEvents) in {
    Kafka.createTopic(StreamingJobBase.TelemetryKafkaTopic)
    val kafkaProducer = Kafka.makeProducer(StreamingJobBase.TelemetryKafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    // should ignore main messages
    val messages = (TestUtils.generateFocusEventMessages(expectedTotalMsgs)
      ++ TestUtils.generateMainMessages(expectedTotalMsgs, customPayload=EventsToAmplitudeTest.CustomMainPingPayload))
      .map(_.toByteArray)

    val listener = new StreamingQueryListener {
      var messagesSeen = 0L
      var sentMessages = false

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        messagesSeen += event.progress.numInputRows

        if(!sentMessages){
          send(messages)
          sentMessages = true
        }

        if(messagesSeen == 2 * expectedTotalMsgs){
          spark.streams.active.foreach(_.processAllAvailable)
          spark.streams.active.foreach(_.stop)
        }
      }
    }

    spark.streams.addListener(listener)

    val args = Array(
      "--kafkaBroker", Kafka.kafkaBrokers,
      "--startingOffsets", "latest",
      "--url", s"http://$Host:$Port$path",
      "--config-file-path", configFilePath(ConfigFileName),
      "--raise-on-error")
    val opts = new EventsToAmplitude.Opts(args)

    EventsToAmplitude.process(opts, apiKey)

    kafkaProducer.close
    spark.streams.removeListener(listener)

    verify(expectedTotalMsgs, createMatcher(focusEventJsonMatch))
  }

  it should "send main ping events via HTTP request" taggedAs(Kafka.DockerComposeTag, DockerEventsTag, DockerMainEvents) in {
    Kafka.createTopic(StreamingJobBase.TelemetryKafkaTopic)
    val kafkaProducer = Kafka.makeProducer(StreamingJobBase.TelemetryKafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val messages = (TestUtils.generateFocusEventMessages(expectedTotalMsgs)
      ++ TestUtils.generateMainMessages(expectedTotalMsgs, customPayload=EventsToAmplitudeTest.CustomMainPingPayload))
      .map(_.toByteArray) // should ignore focus event messages

    val listener = new StreamingQueryListener {
      var messagesSeen = 0L
      var sentMessages = false

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        messagesSeen += event.progress.numInputRows

        if(!sentMessages){
          send(messages)
          sentMessages = true
        }

        if(messagesSeen == 2 * expectedTotalMsgs){
          spark.streams.active.foreach(_.processAllAvailable)
          spark.streams.active.foreach(_.stop)
        }
      }
    }

    spark.streams.addListener(listener)

    val args = Array(
      "--kafkaBroker", Kafka.kafkaBrokers,
      "--startingOffsets", "latest",
      "--url", s"http://$Host:$Port$path",
      "--config-file-path", configFilePath(MainEventsConfigFile),
      "--raise-on-error")
    val opts = new EventsToAmplitude.Opts(args)

    EventsToAmplitude.process(opts, apiKey)

    kafkaProducer.close
    spark.streams.removeListener(listener)
    verify(expectedTotalMsgs, createMatcher(mainPingJsonMatch))
  }

  it should "ignore pings without events" taggedAs(Kafka.DockerComposeTag, DockerEventsTag, DockerMainEvents) in {
    Kafka.createTopic(TelemetryKafkaTopic)
    val kafkaProducer = Kafka.makeProducer(TelemetryKafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val messages = (TestUtils.generateFocusEventMessages(expectedTotalMsgs))
      .map(_.toByteArray)

    val listener = new StreamingQueryListener {
      var messagesSeen = 0L
      var sentMessages = false

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        messagesSeen += event.progress.numInputRows

        if(!sentMessages){
          send(messages)
          sentMessages = true
        }

        if(messagesSeen == expectedTotalMsgs){
          spark.streams.active.foreach(_.processAllAvailable)
          spark.streams.active.foreach(_.stop)
        }
      }
    }

    spark.streams.addListener(listener)

    val args = Array(
      "--kafkaBroker", Kafka.kafkaBrokers,
      "--startingOffsets", "latest",
      "--url", s"http://$Host:$Port$path",
      "--config-file-path", configFilePath(MainEventsConfigFile),
      "--raise-on-error")
    val opts = new EventsToAmplitude.Opts(args)

    EventsToAmplitude.process(opts, apiKey)

    kafkaProducer.close
    spark.streams.removeListener(listener)
    verify(0, postRequestedFor(urlMatching(path)))
  }

  "Session Id offset field" should "be added to session id when present" in {
    val config = EventsToAmplitude.readConfigFile(configFilePath(MainEventsConfigFile))
    val msg = TestUtils.generateMainMessages(1,
      customPayload=EventsToAmplitudeTest.sessionIdOffsetPayload).head

    val res = for {
      JObject(l) <- parse(SendsToAmplitude(msg).getAmplitudeEvents(config).get) \\ "session_id"
      JField("session_id", JInt(session_id)) <- l
    } yield session_id

    res should contain only (1527696000000L, 1527696002000L)
  }

  "Schema configs" should "validate against meta-schema" in {
    EventsToAmplitude.readConfigFile("configs/focus_android_events_schemas.json")
    EventsToAmplitude.readConfigFile("configs/desktop_savant_events_schemas.json")
    EventsToAmplitude.readConfigFile("configs/devtools_events_schemas.json")
  }
}

object EventsToAmplitudeTest {
  val CustomMainPingPayload = Some(
    """
      |    "processes": {
      |      "parent": {
      |        "scalars": {
      |          "media.page_count": 4,
      |          "browser.engagement.unique_domains_count": 7,
      |          "browser.engagement.tab_open_event_count": 11,
      |          "browser.engagement.max_concurrent_window_count": 2,
      |          "browser.engagement.max_concurrent_tab_count": 21,
      |          "browser.engagement.unfiltered_uri_count": 128,
      |          "browser.engagement.window_open_event_count": 1,
      |          "browser.errors.collected_with_stack_count": 37,
      |          "browser.engagement.total_uri_count": 63,
      |          "browser.errors.collected_count": 181,
      |          "browser.engagement.active_ticks": 271
      |        },
      |        "events": [
      |          [
      |            176078022,
      |            "action",
      |            "foreground",
      |            "app"
      |          ],[
      |            176127806,
      |            "action",
      |            "type_query",
      |            "search_bar"
      |          ],[
      |            176151285,
      |            "action",
      |            "click",
      |            "back_button",
      |            "erase_home",
      |            { "host": "side" }
      |          ]
      |        ]
      |      },
      |      "content": {
      |        "events": [
      |          [
      |            176151591,
      |            "action",
      |            "background",
      |            "app",
      |            "",
      |            { "sessionLength": "1000" }
      |          ]
      |        ]
      |      }
      |    }
    """.stripMargin)

  val sessionIdOffsetPayload = Some(
    """
      |    "processes": {
      |      "content": {
      |        "events": [
      |          [
      |            176151591,
      |            "action",
      |            "background",
      |            "app",
      |            "",
      |            { "sessionLength": 1000 }
      |          ],
      |          [
      |            176151591,
      |            "action",
      |            "background",
      |            "app",
      |            "",
      |            { "sessionLength": "1000", "session_id": "2000" }
      |          ]
      |        ]
      |      }
      |    }
    """.stripMargin)
}
