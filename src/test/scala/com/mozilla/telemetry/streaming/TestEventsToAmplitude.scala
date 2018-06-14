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
import com.mozilla.telemetry.pings.SendsToAmplitude
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}
import org.scalatest._

class TestEventsToAmplitude extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with DataFrameSuiteBase {

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
    |      "app_build_id": "20170101000000",
    |      "locale": "it_IT",
    |      "is_default_browser": true,
    |      "experiments": ["experiment1_control", "experiment2_chaos"]
    |    }
    |  }
    """.stripMargin

  // these keys are present in all events, but values differ
  private val requiredKeys = "session_id" :: "insert_id" :: "time" :: Nil

  // specific events we expect to see
  private def eventsJson(eventGroup: String) =
    s"""{ "event_type": "$eventGroup - AppOpen" }""" ::
    s"""{ "event_type": "$eventGroup - Erase", "event_properties": { "erase_object": "erase_home" }, "user_properties": { "host": "side" }}""" ::
    s"""{ "event_type": "second_event_group - AppClose", "event_properties": { "session_length": "1000" }}""" :: Nil

  private val focusEventJsonMatch = JArray(
      eventsJson("m_foc").map{
        e => parse(e) merge parse(focusPingJson)
      }
    )

  private val mainPingJsonMatch = JArray(
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
    val sink = new sinks.HttpSink(s"http://$Host:$Port$path", Map("api_key" -> apiKey))

    msgs.foreach(m => sink.process(SendsToAmplitude(m).getAmplitudeEvents(config).get))

    verify(expectedTotalMsgs, postRequestedFor(urlMatching(path)))
    verify(expectedTotalMsgs, createMatcher(focusEventJsonMatch))
  }

  "HTTPSink" should "send main ping events correctly" in {
    val config = EventsToAmplitude.readConfigFile(configFilePath(MainEventsConfigFile))
    val msgs = TestUtils.generateMainMessages(expectedTotalMsgs,
      customPayload=TestEventsToAmplitude.CustomMainPingPayload)
    val sink = new sinks.HttpSink(s"http://$Host:$Port$path", Map("api_key" -> apiKey))

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
      ++ TestUtils.generateMainMessages(expectedTotalMsgs, customPayload=TestEventsToAmplitude.CustomMainPingPayload))
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

  "Events to Amplitude" should "send main ping events via HTTP request" taggedAs(Kafka.DockerComposeTag, DockerEventsTag, DockerMainEvents) in {
    Kafka.createTopic(StreamingJobBase.TelemetryKafkaTopic)
    val kafkaProducer = Kafka.makeProducer(StreamingJobBase.TelemetryKafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val messages = (TestUtils.generateFocusEventMessages(expectedTotalMsgs)
      ++ TestUtils.generateMainMessages(expectedTotalMsgs, customPayload=TestEventsToAmplitude.CustomMainPingPayload))
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

  "Events to Amplitude" should "ignore pings without events" taggedAs(Kafka.DockerComposeTag, DockerEventsTag, DockerMainEvents) in {
    Kafka.createTopic(EventsToAmplitude.kafkaTopic)
    val kafkaProducer = Kafka.makeProducer(EventsToAmplitude.kafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val messages = (TestUtils.generateMainMessages(expectedTotalMsgs))
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
}

object TestEventsToAmplitude {
  val CustomMainPingPayload = Some(
    """
      |    "processes": {
      |      "parent": {
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
}
