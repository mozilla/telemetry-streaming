// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.pings.FocusEventPing
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers, Tag}

import org.apache.spark.sql.streaming.StreamingQueryListener
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.matching.{EqualToJsonPattern, MatchResult, ValueMatcher}
import com.github.tomakehurst.wiremock.http.Request

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import scalaj.http.Http

import scala.collection.JavaConversions._

import scala.io.Source

import java.util.function.Consumer

import java.net.{URLDecoder, URLEncoder}

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class TestEventsToAmplitude extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  object DockerEventsTag extends Tag("DockerEventsTag")

  val Port = 9876
  val Host = "localhost"
  val ConfigFileName = "/testConfigFile.json"

  val apiKey = "test-api-key"
  var wireMockServer: WireMockServer = _

  implicit val formats = DefaultFormats

  val expectedTotalMsgs = TestUtils.scalarValue

  val spark = SparkSession.builder()
    .appName("Events to Amplitude")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .master("local[1]")
    .getOrCreate()

  // mocked server pieces
  val path = "/httpapi"

  // present in all events
  val sharedEventsJson = s"""{"app_version": "1.1", "os_name": "Android", "os_version": "23", "country": "CA", "city": "Victoria", "device_id": "client1"}"""

  // these keys are present in all events, but values differ
  val requiredKeys = "session_id" :: "insert_id" :: "time" :: Nil

  // specific events we expect to see
  val eventsJson =
    s"""{ "event_type": "m_foc - AppOpen" }""" ::
    s"""{ "event_type": "m_foc - Erase", "event_properties": { "erase_object": "erase_home" }}""" ::
    s"""{ "event_type": "m_foc - AppClose", "event_properties": { "session_length": "1000" }}""" :: Nil

  val jsonMatch = JArray(
      eventsJson.map{
        e => parse(e) merge parse(sharedEventsJson)
      }
    )

  override def beforeEach {
    wireMockServer = new WireMockServer(wireMockConfig().port(Port))
    wireMockServer.start()
    WireMock.configureFor(Host, Port)

    stubFor(get(urlMatching(path + "\\?.*"))
      .withQueryParam("api_key", equalTo(apiKey))
      .willReturn(aResponse().withStatus(200)))
  }

  implicit class JValueExtended(value: JValue) {
    def has(childString: String): Boolean = (value \ childString) != JNothing
  }

  override def afterEach {
    verify(expectedTotalMsgs,
      requestMadeFor(new ValueMatcher[Request] {
        // scalastyle:off methodName
        def `match`(request: Request): MatchResult = {
          val events = URLDecoder.decode(request.queryParameter("event").values.head, "UTF-8")
          MatchResult.of(
            request.getUrl.startsWith(path) &&
            request.queryParameter("api_key").values.head == apiKey &&
            parse(events).asInstanceOf[JArray].arr.flatMap(e => requiredKeys.map(k => e.has(k))).reduce(_ & _) &&
            (new EqualToJsonPattern(compact(jsonMatch), true, true))
              .`match`(events)
              .isExactMatch()
          )
        }
        // scalastyle:on methodName
      })
    )

    wireMockServer.stop()
  }

  private def configFilePath: String = {
    getClass.getResource(ConfigFileName).getPath
  }

  def encode(in: String): String = URLEncoder.encode(in, "UTF-8")

  "HTTPSink" should "send events correctly" in {
    val config = EventsToAmplitude.readConfigFile(configFilePath)
    val msgs = TestUtils.generateFocusEventMessages(expectedTotalMsgs)
    val sink = new sinks.HttpSink(s"http://$Host:$Port$path", Map("api_key" -> apiKey))

    msgs.foreach(m => sink.process(encode(FocusEventPing(m).getEvents(config))))

    verify(expectedTotalMsgs, getRequestedFor(urlMatching(path + "\\?.*")))
  }

  "Events to Amplitude" should "send events via HTTP request" taggedAs(Kafka.DockerComposeTag, DockerEventsTag) in {
    import spark.implicits._

    Kafka.createTopic(EventsToAmplitude.kafkaTopic)
    val kafkaProducer = Kafka.makeProducer(EventsToAmplitude.kafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val messages = (TestUtils.generateFocusEventMessages(expectedTotalMsgs)
      ++ TestUtils.generateMainMessages(expectedTotalMsgs)).map(_.toByteArray) // should ignore main messages

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

    val args =
      "--kafka-broker"     :: Kafka.kafkaBrokers            ::
      "--starting-offsets" :: "latest"                      ::
      "--url"              :: s"http://$Host:$Port$path"    ::
      "--config-file-path" :: configFilePath                ::
      "--raise-on-error"   :: Nil

    EventsToAmplitude.main(args.toArray)

    kafkaProducer.close
    spark.streams.removeListener(listener)
  }
}
