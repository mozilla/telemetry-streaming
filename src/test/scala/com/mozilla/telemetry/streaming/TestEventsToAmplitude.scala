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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import scalaj.http.Http

import scala.collection.JavaConversions._

class TestEventsToAmplitude extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  object DockerEventsTag extends Tag("DockerEventsTag")

  val Port = 9876
  val Host = "localhost"

  val apiKey = "test-api-key"
  val wireMockServer = new WireMockServer(wireMockConfig().port(Port))

  implicit val formats = DefaultFormats

  val k = TestUtils.scalarValue

  val spark = SparkSession.builder()
    .appName("Events to Amplitude")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .master("local[1]")
    .getOrCreate()

  override def beforeEach {
    wireMockServer.start()
    WireMock.configureFor(Host, Port)
  }

  override def afterEach {
    wireMockServer.stop()
  }

  "HTTPSink" should "send events correctly" in {
    val path = "/testendpoint"
    val eventParts = s"""{"app_version": "1.1", "os_name": "Android", "os_version": "23", "country": "CA", "city": "Victoria"}"""
    val eventsJson = "[" + List.fill(4)(eventParts).mkString(",") + "]"

    stubFor(get(urlMatching(path + "\\?.*"))
      .withQueryParam("api_key", equalTo(apiKey))
      .withQueryParam("event", equalToJson(eventsJson, true, true))
      .willReturn(aResponse().withStatus(200)))

    val msgs = TestUtils.generateFocusEventMessages(k)
    val sink = new sinks.HttpSink(s"http://$Host:$Port$path", Map("api_key" -> apiKey))
    msgs.foreach(m => sink.process(FocusEventPing(m).getEvents))

    verify(k, getRequestedFor(urlMatching(path + "\\?.*")))
  }

  "Events to Amplitude" should "send events via HTTP request" taggedAs(Kafka.DockerComposeTag, DockerEventsTag) in {
    val path = "/httpapi"
    val eventParts = s"""{"app_version": "1.1", "os_name": "Android", "os_version": "23", "country": "CA", "city": "Victoria"}"""
    val eventsJson = "[" + List.fill(4)(eventParts).mkString(",") + "]"

    stubFor(get(urlMatching(path + "\\?.*"))
      .withQueryParam("api_key", equalTo(apiKey))
      .withQueryParam("event", equalToJson(eventsJson, true, true))
      .willReturn(aResponse().withStatus(200)))

    import spark.implicits._

    Kafka.createTopic(EventsToAmplitude.kafkaTopic)
    val kafkaProducer = Kafka.makeProducer(EventsToAmplitude.kafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val messages = TestUtils.generateFocusEventMessages(k).map(_.toByteArray)

    val expectedTotalMsgs = k
    val listener = new StreamingQueryListener {
      var messagesSeen = 0L
      var sentMessages = false
      var progressEvents = 0

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        messagesSeen += event.progress.numInputRows
        progressEvents += 1

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

    val args = 
      "--kafkaBroker"     :: Kafka.kafkaBrokers             ::
      "--startingOffsets" :: "latest"                       ::
      "--url"             :: s"http://$Host:$Port$path"     ::
      "--raiseOnError"    :: Nil

    EventsToAmplitude.main(args.toArray)

    verify(expectedTotalMsgs, getRequestedFor(urlMatching(path + "\\?.*")))

    kafkaProducer.close
    spark.streams.removeListener(listener)
  }
}
