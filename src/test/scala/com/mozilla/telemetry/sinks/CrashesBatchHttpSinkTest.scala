/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED
import org.apache.log4j.Level
import org.scalatest._

import scala.annotation.tailrec

class BatchHTTPSinkTest extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val Port = 9876
  val Host = "localhost"
  val Path = "/httpapi"

  var wireMockServer: WireMockServer = new WireMockServer(wireMockConfig().port(Port))

  val maxAttempts = 5
  val delay = 1
  val timeout = 100

  val retryCodes =  List[Int](429, 500, 502, 503, 504)

  val httpSink = new CrashesBatchHttpSink(s"http://$Host:$Port$Path", maxAttempts, delay, timeout,
    prefix = "", sep = "\n", suffix = "", maxBatchSize = 1, retryCodes = retryCodes)

  var scenario = "Response Codes Scenario"
  var eventA = """{"event": "test eventA, please ignore"}"""
  var eventB = """{"event": "test eventB, please ignore"}"""

  val pathMatch = Path + "\\?.*"

  val OK = 200
  val BAD_REQUEST = 400
  val SERVER_ERROR = 503
  val UNKNOWN = 666

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    wireMockServer.stop()
  }

  override def beforeEach {
    WireMock.configureFor(Host, Port)
    stubFor(post(urlMatching(Path)).willReturn(aResponse().withStatus(200)))
  }

  override def afterEach {
    WireMock.reset()
  }

  // silence intentionally failed httpSink logs
  def processEventWithError(): Unit = {
    val prevLogLevel = httpSink.log.getLevel
    httpSink.log.setLevel(Level.ERROR)
    try {
      httpSink.process(eventA)
    } finally {
      httpSink.log.setLevel(prevLogLevel)
    }
  }

  @tailrec
  private def multiStub(responseCodes: Seq[Int], scenarioState: String = STARTED): Unit = {
    val nextScenario = "post" + scenarioState

    stubFor(post(urlMatching(Path))
      .inScenario(scenario)
      .whenScenarioStateIs(scenarioState)
      .willReturn(aResponse().withStatus(responseCodes.head))
      .willSetStateTo(nextScenario))

    if(!responseCodes.tail.isEmpty) {
      multiStub(responseCodes.tail, nextScenario)
    }
  }

  private def verifyCount(count: Int): Unit = {
    verify(count, postRequestedFor(urlMatching(Path)))
  }

  private def verifyRequestString(count: Int, body: String): Unit = {
    verify(count, postRequestedFor(urlMatching(Path)).withRequestBody(equalTo(body)))
  }

  private def verifyRequestJson(count: Int, body: String): Unit = {
    verify(count, postRequestedFor(urlMatching(Path)).withRequestBody(equalToJson(body)))
  }

  "Batch HTTP Sink" should "clear all stored events each time it is flushed" in {
    val k = 11

    for (i <- 1 to k) {
      httpSink.process(eventA)
    }

    verifyRequestString(k, eventA)
  }

  it should "not send anything when flushed with empty buffer" in {
    httpSink.flush()
    verifyCount(0)
  }

  it should "keep attempting on failure" in {
    val responseCodes = SERVER_ERROR :: SERVER_ERROR :: SERVER_ERROR :: OK :: Nil

    multiStub(responseCodes)
    httpSink.process(eventA)
    verifyCount(responseCodes.size)
  }

  it should "stop attempting after too many failures" in {
    val responseCodes = List.fill(maxAttempts)(SERVER_ERROR) :+ OK

    multiStub(responseCodes)
    processEventWithError()
    verifyCount(maxAttempts)
  }

  it should "retry after timeout" in {
    val scenario = "retry after timeout"

    stubFor(post(urlMatching(Path))
      .inScenario(scenario)
      .whenScenarioStateIs(STARTED)
      .willSetStateTo("nowait")
      .willReturn(aResponse()
        .withStatus(BAD_REQUEST)
        .withFixedDelay(timeout + 10)))

    stubFor(post(urlMatching(Path))
      .inScenario(scenario)
      .whenScenarioStateIs("nowait")
      .willReturn(aResponse()
        .withStatus(OK)))

    processEventWithError()
    verifyCount(1) // only the success
  }

  it should "not retry on unknown codes" in {
    val responseCodes = UNKNOWN :: Nil

    multiStub(responseCodes)
    processEventWithError()
    verifyCount(1)
  }

  "Batch HTTP Sink with batch size 1" should "send a single event unmodified" in {
    httpSink.process(eventA)
    verifyRequestString(1, eventA)
  }

  "Batch HTTP Sink with batch size > 1" should "send once when batch size is reached" in {
    val batchSize = 4
    val batchHttpSink = new CrashesBatchHttpSink(
      s"http://$Host:$Port$Path", maxAttempts, delay, timeout,
      prefix = "", sep = "\n", suffix = "", maxBatchSize = batchSize)

    for (i <- 1 to batchSize) {
      batchHttpSink.process(eventA)
    }

    val expected =
      s"""$eventA
         |$eventA
         |$eventA
         |$eventA""".stripMargin

    verifyRequestString(1, expected)
  }

  it should "send each time batch size is reached" in {
    val batchSize = 2
    val batchHttpSink = new CrashesBatchHttpSink(
      s"http://$Host:$Port$Path", maxAttempts, delay, timeout,
      prefix = "", sep = "\n", suffix = "", maxBatchSize = batchSize)

    val k = 8
    for (i <- 1 to k) {
      batchHttpSink.process(eventA)
    }

    val expected =
      s"""$eventA
         |$eventA""".stripMargin

    verifyRequestString(k / batchSize, expected)
  }

  it should "retain order of events processed" in {
    val batchSize = 5
    val batchHttpSink = new CrashesBatchHttpSink(
      s"http://$Host:$Port$Path", maxAttempts, delay, timeout,
      prefix = "", sep = "\n", suffix = "", maxBatchSize = batchSize)

    batchHttpSink.process(eventA)
    batchHttpSink.process(eventA)
    batchHttpSink.process(eventB)
    batchHttpSink.process(eventA)
    batchHttpSink.process(eventB)

    val expected =
      s"""$eventA
         |$eventA
         |$eventB
         |$eventA
         |$eventB""".stripMargin

    verifyRequestString(1, expected)
  }

  it should "correctly add prefix, suffix, and event separators to the request" in {
    val batchHttpSink = new CrashesBatchHttpSink(
      s"http://$Host:$Port$Path", maxAttempts, delay, timeout,
      prefix = "{\"events\": [", sep = ", ", suffix = "]}", maxBatchSize = 5)

    batchHttpSink.process(eventA)
    batchHttpSink.process(eventA)
    batchHttpSink.process(eventB)
    batchHttpSink.process(eventB)
    batchHttpSink.process(eventA)

    val expected =
      s"""{"events":[
         |$eventA,
         |$eventA,
         |$eventB,
         |$eventB,
         |$eventA]}""".stripMargin

    verifyRequestJson(1, expected)
  }
}
