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
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers, Suite}

import scala.annotation.tailrec

/**
  * For details on creating reusable fixtures with correct beforeEach and afterEach behavior, see
  * http://www.scalatest.org/user_guide/sharing_fixtures#composingFixtures
  */
trait HttpSinkTestBase extends BeforeAndAfterEach { this: Suite =>
  val Port = 9876
  val Host = "localhost"
  val Path = "/httpapi"

  var wireMockServer: WireMockServer = _

  val apiKey = "foo"
  val maxAttempts = 5
  val delay = 1
  val timeout = 100

  var httpSink: AmplitudeHttpSink = _

  var scenario = "Response Codes Scenario"
  var event = """{"event": "test event, please ignore"}"""

  val pathMatch = Path + "\\?.*"

  val OK = 200
  val BAD_REQUEST = 400
  val SERVER_ERROR = 503
  val UNKNOWN = 666

  // silence intentionally failed httpSink logs
  def processEventWithError(): Unit = {
    val prevLogLevel = httpSink.log.getLevel
    httpSink.log.setLevel(Level.ERROR)
    try {
      httpSink.process(Seq(event))
    } finally {
      httpSink.log.setLevel(prevLogLevel)
    }
  }

  override protected def beforeEach() {
    wireMockServer = new WireMockServer(wireMockConfig().port(Port))
    wireMockServer.start()
    WireMock.configureFor(Host, Port)
    super.beforeEach()
  }

  override protected def afterEach() {
    try super.afterEach()
    finally wireMockServer.stop()
  }

  @tailrec
  protected final def multiStub(responseCodes: Seq[Int], scenarioState: String = STARTED): Unit = {
    val nextScenario = "post" + scenarioState

    stubFor(post(urlMatching(Path))
      .inScenario(scenario)
      .whenScenarioStateIs(scenarioState)
      .willReturn(aResponse().withStatus(responseCodes.head))
      .willSetStateTo(nextScenario))

    if(responseCodes.tail.nonEmpty) {
      multiStub(responseCodes.tail, nextScenario)
    }
  }

  protected def verifyCount(count: Int): Unit = {
    verify(count, postRequestedFor(urlMatching(Path)))
  }

}

class HttpSinkTest extends FlatSpec with HttpSinkTestBase with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    httpSink = AmplitudeHttpSink(
      apiKey,
      s"http://$Host:$Port$Path",
      config = HttpSink.Config(
        maxAttempts = maxAttempts,
        defaultDelayMillis = delay,
        connectionTimeoutMillis = timeout))
    super.beforeEach()
  }

  "HTTP Sink" should "only send once on success" in {
    val responseCodes = OK :: Nil

    multiStub(responseCodes)
    httpSink.process(Seq(event))
    verifyCount(responseCodes.size)
  }

  it should "keep attempting on failure" in {
    val responseCodes = SERVER_ERROR :: SERVER_ERROR :: SERVER_ERROR :: OK :: Nil

    multiStub(responseCodes)
    httpSink.process(Seq(event))
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
}
