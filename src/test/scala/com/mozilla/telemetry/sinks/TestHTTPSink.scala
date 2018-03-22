/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.streaming.sinks.HttpSink
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.http.Request
import com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED
import org.scalatest._
import scala.annotation.tailrec

class TestHTTPSink extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val Port = 9876
  val Host = "localhost"
  val Path = "/httpapi"

  var wireMockServer: WireMockServer = _

  val maxAttempts = 5
  val delay = 1
  val timeout = 100

  val httpSink = new HttpSink(s"http://$Host:$Port$Path", Map.empty, maxAttempts, delay, timeout)

  var scenario = "Response Codes Scenario"
  var event = """{"event": "test event, please ignore"}"""

  val pathMatch = Path + "\\?.*"

  val OK = 200
  val BAD_REQUEST = 400
  val SERVER_ERROR = 503
  val UNKNOWN = 666

  override def beforeEach {
    wireMockServer = new WireMockServer(wireMockConfig().port(Port))
    wireMockServer.start()
    WireMock.configureFor(Host, Port)
  }

  override def afterEach {
    wireMockServer.stop()
  }

  @tailrec
  private def multiStub(responseCodes: Seq[Int], scenarioState: String = STARTED): Unit = {
    val nextScenario = "post" + scenarioState

    stubFor(get(urlMatching(pathMatch))
      .inScenario(scenario)
      .whenScenarioStateIs(scenarioState)
      .willReturn(aResponse().withStatus(responseCodes.head))
      .willSetStateTo(nextScenario))

    if(!responseCodes.tail.isEmpty) {
      multiStub(responseCodes.tail, nextScenario)
    }
  }

  private def verifyCount(count: Int): Unit = {
    verify(count, getRequestedFor(urlMatching(pathMatch)))
  }

  "HTTP Sink" should "only send once on success" in {
    val responseCodes = OK :: Nil

    multiStub(responseCodes)
    httpSink.process(event)
    verifyCount(responseCodes.size)
  }

  it should "keep attempting on failure" in {
    val responseCodes = SERVER_ERROR :: SERVER_ERROR :: SERVER_ERROR :: OK :: Nil

    multiStub(responseCodes)
    httpSink.process(event)
    verifyCount(responseCodes.size)
  }

  it should "stop attempting after too many failures" in {
    val responseCodes = List.fill(maxAttempts)(SERVER_ERROR) :+ OK

    multiStub(responseCodes)
    httpSink.process(event)
    verifyCount(maxAttempts)
  }

  it should "retry after timeout" in {
    val scenario = "retry after timeout"

    stubFor(get(urlMatching(pathMatch))
      .inScenario(scenario)
      .whenScenarioStateIs(STARTED)
      .willSetStateTo("nowait")
      .willReturn(aResponse()
        .withStatus(BAD_REQUEST)
        .withFixedDelay(timeout + 10)))

    stubFor(get(urlMatching(pathMatch))
      .inScenario(scenario)
      .whenScenarioStateIs("nowait")
      .willReturn(aResponse()
        .withStatus(OK)))

    httpSink.process(event)
    verifyCount(1) // only the success
  }

  it should "not retry on unknown codes" in {
    val responseCodes = UNKNOWN :: Nil

    multiStub(responseCodes)
    httpSink.process(event)
    verifyCount(1)
  }
}
