/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.io.File
import java.util.concurrent.TimeUnit

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.holdenkarau.spark.testing.StructuredStreamingBase
import com.mozilla.telemetry.streaming.EnrollmentEvents.{ExperimentA, ExperimentB, enrollmentEventJson}
import com.mozilla.telemetry.sinks.HttpSink
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec, GivenWhenThen, Matchers}

class ExperimentEnrollmentsToTestTubeTest extends FlatSpec with Matchers with GivenWhenThen with StructuredStreamingBase with BeforeAndAfterEach {

  val Port = 9876
  val Host = "localhost"
  val path = "/enrollment"
  val k = 2

  private val wireMockServer = new WireMockServer(wireMockConfig().port(Port))

  val streamingCheckpointPath = "/tmp/checkpoint"

  "Experiment Enrollment forwarder" should "send aggregate enrollment events to TestTube" in {
    import spark.implicits._

    Given("set of main pings with experiment enrollment events")
    val mainPings = (
      TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = true))
        ++ TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = false))
        ++ TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentB, Some("one"), enroll = true))
        ++ TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentB, None, enroll = false))
      ).map(_.toByteArray).seq

    val pingsStream = MemoryStream[Array[Byte]]

    val httpSink = new HttpSink(s"http://$Host:$Port$path", Map())(ExperimentEnrollmentsToTestTube.httpSendMethod)

    When("pings are aggregated and sent")
    val query = ExperimentEnrollmentsToTestTube.aggregateAndSend(pingsStream.toDF(), httpSink, streamingCheckpointPath)
    pingsStream.addData(mainPings)
    query.processAllAvailable()

    // send some more data in order to advance watermark and trigger sink commits
    pingsStream.addData(TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentB, Some("one"), enroll = true),
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(6))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("TestTube receives data in correct format")
    verify(3, postRequestedFor(urlMatching(path)))
    verify(1, postRequestedFor(urlMatching(path)).withRequestBody(equalToJson(
      """
        |{"enrollment": [
        |  {
        |    "experiment_id" : "pref-flip-timer-speed-up-60-1443940",
        |    "branch_id" : "six",
        |    "type" : "preference_study",
        |    "unenroll_count" : 2,
        |    "enroll_count" : 2,
        |    "window_start" : 1460036100000,
        |    "window_end" : 1460036400000,
        |    "submission_date_s3" : "20160407"
        |  }
        |]}""".stripMargin, true, true)))
    verify(1, postRequestedFor(urlMatching(path)).withRequestBody(equalToJson(
      """
        |{"enrollment":[
        |  {
        |    "experiment_id" : "pref-flip-search-composition-57-release-1413565",
        |    "branch_id" : "one",
        |    "type" : "preference_study",
        |    "enroll_count" : 2,
        |    "unenroll_count" : 0,
        |    "window_start" : 1460036100000,
        |    "window_end" : 1460036400000,
        |    "submission_date_s3" : "20160407"
        |  }
        |]}""".stripMargin, true, true)))
    verify(1, postRequestedFor(urlMatching(path)).withRequestBody(equalToJson(
      """
        |{"enrollment":[
        |  {
        |    "experiment_id" : "pref-flip-search-composition-57-release-1413565",
        |    "branch_id" : null,
        |    "type" : "preference_study",
        |    "enroll_count" : 0,
        |    "unenroll_count" : 2,
        |    "window_start" : 1460036100000,
        |    "window_end" : 1460036400000,
        |    "submission_date_s3" : "20160407"
        |  }
        |]}""".stripMargin, true, true)))
  }

  it should "read enrollment events from event pings" in {
    import spark.implicits._

    Given("set of main and event pings with experiment enrollment events")
    val pings = (
      TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = true))
        ++ TestUtils.generateEventMessages(k)
      ).map(_.toByteArray).seq

    val pingsStream = MemoryStream[Array[Byte]]
    val httpSink = new HttpSink(s"http://$Host:$Port$path", Map())(ExperimentEnrollmentsToTestTube.httpSendMethod)

    When("pings are aggregated and sent")
    val query = ExperimentEnrollmentsToTestTube.aggregateAndSend(pingsStream.toDF(), httpSink, streamingCheckpointPath)
    pingsStream.addData(pings)
    query.processAllAvailable()

    // send some more data in order to advance watermark and trigger sink commits
    pingsStream.addData(TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentB, Some("one"), enroll = true),
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(6))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("TestTube receives data aggregated from both ping types")
    verify(2, postRequestedFor(urlMatching(path)))
    verify(1, postRequestedFor(urlMatching(path)).withRequestBody(equalToJson(
      """
        |{"enrollment": [
        |  {
        |    "experiment_id" : "pref-flip-timer-speed-up-60-1443940",
        |    "branch_id" : "six",
        |    "type" : "preference_study",
        |    "unenroll_count" : 0,
        |    "enroll_count" : 2,
        |    "window_start" : 1460036100000,
        |    "window_end" : 1460036400000,
        |    "submission_date_s3" : "20160407"
        |  }
        |]}""".stripMargin, true, true)))
    verify(1, postRequestedFor(urlMatching(path)).withRequestBody(equalToJson(
      """
        |{"enrollment": [
        |  {
        |    "experiment_id" : "awesome-experiment",
        |    "branch_id" : "control",
        |    "type" : "preference_study",
        |    "unenroll_count" : 0,
        |    "enroll_count" : 2,
        |    "window_start" : 1460036100000,
        |    "window_end" : 1460036400000,
        |    "submission_date_s3" : "20160407"
        |  }
        |]}""".stripMargin, true, true)))
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    wireMockServer.start()
    WireMock.configureFor(Host, Port)
    stubFor(post(urlMatching(path)).willReturn(aResponse().withStatus(200)))
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    WireMock.reset()
    wireMockServer.stop()
    FileUtils.deleteDirectory(new File(streamingCheckpointPath))
  }
}
