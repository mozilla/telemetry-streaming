/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import com.mozilla.telemetry.pings._
import com.mozilla.telemetry.streaming.EventsToAmplitudeTest.CustomMainPingPayload
import org.scalatest.{FlatSpec, Matchers}


class PingsTest extends FlatSpec with Matchers{

  val ContentHistogramPayload = Some(
    """
      |"processes": {
      |  "content": {
      |    "histograms": {
      |      "INPUT_EVENT_RESPONSE_COALESCED_MS": {
      |        "values": {
      |          "1": 1,
      |          "150": 1,
      |          "250": 1,
      |          "2500": 1,
      |          "10000": 1
      |        }
      |      }
      |    }
      |  },
      |  "parent": {
      |    "scalars": {
      |      "media.page_count": 4,
      |      "browser.engagement.unique_domains_count": 7,
      |      "browser.engagement.tab_open_event_count": 11,
      |      "browser.engagement.max_concurrent_window_count": 2,
      |      "browser.engagement.max_concurrent_tab_count": 21,
      |      "browser.engagement.unfiltered_uri_count": 128,
      |      "browser.engagement.window_open_event_count": 1,
      |      "browser.errors.collected_with_stack_count": 37,
      |      "browser.engagement.total_uri_count": 63,
      |      "browser.errors.collected_count": 181,
      |      "browser.engagement.active_ticks": 271
      |    }
      |  }
      |}
      |""".stripMargin)

  val message = TestUtils.generateMainMessages(1, customPayload = ContentHistogramPayload).head
  val mainPing = MainPing(message)
  val ts = TestUtils.testTimestampMillis

  "MainPing" should "return the value of a count histogram" in {
    mainPing.getCountHistogramValue("foo").isEmpty should be (true)
    mainPing.getCountHistogramValue("BROWSER_SHIM_USAGE_BLOCKED").get should be (1)
  }

  it should "return the value of a keyed count histogram" in {
    mainPing.getCountKeyedHistogramValue("foo", "bar").isEmpty should be (true)
    mainPing.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "foo").isEmpty should be (true)
    mainPing.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "content").get should be (1)
  }

  it should "return the value of a process scalar" in {
    mainPing.getScalarValue("parent", "browser.engagement.total_uri_count").head should be (63)
  }

  it should "return the value of its usage hours" in {
    mainPing.usageHours.get should be (1.0)
    val messageNoUsageHours = TestUtils.generateMainMessages(1, Some(Map("payload.info" -> "{}"))).head
    val pingNoUsageHours = MainPing(messageNoUsageHours)
    pingNoUsageHours.usageHours.isEmpty should be (true)
  }

  it should "return its timestamp" in {
    mainPing.meta.normalizedTimestamp() should be (new Timestamp(ts))
  }

  it should "return the right threshold count" in {
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 150, "main") should be (14)
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 250, "main") should be (12)
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 2500, "main") should be (9)

    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 150, "content") should be (4)
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 250, "content") should be (3)
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 2500, "content") should be (2)
  }

  it should "return its firstPaint value" in {
    mainPing.firstPaint should be (Some(1200))
  }

  it should "detect if it's the first subsession" in {
    mainPing.isFirstSubsession should be (Some(true))
  }

  it should "not return its firstPaint value if non-first subsession" in {
    val subsequentMessage = TestUtils.generateMainMessages(1, Some(Map(
      "payload.info" -> """{"subsessionLength": 3600, "subsessionCounter": 2}"""
    ))).head
    val subsequentPing = MainPing(subsequentMessage)
    subsequentPing.firstPaint should be(None)
  }

  it should "return its sessionId" in {
    mainPing.sessionId should be (Some("sample-session-id"))
  }

  "An OS instance" should "normalize the version" in {
    OS(Some("linux"), Some("1.1.1-ignore")).normalizedVersion shouldBe Some("1.1.1")
    OS(Some("linux"), Some("1.1.1ignore")).normalizedVersion shouldBe Some("1.1.1")
    OS(Some("linux"), Some("1.1")).normalizedVersion shouldBe Some("1.1")
    OS(Some("linux"), Some("1.1-ignore")).normalizedVersion shouldBe Some("1.1")
    OS(Some("linux"), Some("1.1ignore")).normalizedVersion shouldBe Some("1.1")
    OS(Some("linux"), Some("1")).normalizedVersion shouldBe Some("1")
    OS(Some("linux"), Some("1-ignore")).normalizedVersion shouldBe Some("1")
    OS(Some("linux"), Some("1ignore")).normalizedVersion shouldBe Some("1")
    OS(Some("linux"), Some("non-numeric")).normalizedVersion shouldBe None
    OS(Some("linux"), Some("nonnumeric1.1")).normalizedVersion shouldBe None
  }

  "Main Ping" can "read events" in {
    val message = TestUtils.generateMainMessages(1, customPayload=CustomMainPingPayload).head
    val eventedPing = MainPing(message)
    eventedPing.events should contain (Event(176078022, "action", "foreground", "app", None, None))
    // this is a content process event
    eventedPing.events should contain (Event(176151591, "action", "background", "app", Some(""), Some(Map("sessionLength" -> "1000"))))
  }
}
