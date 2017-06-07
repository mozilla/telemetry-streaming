// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import org.scalatest.{FlatSpec, Matchers}
import com.mozilla.telemetry.pings._


class TestPings extends FlatSpec with Matchers{

  val message = TestUtils.generateMainMessages(1).head
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
}
