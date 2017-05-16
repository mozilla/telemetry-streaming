// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import org.scalatest.{FlatSpec, Matchers}
import com.mozilla.telemetry.pings._


class TestPings extends FlatSpec with Matchers{

  val message = TestUtils.generateMainMessages(1).head
  val mainPing = messageToMainPing(message)

  "MainPing" should "return the value of a count histogram" in {
    mainPing.getCountHistogramValue("foo") should be (0)
    mainPing.getCountHistogramValue("BROWSER_SHIM_USAGE_BLOCKED") should be (1)
  }
  it should "return the value of a keyed count histogram" in {
    mainPing.getCountKeyedHistogramValue("foo", "bar") should be (0)
    mainPing.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "foo") should be (0)
    mainPing.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "content") should be (1)
  }
  it should "return the value of its usage hours" in {
    mainPing.usageHours should be (1.0)
    val messageNoUsageHours = TestUtils.generateMainMessages(1, Some(Map("payload.info" -> "{}"))).head
    val pingNoUsageHours = messageToMainPing(messageNoUsageHours)
    pingNoUsageHours.usageHours should be (0.0)
  }
  it should "return its timestamp" in {
    mainPing.meta.normalizedTimestamp() should be (new Timestamp(1460036116829L))
  }


}
