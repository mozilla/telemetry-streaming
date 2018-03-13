// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import org.scalatest.{FlatSpec, Matchers}
import com.mozilla.telemetry.pings._
import com.mozilla.telemetry.streaming.EventsToAmplitude.Config
import org.joda.time.{DateTime, Duration}


class TestFocusEventPing extends FlatSpec with Matchers{
  val message = TestUtils.generateFocusEventMessages(1).head
  val ping = FocusEventPing(message)
  val ts = TestUtils.testTimestampMillis

  "Focus Event Ping" can "read events" in {
    ping.events should contain (Event(176078022, "action", "foreground", "app", None, None))
  }

  it can "correctly sample itself" in {
    val noFilters = Config("name", Map.empty, Nil)

    ping.includePing(0.0, noFilters) should be (false)
    ping.includePing(0.72, noFilters) should be (false)
    ping.includePing(0.74, noFilters) should be (true)
    ping.includePing(1.0, noFilters) should be (true)
  }

  it can "correctly filter itself" in {
    val notIncluded = Config("name", Map("os" -> List("iOS")), Nil)
    ping.includePing(1.0, notIncluded) should be (false)
  }

  it can "correctly include itself" in {
    val included = Config("name", Map("os" -> List("Android")), Nil)
    ping.includePing(1.0, included) should be (true)
  }

  it can "ignore non-existent properties in filter" in {
    val included = Config("name", Map("nonexistent" -> List("Android")), Nil)
    ping.includePing(1.0, included) should be (true)
  }

  it can "filter on non-string properties" in {
    val included = Config("name", Map("created" -> List("1506024685632")), Nil)
    ping.includePing(1.0, included) should be (true)
  }
}
