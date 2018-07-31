/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.pings._
import com.mozilla.telemetry.streaming.EventsToAmplitude.Config
import org.scalatest.{FlatSpec, Matchers}


class FocusEventPingTest extends FlatSpec with Matchers{
  val message = TestUtils.generateFocusEventMessages(1).head
  val ping = SendsToAmplitude(message)
  val ts = TestUtils.testTimestampMillis
  val config = Config("telemetry", Map.empty, Nil)

  "Focus Event Ping" can "read events" in {
    ping.events should contain (Event(176078022, "action", "foreground", "app", None, None))
  }

  it can "correctly sample itself" in {
    val noFilters = config.copy(filters = Map.empty)

    ping.includePing(0.0, noFilters) should be (false)
    ping.includePing(0.72, noFilters) should be (false)
    ping.includePing(0.74, noFilters) should be (true)
    ping.includePing(1.0, noFilters) should be (true)
  }

  it can "correctly filter itself" in {
    val notIncluded = config.copy(filters = Map("os" -> List("iOS")))
    ping.includePing(1.0, notIncluded) should be (false)
  }

  it can "correctly include itself" in {
    val included = config.copy(filters = Map("os" -> List("Android")))
    ping.includePing(1.0, included) should be (true)
  }

  it can "throw on non-existent properties in filter" in {
    val included = config.copy(filters = Map("nonexistent" -> List("Android")))
    an [NoSuchElementException] should be thrownBy ping.includePing(1.0, included) // scalastyle:ignore
  }

  it can "filter on non-string properties" in {
    val included = config.copy(filters = Map("created" -> List("1506024685632")))
    ping.includePing(1.0, included) should be (true)
  }
}
