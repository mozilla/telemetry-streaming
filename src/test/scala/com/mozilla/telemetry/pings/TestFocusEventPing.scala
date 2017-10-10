package com.mozilla.telemetry.streaming

import org.scalatest.{FlatSpec, Matchers}
import com.mozilla.telemetry.pings._
import org.joda.time.{DateTime, Duration}


class TestFocusEventPing extends FlatSpec with Matchers{
  val message = TestUtils.generateFocusEventMessages(1).head
  val ping = FocusEventPing(message)
  val ts = TestUtils.testTimestampMillis

  "Focus Event Ping" can "read events" in {
    ping.events should contain (Event(176151591, "action", "background", "app", None, None))
  }

  it can "correctly sample itself" in {
    ping.includeClient(0.0) should be (false)
    ping.includeClient(0.72) should be (false)
    ping.includeClient(0.74) should be (true)
    ping.includeClient(1.0) should be (true)
  }
}

