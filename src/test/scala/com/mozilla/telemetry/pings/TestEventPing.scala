/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.streaming.TestUtils
import org.scalatest.{FlatSpec, Matchers}

class TestEventPing extends FlatSpec with Matchers {
  val message = TestUtils.generateEventMessages(1).head
  val eventPing = EventPing(message)

  "EventPing" should "contain event meta fields" in {
    eventPing.payload.lostEventsCount should be (0)
    eventPing.payload.processStartTimestamp should be (1530291900000L)
    eventPing.payload.reason should be ("periodic")
    eventPing.payload.sessionId should be ("dd302e9d-569b-4058-b7e8-02b2ff83522c")
    eventPing.payload.subsessionId should be ("79a2728f-af12-4ed3-b56d-0531a03c2f26")
  }

  "EventPing" should "parse events correctly" in {
    eventPing.events should be(Seq(
      Event(4118829, "activity_stream", "end", "session", Some("909"),
        Some(Map("addon_version" -> "2018.06.22.1337-8d599e17", "user_prefs" -> "63",
          "session_id" -> "{78fe2428-15fb-4448-b517-cbb85f22def0}", "page" -> "about:newtab"))),
      Event(4203540, "normandy", "enroll", "preference_study", Some("awesome-experiment"),
        Some(Map("branch" -> "control", "experimentType" -> "exp"))),
        Event(4203541, "test", "no", "string_value", None, Some(Map("hello" -> "world"))),
        Event(4203542, "test", "no", "extras", None, None)
    ))
  }
}
