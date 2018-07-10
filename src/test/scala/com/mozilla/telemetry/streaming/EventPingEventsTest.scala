/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}


class EventPingEventsTest extends FlatSpec with Matchers with DataFrameSuiteBase {
  "Event Ping Events" can "explode events into dataset" in {
    import spark.implicits._

    val messageDF = spark.sparkContext.parallelize(
      TestUtils.generateEventMessages(10).map(_.toByteArray)
    ).toDF("value")

    val results = EventPingEvents.explodeEvents(messageDF).persist

    results.count should be(40)
    results.filter("event_process = 'parent'").count == 20
    results.filter("event_process = 'dynamic'").count == 20
    results.first should be(
      EventPingEvents.EventRow("an_id", "client1", "release", "IT", None, "Firefox", "42.0", Some("Linux"), Some("42"),
        "dd302e9d-569b-4058-b7e8-02b2ff83522c", "79a2728f-af12-4ed3-b56d-0531a03c2f26", 1530291900000L, 1460036116,
        None, Some(Map("experiment2" -> "chaos", "experiment1" -> "control")), 4118829, "activity_stream", "end",
        "session", Some("909"),
        Some(Map("addon_version" -> "2018.06.22.1337-8d599e17", "user_prefs" -> "63",
          "session_id" -> "{78fe2428-15fb-4448-b517-cbb85f22def0}", "page" -> "about:newtab")), "parent"))
  }

}
