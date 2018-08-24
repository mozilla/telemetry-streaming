/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import org.scalatest.{FlatSpec, Matchers}
import scalaj.http.{Http, StringBodyConnectFunc}

case class Event(device_id: String)
case class UploadRequestBody(api_key: String, events: Seq[Event])

class AmplitudeHttpSinkTest extends FlatSpec with Matchers {

  val batchUrl = "https://api.amplitude.com/batch"

  "AmplitudeBatcher" should "split batches by size" in {
    val testStrings = List.fill(5)("abcdefghij")

    val batcher = AmplitudeHttpSink.Batcher(maxBytesPerBatch = 50)
    val result: List[List[String]] = batcher(testStrings.iterator).map(_.toList).toList

    result should contain theSameElementsAs
      List(List("abcdefghij", "abcdefghij"), List("abcdefghij", "abcdefghij"), List("abcdefghij"))
  }

  it should "split batches by count" in {
    val testStrings = List.fill(5)("abcdefghij")

    val batcher = AmplitudeHttpSink.Batcher(maxBytesPerBatch = Int.MaxValue, maxEventsPerBatch = 2)
    val result: List[List[String]] = batcher(testStrings.iterator).map(_.toList).toList

    result should contain theSameElementsAs
      List(List("abcdefghij", "abcdefghij"), List("abcdefghij", "abcdefghij"), List("abcdefghij"))
  }

  it should "produce valid JSON" in {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.{read, write}
    implicit val formats = Serialization.formats(NoTypeHints)

    val events = Seq(Event("abcde"), Event("fghei"))

    Seq(UploadRequestBody("foo", Seq.empty)) should have length 1

    val httpSink = AmplitudeHttpSink("foo", batchUrl)
    val bodyStr = httpSink
      .httpSendMethod(Http(httpSink.url), events.map(write(_)))
      .connectFunc.asInstanceOf[StringBodyConnectFunc].data

    val body = read[UploadRequestBody](bodyStr)
    body.api_key should be ("foo")
    body.events should contain theSameElementsAs events
  }

}
