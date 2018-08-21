/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import scalaj.http.HttpRequest

import scala.collection.AbstractIterator

case class AmplitudeBatchHttpSink(
  apiKey: String,
  url: String = "https://api.amplitude.com/batch",
  maxEventsPerBatch: Int = 2000,  // Amplitude will return a 413 for more than 2000 events.
  maxBytesPerBatch: Int = 10 * 1024 * 1024,  // 10 MB, half of the API's 20 MB payload limit.
  maxAttempts: Int = Int.MaxValue,
  defaultDelayMillis: Int = 500,
  maxDelayMillis: Int = 30000,
  connectionTimeoutMillis: Int = 2000,
  readTimeoutMillis: Int = 5000
) extends HttpSink[Seq[String]] {

  override def httpSendMethod(request: HttpRequest, events: Seq[String]): HttpRequest = {
    val uploadRequestBody =
      s"""{"api_key":"$apiKey","events":[${events.mkString(",")}]}"""
    log.info(s"Sending a batch of length ${uploadRequestBody.length} containing ${events.length} events")
    request.postData(uploadRequestBody)
      .header("Content-Type", "application/json")
      .header("Accept", "*/*")
  }

  def processInBatches(events: Iterator[String]): Unit = {
    splitIntoBatches(events).foreach(process)
  }

  /**
    * Split an iterator of event strings into batches, each no larger than approximately maxBytesPerBatch
    * and having fewer than maxEventsPerBatch entries such that we stay under the Amplitude batch API
    * payload size limit of 20 MB and event count limit of 2000.
    *
    * See https://developers.amplitude.com/#post-batch
    */
  def splitIntoBatches(input: Iterator[String]): Iterator[Seq[String]] =
    new AbstractIterator[Seq[String]] {
      private var it: Iterator[String] = input

      override def hasNext: Boolean = it.hasNext

      override def next(): Seq[String] = {
        var count: Int = 0
        var bytes: Int = 0
        val p = { s: String =>
          count += 1
          bytes += 2 * s.length
          bytes <= maxBytesPerBatch && count <= maxEventsPerBatch
        }
        val (batch, remainder) = it.span(p)
        it = remainder
        batch.toSeq
      }
    }

}
