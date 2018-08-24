/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import scalaj.http.HttpRequest

import scala.collection.AbstractIterator

object AmplitudeHttpSink {
  val Config = HttpSink.Config(maxAttempts = Int.MaxValue)
  val maxBytesHttp: Int = 512 * 1024  //  512 kB, half the documented /httpapi size limit.
  val maxBytesBatch: Int = 10 * 1024 * 1024  // 10 MB, half the documented /batchapi size limit.

  def stringsAsJsonList(events: Seq[String]): String =
    s"""[${events.mkString(",")}]"""

  /**
    * Factory that instantiates a class based on the `url` parameter.
    *
    * The constructors for the concrete classes are private so that we can control matching the url with
    * the appropriate logic for the endpoint.
    */
  def apply(apiKey: String, url: String, config: HttpSink.Config = Config, batcher: Option[Batcher] = None): AmplitudeHttpSink =
    url match {
      case _ if url.endsWith("/httpapi") =>
        val b = batcher.getOrElse(Batcher(maxBytesPerBatch = maxBytesHttp))
        AmplitudeHttpApiSink(apiKey, url, b, config)
      case _ if url.endsWith("/batch") =>
        val b = batcher.getOrElse(Batcher(maxBytesPerBatch = maxBytesBatch))
        AmplitudeBatchApiSink(apiKey, url, b, config)
      case _ =>
        throw new IllegalArgumentException(s"Unknown Amplitude endpoint: $url")
    }

  /**
    * Controls logic for splitting up an iterator of events into batches.
    *
    * Amplitude documents maximum event count and size in their docs; see
    * https://developers.amplitude.com/#http-api
    * https://developers.amplitude.com/#post-batch
    */
  case class Batcher(maxBytesPerBatch: Int, maxEventsPerBatch: Int = 2000) {
    /**
      * Split an iterator of event strings into batches, each no larger than approximately maxBytesPerBatch
      * and having fewer than maxEventsPerBatch entries such that we stay under the Amplitude's limits.
      */
    def apply(input: Iterator[String]): Iterator[Seq[String]] =
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
}

private case class AmplitudeHttpApiSink(apiKey: String, url: String, batcher: AmplitudeHttpSink.Batcher, config: HttpSink.Config) extends AmplitudeHttpSink {

  override def batch(eventsIterator: Iterator[Seq[String]]): Iterator[Seq[String]] = {
    // We try to keep all events for a single ping together in one request to /httpapi;
    // splitIntoBatches will almost always return a single batch here.
    eventsIterator.flatMap(events => batcher(events.iterator))
  }

  override def httpSendMethod(request: HttpRequest, events: Seq[String]): HttpRequest = {
    val params = Seq(
      "api_key" -> apiKey,
      "event" -> AmplitudeHttpSink.stringsAsJsonList(events)
    )
    request.postForm(params)
  }

}

private case class AmplitudeBatchApiSink(apiKey: String, url: String, batcher: AmplitudeHttpSink.Batcher, config: HttpSink.Config) extends AmplitudeHttpSink {

  override def batch(eventsIterator: Iterator[Seq[String]]): Iterator[Seq[String]] = {
    // We want to group together many pings into few requests to /batch,
    // so we send the entire flattened event iterator through the batching algorithm.
    batcher(eventsIterator.flatten)
  }

  override def httpSendMethod(request: HttpRequest, events: Seq[String]): HttpRequest = {
    val body = s"""{"api_key":"$apiKey","events":[${events.mkString(",")}]}"""
    log.info(s"Sending a batch of length ${body.length} containing ${events.length} events")
    request.postData(body)
      .header("Content-Type", "application/json")
      .header("Accept", "*/*")
  }

}

/**
  * Base class for sending to the various Amplitude APIs.
  *
  * Note that we choose the type parameter to be `Seq[String]` rather than String because
  * both implementations are able to accept batches and we want to be able to process batches here
  * rather than sending messages event by event.
  */
private[sinks] abstract class AmplitudeHttpSink extends HttpSink[Seq[String]] {

  val batcher: AmplitudeHttpSink.Batcher

  /**
    * Each subclass must implement this method to control how to split up events into
    * separate HTTP requests.
    */
  def batch(eventsIterator: Iterator[Seq[String]]): Iterator[Seq[String]]

  /**
    * This is the entrypoint for batch processing mode.
    */
  def batchAndProcess(eventsIterator: Iterator[Seq[String]], minDelayMillis: Int = 0): Unit = {
    batch(eventsIterator).foreach { events =>
      super.process(events)
      java.lang.Thread.sleep(minDelayMillis)
    }
  }

  /**
    * This is the entrypoint for streaming mode.
    */
  override def process(events: Seq[String]): Unit = {
    if (events.length <= batcher.maxEventsPerBatch) {
      super.process(events)
    } else {
      // In streaming mode, `process` gets called directly, so we might need to break up a ping's events to smaller batches.
      batch(Iterator(events)).foreach(super.process)
    }
  }

  /**
    * Amplitude states "we do not recommend sending more than 10 events per batch" due to throttling rules,
    * though they later mention request size limits are 2000 events or 1 MB.
    * If we exceed either of those limits, we should receive a 413 response and this method will be invoked
    * to split up the payload.
    *
    * Our batching logic should make 413 responses unlikely, though.
    */
  override def handlePayloadTooLarge(events: Seq[String]): Unit = {
    val midpoint: Int = events.length / 2
    val (firstHalf, secondHalf) = events.splitAt(midpoint)
    process(firstHalf)
    process(secondHalf)
  }

}
