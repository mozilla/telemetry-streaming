/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import scalaj.http.HttpRequest

case class AmplitudeHttpSink(
  apiKey: String,
  url: String = "https://api.amplitude.com/httpapi",
  maxAttempts: Int = Int.MaxValue,
  defaultDelayMillis: Int = 500,
  maxDelayMillis: Int = 30000,
  connectionTimeoutMillis: Int = 2000,
  readTimeoutMillis: Int = 5000
) extends HttpSink[String] {

  /**
    * Note that `event` may be either a single JSON object describing an event or it may be a
    * JSON array of event objects. This class does no handling for those two cases;
    * it's expected that the input `event` string follows one of those two formats. See:
    *
    * https://developers.amplitude.com/#request-format
    *
    * Note, however, that when sending a JSON array via this API, Amplitude states that
    * "we do not recommend sending more than 10 events per batch" due to throttling rules,
    * though they later mention request size limits are 2000 events or 1 MB.
    * So far, our batching strategy has been to make one request per ping and we have not explicitly
    * checked batch size, so we could hit a non-retryable 413 status code.
    */
  override def httpSendMethod(request: HttpRequest, event: String): HttpRequest = {
    val params = Map(
      "api_key" -> apiKey,
      "event" -> event
    )
    request.postForm(params.toSeq)
  }
}
