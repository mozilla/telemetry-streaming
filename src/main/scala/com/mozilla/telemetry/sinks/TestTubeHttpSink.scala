/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import scalaj.http.HttpRequest

case class TestTubeHttpSink(
  url: String,
  maxAttempts: Int = 5,
  defaultDelayMillis: Int = 500,
  maxDelayMillis: Int = 30000,
  connectionTimeoutMillis: Int = 2000,
  readTimeoutMillis: Int = 5000
) extends HttpSink[String] {

  override def httpSendMethod(request: HttpRequest, data: String): HttpRequest = {
    request
      .postData(s"""{"enrollment":[$data]}""")
      .header("content-type", "application/json")
  }

}
