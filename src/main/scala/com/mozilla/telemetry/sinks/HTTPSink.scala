// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming.sinks

import scalaj.http.{Http, HttpRequest}
import org.apache.spark.sql.ForeachWriter

class HttpSink[String](url: String, parameters: Map[String, String]) extends ForeachWriter[String] {
  def close(errorOrNull: Throwable): Unit = {}
  def open(partitionId: Long,version: Long): Boolean = true

  def getRequest: HttpRequest = {
    parameters.foldLeft(Http(url.toString)){
      case (r, e) => r.param(e._1.toString, e._2.toString)
    }
  }

  def process(event: String): Unit = {
    getRequest
      .param("event", event.toString)
      .asString
  }
}
