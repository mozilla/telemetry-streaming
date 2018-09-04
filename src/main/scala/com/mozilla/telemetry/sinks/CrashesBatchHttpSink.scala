/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import scalaj.http.HttpRequest

import scala.collection.mutable.ArrayBuffer

case class CrashesBatchHttpSink(
  url: String,
  prefix: String = "",
  sep: String = "\n",
  suffix: String = "",
  maxBatchSize: Int = 1,
  config: HttpSink.Config = HttpSink.Config()
) extends HttpSink[String] {

  private val batchCalls = ArrayBuffer[String]()

  override def close(errorOrNull: Throwable): Unit = {
    flush()

    errorOrNull match {
      case null =>
      case e => log.error(e.getStackTrace.mkString("\n"))
    }
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    batchCalls.clear()
    true
  }

  override def httpSendMethod(request: HttpRequest, value: String): HttpRequest = {
    throw new NotImplementedError("This class overrides `process` rather than using this method!")
  }

  override def process(data: String): Unit = {
    batchCalls.append(data)
    if (batchCalls.length >= maxBatchSize) {
      flush()
    }
  }

  def flush(): Unit = {
    if (batchCalls.nonEmpty) {
      val payload = batchCalls.mkString(prefix, sep, suffix)
      batchCalls.clear()
      attempt("", baseRequest.postData(payload))
    }
  }

}
