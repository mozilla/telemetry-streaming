/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming.sinks

import scalaj.http.{Http, HttpRequest}
import org.apache.spark.sql.ForeachWriter
import scala.annotation.tailrec
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

class HttpSink[String](url: String, data: Map[String, String], maxAttempts: Int = 5, defaultDelay: Int = 500, connectionTimeout: Int = 2000)
  extends ForeachWriter[String] {

  // codes to retry a request on - allow retries on timeouts
  // docs: https://amplitude.zendesk.com/hc/en-us/articles/204771828#http-status-codes-retrying-failed-requests

  val TimeoutPseudoCode = -1
  val ErrorPseudoCode = -2

  val RetryCodes = TimeoutPseudoCode :: 429 :: 500 :: 502 :: 503 :: 504 :: Nil
  val OK = 200

  // timeouts in ms
  val ReadTimeout = 5000

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("HttpSinkLogger")

  def close(errorOrNull: Throwable): Unit = {
    errorOrNull match {
      case null =>
      case e => log.error(e.getStackTrace.mkString("\n"))
    }
  }

  def open(partitionId: Long, version: Long): Boolean = true

  def process(event: String): Unit = {
    attempt(
      Http(url.toString)
        .postForm(("event" -> event.toString) :: data.toList.map{ case(a, b) => a.toString -> b.toString })
        .timeout(connTimeoutMs = connectionTimeout, readTimeoutMs = ReadTimeout))
  }

  private def backoff(tries: Int): Long = (scala.math.pow(2, tries) - 1).toLong * defaultDelay

  @tailrec
  private def attempt(request: HttpRequest, tries: Int = 0): Unit = {
    if(tries > 0) { // minor optimization
      java.lang.Thread.sleep(backoff(tries))
    }

    val code = Try(request.asString.code) match {
      case Success(c) => c
      case Failure(e: java.net.SocketTimeoutException) => TimeoutPseudoCode
      case Failure(e) if NonFatal(e) => {
        log.error(e.getStackTrace.mkString("\n"))
        ErrorPseudoCode
      }
    }

    (code, tries + 1 == maxAttempts) match {
      case (OK, _) =>
      case (ErrorPseudoCode, _) =>
      case (c, false) if RetryCodes.contains(c) => attempt(request, tries + 1)
      case (c, _) => {
        val url = request.url + "?" + request.params.map{ case(k, v) => s"k=v" }.mkString("&")
        log.warn(s"Failed request: $url, last status code: $c")
      }
    }
  }
}
