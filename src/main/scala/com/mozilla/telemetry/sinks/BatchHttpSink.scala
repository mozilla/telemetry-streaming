/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import org.apache.spark.sql.ForeachWriter
import scalaj.http.{Http, HttpRequest}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class BatchHttpSink(url: String, maxAttempts: Int = 5, defaultDelay: Int = 500, connectionTimeout: Int = 2000,
                    prefix: String = "", sep: String = "\n", suffix: String = "", maxBatchSize: Int = 1,
                    retryCodes: List[Int] = List.empty[Int], successCode: Int = 200)
  extends ForeachWriter[String] {

  val TimeoutPseudoCode = -1
  val ErrorPseudoCode = -2

  val RetryCodes = TimeoutPseudoCode :: Nil ++ retryCodes
  val OK = successCode

  // timeouts in ms
  val ReadTimeout = 5000

  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName)

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

      val success = attempt(
        Http(url.toString)
          .postData(payload)
          .timeout(connTimeoutMs = connectionTimeout, readTimeoutMs = ReadTimeout))

      if (!success) {
        log.warn(s"Failed payload: $payload")
      }
    }
  }

  private def backoff(tries: Int): Long = (scala.math.pow(2, tries) - 1).toLong * defaultDelay

  @tailrec
  private def attempt(request: HttpRequest, tries: Int = 0): Boolean = {
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
      case (OK, _) => true
      case (ErrorPseudoCode, _) => true
      case (c, false) if RetryCodes.contains(c) => attempt(request, tries + 1)
      case (c, _) => {
        val url = request.url + "?" + request.params.map{ case(k, v) => s"$k=$v" }.mkString("&")
        log.warn(s"Failed request: $url, last status code: $c")
        false
      }
    }
  }
}
