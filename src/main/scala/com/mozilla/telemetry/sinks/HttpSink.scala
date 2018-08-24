/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import org.apache.spark.sql.ForeachWriter
import scalaj.http.{Http, HttpRequest}

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object HttpSink {

  case class Config(
    maxAttempts: Int = 5,
    defaultDelayMillis: Int = 500,
    maxDelayMillis: Int = 30000,
    connectionTimeoutMillis: Int = 2000,
    readTimeoutMillis: Int = 5000,
    successCodes: Set[Int] = Set(OK),
    retryCodes: Set[Int] = RetryCodes
  )

  val TimeoutPseudoCode: Int = -1
  val ErrorPseudoCode: Int = -2
  val OK = 200
  val Conflict = 409
  val PayloadTooLarge = 413
  val TooManyRequests = 429
  val InternalServerError = 500
  val BadGateway = 502
  val ServiceUnavailable = 503
  val GatewayTimeout = 504

  /**
    * The general set of HTTP status codes that indicate an error that can be resolved by retrying.
    *
    * For example, these are called out specifically in Amplitude's documentation:
    * https://developers.amplitude.com/#http-status-codes--amp--retrying-failed-requests
    */
  val RetryCodes: Set[Int] =
    Set(TimeoutPseudoCode, Conflict, TooManyRequests, InternalServerError, BadGateway, ServiceUnavailable, GatewayTimeout)
}

/**
  * An abstract base class for sending data to an HTTP API.
  *
  * @tparam T type of the values passed in to httpSendMethod
  */
abstract class HttpSink[T]() extends ForeachWriter[T] {

  // Classes should implement these as vals in the constructor.
  val url: String
  val config: HttpSink.Config

  protected final val baseRequest = Http(url)
    .timeout(connTimeoutMs = config.connectionTimeoutMillis, readTimeoutMs = config.readTimeoutMillis)

  import HttpSink._

  // Using a transient logger for Spark application per https://stackoverflow.com/a/30453662/1260237
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("HttpSink")

  /**
    * This method will wrapped in retry logic and called to submit data to the target API.
    */
  def httpSendMethod(request: HttpRequest, value: T): HttpRequest

  /**
    * By default, we drop requests that return 413, but classes can override this
    * method to recover and send smaller requests.
    */
  def handlePayloadTooLarge(value: T): Unit = {
    log.warn("Dropping request that returned 413: Payload Too Large")
  }

  override def close(errorOrNull: Throwable): Unit = {
    errorOrNull match {
      case null =>
      case e => log.error(e.getStackTrace.mkString("\n"))
    }
  }

  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: T): Unit = {
    attempt(value, httpSendMethod(baseRequest, value))
  }

  private def backoffMillis(tries: Int): Long = {
    val millis = (scala.math.pow(2, tries) - 1).toLong * config.defaultDelayMillis
    math.min(millis, config.maxDelayMillis)
  }

  @tailrec
  protected final def attempt(value: T, request: HttpRequest, tries: Int = 0): Unit = {
    val nextTry = tries + 1

    if(tries > 0) { // minor optimization
      java.lang.Thread.sleep(backoffMillis(tries))
    }

    val (code, response) = Try(request.asString) match {
      case Success(r) => (r.code, Some(r))
      case Failure(_: java.net.SocketTimeoutException) => (TimeoutPseudoCode, None)
      case Failure(e) if NonFatal(e) => {
        log.error(e.getStackTrace.mkString("\n"))
        (ErrorPseudoCode, None)
      }
    }

    code match {
      case ErrorPseudoCode => // pass; nothing more we can do to fix the problem.
      case _ if config.successCodes.contains(code) => // pass; our work here is done.
      case PayloadTooLarge => handlePayloadTooLarge(value)
      case _ if nextTry < config.maxAttempts && config.retryCodes.contains(code) =>
        attempt(value, request, nextTry)
      case _ =>
        val body = response.map(": " + _.body).getOrElse("")
        log.warn(s"Dropping request that failed with last status code `$code' $body")
    }
  }
}
