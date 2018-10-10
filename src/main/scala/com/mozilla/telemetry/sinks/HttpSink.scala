/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import org.apache.spark.metrics.source.custom.AccumulatorMetricsSource
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.util.LongAccumulator
import scalaj.http.{Http, HttpRequest}

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object HttpSink {

  /**
    * Configuration options for HttpSink subclasses.
    *
    * These could all be constructor parameters, but we use a separate case class instead to
    * avoid having to duplicate and keep the parameter list in sync across many subclasses.
    */
  case class Config(
    maxAttempts: Int = 5,
    defaultDelayMillis: Int = 500,
    maxDelayMillis: Int = 30000,
    connectionTimeoutMillis: Int = 2000,
    readTimeoutMillis: Int = 5000,
    successCodes: Set[Int] = Set(OK),
    retryCodes: Set[Int] = RetryCodes,
    metrics: Option[Metrics] = None
  ) {
    /** Return a copy of this config with HttpSink metrics enabled. */
    def withMetrics(implicit spark: SparkSession): Config = {
      val source = new AccumulatorMetricsSource("HttpSink")
      val config = this.withMetrics(source)(spark)
      source.start()
      config
    }

    /** Return a copy of this config with HttpSink metrics enabled, registering accumulators with the given source. */
    def withMetrics(source: AccumulatorMetricsSource)(implicit spark: SparkSession): Config = {
      require(this.metrics.isEmpty, "Metrics are already enabled for this HttpSink.Config")
      val metrics = Metrics.registeredTo(source)
      this.copy(metrics = Option(metrics))
    }

    /** If metrics are enabled for this config, add 1 on the target accumulator. */
    def mark(f: Metrics => LongAccumulator): Unit = {
      metrics.map(f).foreach(_.add(1L))
    }
  }

  object Metrics {
    /**
      * Factory method that creates a Metrics instance, registering each accumulator with
      * the given metrics source.
      */
    def registeredTo(source: AccumulatorMetricsSource)(implicit spark: SparkSession): Metrics = {
      def metric(name: String): LongAccumulator = {
        val acc = spark.sparkContext.longAccumulator(name)
        source.registerAccumulator(acc)
        acc
      }

      Metrics(
        error = metric("error"),
        success = metric("success"),
        payloadTooLarge = metric("payload-too-large"),
        retry = metric("retry"),
        dropped = metric("dropped"))
    }
  }

  /**
    * A container for accumulators tracking HTTP response code rates.
    *
    * We separate these to a separate class because a SparkContext is needed for registering
    * these accumulators. By keeping the accumulators out of the base HttpSink class, it remains
    * testable without a SparkContext in the case that metrics are not enabled.
    */
  case class Metrics(
    error: LongAccumulator,
    success: LongAccumulator,
    payloadTooLarge: LongAccumulator,
    retry: LongAccumulator,
    dropped: LongAccumulator)
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
abstract class HttpSink[T] extends ForeachWriter[T] {

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
      case ErrorPseudoCode =>
        config.mark(_.error)
        // nothing more to do; this is a problem we can't fix.
      case _ if config.successCodes.contains(code) =>
        config.mark(_.success)
        // nothing more to do; we succeeded!
      case PayloadTooLarge =>
        config.mark(_.payloadTooLarge)
        handlePayloadTooLarge(value)
      case _ if nextTry < config.maxAttempts && config.retryCodes.contains(code) =>
        config.mark(_.retry)
        attempt(value, request, nextTry)
      case _ =>
        val body = response.map(": " + _.body).getOrElse("")
        log.warn(s"Dropping request that failed with last status code `$code' $body")
        config.mark(_.dropped)
    }
  }

}
