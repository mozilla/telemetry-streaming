/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class HttpSinkMetricsTest extends FlatSpec with HttpSinkTestBase with BeforeAndAfterEach with Matchers with DatasetSuiteBase {

  override protected def beforeEach(): Unit = {
    val httpSinkConfig = HttpSink.Config(
      maxAttempts = maxAttempts,
      defaultDelayMillis = delay,
      connectionTimeoutMillis = timeout)
      .withMetrics(spark)

    httpSink = AmplitudeHttpSink(
      apiKey,
      s"http://$Host:$Port$Path",
      httpSinkConfig)

    super.beforeEach()
  }

  "HttpSink.Metrics" should "output metrics" in {
    val responseCodes = OK :: OK :: Nil

    httpSink.config.metrics.get.retry.add(1L)
    httpSink.config.mark(_.error)

    multiStub(responseCodes)
    httpSink.process(Seq(event))
    httpSink.process(Seq(event))
    verifyCount(responseCodes.size)

    val metrics = httpSink.config.metrics.get

    metrics.error.value should be (1L)
    metrics.success.value should be (2L)
    metrics.payloadTooLarge.value should be(0L)
    metrics.retry.value should be (1L)
    metrics.dropped.value should be (0L)
  }

}
