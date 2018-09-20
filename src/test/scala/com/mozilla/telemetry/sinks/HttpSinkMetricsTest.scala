/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import java.io.File

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.custom.{AccumulatorMetricsSource, AccumulatorMetricsSourceTestBase}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers, PrivateMethodTester}

import scala.io.Source

class HttpSinkMetricsTest extends FlatSpec
  with AccumulatorMetricsSourceTestBase with HttpSinkTestBase
  with BeforeAndAfterEach
  with Matchers with PrivateMethodTester {

  override protected val MetricsSourceName = "HttpSink"
  override protected val MetricName = "success"
  val NumAccumulators = 5

  var metricsSource: AccumulatorMetricsSource = _
  var spark: SparkSession = _

  override protected def beforeEach(): Unit = {
    metricsSource = new AccumulatorMetricsSource("HttpSink")

    spark = SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.metrics.conf.*.sink.csv.class", "org.apache.spark.metrics.sink.CsvSink")
      .config("spark.metrics.conf.*.sink.csv.period", "1")
      .config("spark.metrics.conf.*.sink.csv.directory", MetricsOutputDir)
      .config("spark.metrics.namespace", "${spark.app.name}")
      .getOrCreate()

    val httpSinkConfig = HttpSink.Config(
      maxAttempts = maxAttempts,
      defaultDelayMillis = delay,
      connectionTimeoutMillis = timeout)
      .withMetrics(metricsSource)(spark.sparkContext)

    httpSink = AmplitudeHttpSink(
      apiKey,
      s"http://$Host:$Port$Path",
      httpSinkConfig)

    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    try super.afterEach()
    finally spark.stop()
  }

  "HttpSink.Metrics" should "output metrics" in {
    val responseCodes = OK :: OK :: Nil

    metricsSource.start()
    httpSink.config.metrics.get.retry.add(1L)
    httpSink.config.mark(_.error)

    multiStub(responseCodes)
    httpSink.process(Seq(event))
    httpSink.process(Seq(event))
    verifyCount(responseCodes.size)

    // Stop metricsSource, which should force a final flush to metrics outputs.
    val stop = PrivateMethod[Unit]('stop)
    metricsSource invokePrivate stop()
    SparkEnv.get.metricsSystem.report()

    val customMetricFiles = new File(MetricsOutputDir)
      .listFiles()
      .filter(_.getName.startsWith(s"metrics-test.driver.$MetricsSourceName"))

    customMetricFiles should have size 5

    val counterFile = customMetricFiles.filter(_.getName.endsWith(s"$MetricName.csv")).head
    val maxCount = Source.fromFile(counterFile).getLines().toSeq.tail // first line is a header
      .map { line =>
      val count = line.split(",")(1) // second column is count, next are rates
      count.toLong
    }.max
    assert(maxCount == responseCodes.size,
      s"Maximum measured count should be ${responseCodes.size}. Collected metrics were: \n"
        + printContents(customMetricFiles))
  }

}
