/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package org.apache.spark.metrics.source.custom

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{ForeachWriter, SQLContext, SparkSession}
import org.scalatest._

import scala.io.Source

/**
  * For details on creating reusable fixtures with correct beforeEach and afterEach behavior, see
  * http://www.scalatest.org/user_guide/sharing_fixtures#composingFixtures
  */
trait AccumulatorMetricsSourceTestBase extends BeforeAndAfterEach { this: Suite =>

  protected val MetricsOutputDir = "/tmp/metrics-test/"
  protected val appName = "metrics-test"
  protected val MetricsSourceName = "ApplicationMetrics"
  protected val MetricName = "test-metric"

  override protected def beforeEach(): Unit = {
    cleanupTestDirectories()
    Files.createDirectories(Paths.get(MetricsOutputDir))
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    try super.afterEach()
    finally cleanupTestDirectories()
  }

  protected def cleanupTestDirectories(): Unit = {
    FileUtils.deleteDirectory(new File(MetricsOutputDir))
  }

  protected def printContents(files: Array[File]): String = {
    files.map { f =>
      "============================================================\n" +
        "File: " + f.getName + "\n" +
        scala.io.Source.fromFile(f).getLines().mkString("\n") + "\n" +
        "============================================================"
    }.mkString("\n")
  }
}

class AccumulatorMetricsSourceTest extends FlatSpec with AccumulatorMetricsSourceTestBase with Matchers with GivenWhenThen {

  "AccumulatorMetricsSource" should "provide accumulator-based metrics" in {
    Given("Spark session configured with csv metrics reporter")
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.metrics.conf.*.sink.csv.class", "org.apache.spark.metrics.sink.CsvSink")
      .config("spark.metrics.conf.*.sink.csv.period", "1")
      .config("spark.metrics.conf.*.sink.csv.directory", MetricsOutputDir)
      .config("spark.metrics.namespace", "${spark.app.name}")
      .config("spark.sql.streaming.metricsEnabled", "true")
      .getOrCreate()
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._

    And("accumulator metrics source with an accumulator registered")
    val metricsSource = new AccumulatorMetricsSource(MetricsSourceName)
    val testMetric = spark.sparkContext.longAccumulator(MetricName)
    metricsSource.registerAccumulator(testMetric)
    metricsSource.start()

    When("accumulator is incremented during processing")
    val DatasetSize = 4
    val stream = MemoryStream[Int]
    stream.addData(1 to DatasetSize)

    stream.toDS().writeStream.foreach(new ForeachWriter[Int] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Int): Unit = testMetric.add(1)

      override def close(errorOrNull: Throwable): Unit = ()
    }).queryName("TestQuery").start().processAllAvailable()

    Then("accumulated metrics are outputted to configured metrics directory")
    // flush metrics
    metricsSource.stop()
    SparkEnv.get.metricsSystem.report()

    val customMetricFiles = new File(MetricsOutputDir).listFiles().filter(_.getName.startsWith(s"metrics-test.driver.$MetricsSourceName"))

    customMetricFiles should have size 1

    val counterFile = customMetricFiles.filter(_.getName.endsWith(s"$MetricName.csv")).head
    val maxCount = Source.fromFile(counterFile).getLines().toSeq.tail // first line is a header
      .map { line =>
      val count = line.split(",")(1) // second column is count, next are rates
      count.toLong
    }.max
    assert(maxCount == DatasetSize, s"Maximum measured count should be ${DatasetSize}. Collected metrics were: \n" + printContents(customMetricFiles))

    spark.stop()
  }

}
