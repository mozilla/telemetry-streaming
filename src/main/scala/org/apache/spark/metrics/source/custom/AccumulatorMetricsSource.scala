/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package org.apache.spark.metrics.source.custom
// This class is defined under `org.apache.spark` package, because it extends
// package-private `org.apache.spark.metrics.source.Source`

import java.util.concurrent.{Executors, TimeUnit}
import java.{lang => jl}

import com.codahale.metrics.MetricRegistry
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.apache.log4j.LogManager
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.{AccumulatorV2, ShutdownHookManager}

import scala.collection.mutable

/**
  * Accumulator-based metric source.
  *
  * This source uses Spark accumulators for collecting metrics. After registering an accumulator, it will be
  * periodically polled for its value which will be reported to Spark metrics system using [[com.codahale.metrics.Meter]].
  * After accumulators are registered it is necessary to start the source with [[AccumulatorMetricsSource#start()]],
  * which will start internal polling thread and register shutdown hook for flushing metrics.
  *
  * Usage:
  * {{{
  * val metricsSource = new AccumulatorMetricsSource("prefix")
  * val metric = spark.sparkContext.longAccumulator("metric-name")
  * metricsSource.registerAccumulator(metric)
  * metricsSource.start()
  * }}}
  *
  * @param prefix for metrics collected by this source
  */
class AccumulatorMetricsSource(prefix: String) extends Source {
  override val sourceName: String = prefix
  override val metricRegistry: MetricRegistry = new MetricRegistry

  private val log = LogManager.getLogger(this.getClass.getName)

  private val accumulators = mutable.Map[String, AccumulatorV2[jl.Long, jl.Long]]()

  private val executor = Executors.newSingleThreadScheduledExecutor(
    new BasicThreadFactory.Builder()
      .namingPattern("accumulator-metrics-source-thread-%d")
      .daemon(true)
      .priority(Thread.NORM_PRIORITY)
      .build())

  def registerAccumulator(acc: AccumulatorV2[jl.Long, jl.Long]): Unit = {
    val name = acc.name.getOrElse("UNNAMED_ACCUMULATOR")
    require(!accumulators.keySet.contains(name), "Accumulator already registered")
    accumulators.put(name, acc)
    metricRegistry.meter(name) // register metric beforehand to make sure no errors are thrown later during collection
  }

  def start(pollingPeriod: Long = 10, unit: TimeUnit = TimeUnit.SECONDS): Unit = {
    executor.scheduleAtFixedRate(new Runnable() {
      override def run(): Unit = {
        try {
          report()
        } catch {
          case ex: Exception =>
            log.error("Exception thrown from AccumulatorMetricsSource#report. Exception was suppressed.", ex)
        }
      }
    }, pollingPeriod, pollingPeriod, unit)

    SparkEnv.get.metricsSystem.registerSource(this)
    ShutdownHookManager.addShutdownHook(() => this.stop())
  }

  private[custom] def stop(): Unit = {
    executor.shutdown()
    report()
  }

  private def report(): Unit = {
    accumulators.foreach { case (name, acc) =>
      val value = acc.value
      val meter = metricRegistry.meter(name)
      meter.mark(getCountOverflowSafe(value, meter.getCount))
    }
  }

  private def getCountOverflowSafe(current: jl.Long, previous: jl.Long): jl.Long = {
    val unsafeCount = current - previous
    val safeCount = if (unsafeCount < 0) jl.Long.MAX_VALUE - previous else unsafeCount
    safeCount
  }
}
