/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

case class ForeachSink(f: (Long, DataFrame) => Unit) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    f(batchId, data)
  }
}

abstract class ForeachSinkProvider extends StreamSinkProvider {
  def f(batchId: Long, data: DataFrame): Unit

  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): ForeachSink = {
    ForeachSink(f)
  }
}
