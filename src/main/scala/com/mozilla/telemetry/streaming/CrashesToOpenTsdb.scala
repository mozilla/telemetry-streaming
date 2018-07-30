/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.sinks.BatchHttpSink

object CrashesToOpenTsdb extends CrashPingStreamingBase {

  override val sparkAppName: String = this.getClass.getSimpleName

  override def buildOutputString(measurementName: String, timestamp: Long,
                                 buildId: String, tags: Map[String, String]): String = {
    val formattedTags = tags
      .map { case (k, v) => s""""$k": "$v"""" }
      .mkString("{", ",\n", "}")

      s"""
         |{
         |  "metric": "$measurementName",
         |  "timestamp": ${timestamp / 1000000},
         |  "value": "$buildId",
         |  "tags": $formattedTags
         |}
        """.stripMargin
  }

  override def getHttpSink(url: String, maxBatchSize: Int): BatchHttpSink = {
    new BatchHttpSink(url, maxBatchSize, prefix = "[", sep = ",", suffix = "]")
  }
}
