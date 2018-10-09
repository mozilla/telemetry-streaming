/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.sinks.{CrashesBatchHttpSink, HttpSink}

object CrashesToOpenTsdb extends CrashPingStreamingBase {
  override val JobName: String = "crashes_to_opentsdb"

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

  override def getHttpSink(url: String, maxBatchSize: Int): CrashesBatchHttpSink = {
    val config = HttpSink.Config(successCodes = Set(204))
    CrashesBatchHttpSink(url, maxBatchSize = maxBatchSize, prefix = "[", sep = ",", suffix = "]", config = config)
  }

  // characters from:
  // http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
  override def formatCrashSignature(signature: String): String = {
    signature
      .replace(" | ", ".")
      .replace("::", "-")
      .replace(" ", "_")
      .replaceAll("[^a-zA-Z0-9_./-]", "/")
  }
}
