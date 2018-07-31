/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.monitoring


/* Conforms to the DogStatsD datagram format as documented here:
 * https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/ */
sealed trait DogStatsDMetric {
  val metricName: String
  protected val metricStringValue: String
  protected val metricType: String
  val kvTags: Option[Map[String, String]]
  val bareTags: Option[Seq[String]]

  def format(sampleRate: Option[Double] = None): String = {
    Array(Some(metric), Some(metricType), sampleRateString(sampleRate), tags)
      .flatten
      .mkString("|")
  }

  /* Per the DogStatsD datagram documentation, the character set that's normalized here is reserved and cannot be
   * used in metric names or tags */
  private def normalize(input: String): String = {
    input.replaceAll("[:|@]", "_")
  }

  private def sampleRateString(sampleRate: Option[Double]): Option[String] = sampleRate.map("@" + _.toString)

  lazy private val metric: String = s"${normalize(metricName)}:$metricStringValue"
  lazy private val tags: Option[String] = {
    val kv = kvTags.map(_.map {case (k, v) => s"${normalize(k)}:${normalize(v)}"}.mkString(","))
    val bare = bareTags.map(_.map(normalize).mkString(","))

    Seq(kv, bare).flatten.mkString(",") match {
      case "" => None
      case x => Some("#" + x)
    }
  }
}

case class DogStatsDCounter(metricName: String, metricValue: Int = 1, kvTags: Option[Map[String, String]] = None,
                            bareTags: Option[Seq[String]] = None) extends DogStatsDMetric {
  protected val metricStringValue = metricValue.toString
  protected val metricType = "c"
}
