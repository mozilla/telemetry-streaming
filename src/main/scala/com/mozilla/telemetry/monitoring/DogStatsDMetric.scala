/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.monitoring


/* Conforms to the DogStatsD datagram format as documented here:
 * https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/ */
case class DogStatsDMetric(metricName: String, metricValue: String, metricType: String, kvTags: Option[Map[String, String]] = None,
                      bareTags: Option[Seq[String]] = None) {
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

  lazy private val metric: String = s"${normalize(metricName)}:$metricValue"
  lazy private val tags: Option[String] = {
    val kv = kvTags.map(_.map {case (k, v) => s"${normalize(k)}:${normalize(v)}"}.mkString(","))
    val bare = bareTags.map(_.map(normalize).mkString(","))

    Seq(kv, bare).flatten.mkString(",") match {
      case "" => None
      case x => Some("#" + x)
    }
  }
}

object DogStatsDMetric {
  // While it would have been cleaner to structure these as a trait w each metric type as a concrete implementation of
  // such, the dataset type parameter must be a subtype of Product, so we're unable to go that route if we want to have
  // multiple types of metrics in a single dataframe
  def makeCounter(metricName: String, metricValue: Int = 1, kvTags: Option[Map[String, String]] = None,
                              bareTags: Option[Seq[String]] = None): DogStatsDMetric = {
    DogStatsDMetric(metricName, metricValue.toString, "c", kvTags, bareTags)
  }

  def makeTimer(metricName: String, metricValue: Int, kvTags: Option[Map[String, String]] = None,
                            bareTags: Option[Seq[String]] = None): DogStatsDMetric = {
    DogStatsDMetric(metricName, metricValue.toString, "ms", kvTags, bareTags)
  }
}
