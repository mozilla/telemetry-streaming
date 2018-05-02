/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import org.json4s.DefaultFormats

case class CorePing(arch: String,
                    durations: Option[Int],
                    experiments: Option[Array[String]],
                    meta: Meta,
                    os: String,
                    osversion: String) extends Ping {

  override def getExperiments: Array[(Option[String], Option[String])] = {
    // add a null experiment_id and experiment_branch for each ping
    // add a null experiment_branch for each experiment, as experiments on Fennec do not report branches
    (experiments.map(_.map(e => (Option(e), Option.empty[String])))
      .getOrElse(Array[(Option[String], Option[String])]()) :+ (None, None)
      ).distinct
  }

  override def getVersion: Option[String] = Option(meta.appVersion)

  override def getDisplayVersion: Option[String] = Option(meta.appVersion)

  override def getOsName: Option[String] = Option(os)

  override def getOsVersion: Option[String] = Option(osversion)

  override def getArchitecture: Option[String] = Option(arch)

  def usageHours: Option[Float] = {
    val seconds_per_hour = 3600
    this.durations match {
      case Some(d) => Option(d.toFloat / seconds_per_hour)
      case _ => None
    }
  }
}

object CorePing {
  def apply(message: Message): CorePing = {
    implicit val formats = DefaultFormats
    val ping = messageToPing(message)
    ping.extract[CorePing]
  }
}
