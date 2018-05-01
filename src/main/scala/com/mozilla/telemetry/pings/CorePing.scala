/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.Ping.{SecondsPerHour, messageToPing}
import org.json4s.DefaultFormats

case class CorePing(arch: String,
                    displayVersion: Option[String],
                    durations: Option[Int],
                    experiments: Option[Array[String]],
                    meta: Meta,
                    os: String,
                    osversion: String,
                    seq: Int) extends Ping {

  override def getExperiments: Array[(Option[String], Option[String])] = {
    // Ignore Fennec experiments for now as these are different from desktop ones
    Array((None, None))
  }

  override def getVersion: Option[String] = Option(meta.appVersion)

  override def getDisplayVersion: Option[String] = displayVersion.orElse(Option(meta.appVersion))

  override def getOsName: Option[String] = Option(os)

  override def getOsVersion: Option[String] = Option(osversion)

  override def getArchitecture: Option[String] = Option(arch)

  def usageHours: Option[Float] = {
    this.durations match {
      case Some(d) => Option(d.toFloat / SecondsPerHour)
      case _ => None
    }
  }

  override protected def getRawBuildId: Option[String] = Option(meta.appBuildId)
}

object CorePing {
  def apply(message: Message): CorePing = {
    implicit val formats = DefaultFormats
    val ping = messageToPing(message)
    ping.extract[CorePing]
  }
}
