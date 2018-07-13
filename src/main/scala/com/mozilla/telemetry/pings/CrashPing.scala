/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.Ping.messageToPing
import org.json4s.{DefaultFormats, JValue}

case class CrashPing(application: Application,
                     clientId: Option[String],
                     payload: CrashPayload,
                     // Environment is omitted it's partially available under meta
                     meta: Meta
                    ) extends Ping with HasEnvironment with HasApplication {

  override def getOsName: Option[String] = {
    val reportedOsName = meta.`environment.system`.map(_.os.name)
    if (meta.appName == "Fennec" && reportedOsName.contains("Linux")) {
      //Fennec incorrectly reports its OS in crash pings
      Some("Android")
    } else {
      reportedOsName
    }
  }

  def isMainCrash: Boolean = {
    payload.processType.getOrElse("main") == "main"
  }

  def isContentCrash: Boolean = {
    payload.processType.contains("content")
  }

  def isContentShutdownCrash: Boolean = {
    payload.metadata.ipc_channel_error.contains("ShutDownKill")
  }

  def isStartupCrash: Boolean = {
    payload.metadata.StartupCrash.getOrElse("0") == "1"
  }
}

object CrashPing {
  def apply(message: Message): CrashPing = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List(
      "environment.build",
      "environment.settings",
      "environment.system",
      "environment.profile",
      "environment.addons",
      "environment.experiments"
    )
    val ping = messageToPing(message, jsonFieldNames)
    ping.extract[CrashPing]
  }
}

case class CrashPayload(crashDate: String,
                        processType: Option[String],
                        hasCrashEnvironment: Option[Boolean],
                        metadata: CrashMetadata,
                        version: Option[Int],
                        stackTraces: JValue)

case class CrashMetadata(StartupCrash: Option[String],
                         ipc_channel_error: Option[String],
                         MozCrashReason: Option[String],
                         OOMAllocationSize: Option[String],
                         ShutdownProgress: Option[String],
                         AsyncShutdownTimeout: JValue)
