/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.Ping.messageToPing
import com.mozilla.telemetry.pings.main.Processes
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JNothing, JValue}


case class EventPing(application: Application,
                     meta: Meta,
                     payload: EventPingPayload)
  extends Ping with HasEnvironment with HasApplication with SendsToAmplitudeWithEnvironment {
  val processEventMap: Map[String, Seq[Event]] = Processes.names.map(
    p => p -> Ping.extractEvents(payload.events.getOrElse(p, JNothing), List(Nil))
  ).toMap

  val events: Seq[Event] = processEventMap.flatMap(_._2).toSeq

  def getClientId: Option[String] = meta.clientId

  def sessionStart: Long = payload.processStartTimestamp

  def getCreated: Option[Long] = meta.creationTimestamp.map(t => (t / 1e9).toLong)

  def getLocale: Option[String] = meta.`environment.settings`.map(_.locale).flatten

  def getMSStyleExperiments: Option[Map[String, String]] = {
    val experimentsArray = getExperiments
    experimentsArray.length match {
      case 0 => None
      case _ => Some(experimentsArray.flatMap {
        case (Some(exp), Some(branch)) => Some(exp -> branch)
        case _ => None
      }.toMap)
    }
  }

  def getNormandyEvents: Seq[Event] = {
    val dynamicProcessEvents = processEventMap.getOrElse("dynamic", Seq.empty[Event])
    dynamicProcessEvents.filter(_.category == "normandy")
  }
}

object EventPing {
  def apply(message: Message): EventPing = {
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
    ping.extract[EventPing]
  }
}

case class EventPingPayload(events: Map[String, JValue],
                            lostEventsCount: Int,
                            processStartTimestamp: Long,
                            reason: String,
                            sessionId: String,
                            subsessionId: String)
