// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.pings

import org.json4s._

import java.security.MessageDigest
import scala.util.hashing.MurmurHash3

import com.mozilla.telemetry.heka.Message

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class FocusEventPing(
    clientId: String,
    created: Long,
    events: Seq[Event],
    v: String,
    seq: Integer,
    os: String,
    osversion: String,
    meta: Meta) {

  def getEvents: String = {
    val eventsList = events.map{ e =>
      ("device_id" -> getHashedClientId) ~
      ("event_type" -> e.getType) ~
      ("time" -> (e.timestamp + created)) ~
      ("event_properties" ->
        e.extra.getOrElse(Map.empty).foldLeft(JObject())(_ ~ _) ~
        ("object" -> e.`object`) ~
        ("value" -> e.value)) ~
      ("app_version" -> meta.appVersion) ~
      ("os_name" -> os) ~
      ("os_version" -> osversion) ~
      ("country" -> meta.geoCountry) ~
      ("city" -> meta.geoCity)
    }

    compact(render(eventsList))
  }

  def getHashedClientId: String = {
    val messageDigest = MessageDigest.getInstance("SHA-256")
    new String(messageDigest.digest(clientId.getBytes()))
  }

  def includeClient(sample: Double): Boolean = {
    meta.sampleId.getOrElse(sample * 100) < (sample * 100)
  }
}

object FocusEventPing {
  def apply(message: Message): FocusEventPing = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List()
    val ping = messageToPing(message, jsonFieldNames, List("events" :: Nil))
    ping.extract[FocusEventPing]
  }
}
