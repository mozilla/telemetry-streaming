/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.FireTvEventPing.OptionToType
import org.json4s.JsonDSL._
import org.json4s._

case class FireTvEventPing(arch: Option[String],
                           clientId: String,
                           created: Long,
                           device: String,
                           events: Seq[Event],
                           locale: String,
                           os: String,
                           osversion: String,
                           seq: Integer,
                           settings: FireTvSettings,
                           v: String,
                           meta: Meta) extends SendsToAmplitude {

  override def getClientId: Option[String] = Some(clientId)

  def sessionStart: Long = created

  def getSessionId: Option[String] = Some((events.map(_.timestamp).max).toString)

  def getOsName: Option[String] = Some(os)

  def getOsVersion: Option[String] = Some(osversion)

  def getCreated: Option[Long] = Some(created)

  override def pingAmplitudeProperties: JObject = {
    ("device_id" -> getClientId) ~
    ("device_model" -> device) ~
    ("arch" -> arch) ~
    ("locale" -> locale) ~
    ("os_version" -> getOsVersion) ~
    ("user_properties" ->
      ("tracking_protection_enabled" -> settings.tracking_protection_enabled.asBool) ~
      ("total_home_tile_count" -> settings.total_home_tile_count) ~
      ("custom_home_tile_count" -> settings.custom_home_tile_count) ~
      ("remote_control_name" -> settings.remote_control_name) ~
      ("app_id" -> settings.app_id))
  }
}

case class FireTvSettings(tracking_protection_enabled: Option[String],
                          total_home_tile_count: Option[String],
                          custom_home_tile_count: Option[String],
                          remote_control_name: Option[String],
                          app_id: Option[String])

object FireTvEventPing {
  def apply(message: Message): MobileEventPing = {
    implicit val formats = DefaultFormats

    val ping = Ping.messageToPing(message, List(), eventLocations)
    ping.extract[MobileEventPing]
  }

  val eventLocations = List("events" :: Nil)

  implicit class OptionToType(val opt: Option[String]) extends AnyVal {
    def asBool: Option[Boolean] = opt.map(_ == "true")
  }
}
