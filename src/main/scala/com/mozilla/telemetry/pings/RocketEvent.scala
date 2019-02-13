/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import org.json4s.JsonDSL._
import org.json4s._

case class RocketEventPing(clientId: String,
                          created: Long,
                          events: Seq[Event],
                          v: String,
                          seq: Integer,
                          os: String,
                          osversion: String,
                          settings: RocketSettings,
                          meta: Meta) extends SendsToAmplitude {

  override def getClientId: Option[String] = Some(clientId)

  def sessionStart: Long = created

  def getSessionId: Option[String] = Some((events.map(_.timestamp).max).toString)

  def getOsName: Option[String] = Some(os)

  def getOsVersion: Option[String] = Some(osversion)

  def getCreated: Option[Long] = Some(created)

  override def pingAmplitudeProperties: JObject = {
    ("device_id" -> getClientId) ~
    ("user_properties" ->
      ("pref_search_engine" -> settings.pref_search_engine) ~
      ("pref_privacy_turbo_mode" -> settings.turboMode) ~
      ("pref_performance_block_images" -> settings.blockImage) ~
      ("pref_default_browser" -> settings.defaultBrowser) ~
      ("pref_save_downloads_to" -> settings.pref_save_downloads_to) ~
      ("pref_webview_version" -> settings.pref_webview_version) ~
      ("pref_locale" -> settings.pref_locale))
  }
}

case class RocketSettings(pref_search_engine: Option[String],
                         pref_privacy_turbo_mode: Option[String],
                         pref_performance_block_images: Option[String],
                         pref_default_browser: Option[String],
                         pref_save_downloads_to: Option[String],
                         pref_webview_version: Option[String],
                         pref_locale: Option[String]) {

  def asBool(param: Option[String]): Option[Boolean] = param.map(_ == "true")

  def turboMode: Option[Boolean] = asBool(pref_privacy_turbo_mode)

  def blockImage: Option[Boolean] = asBool(pref_performance_block_images)

  def defaultBrowser: Option[Boolean] = asBool(pref_default_browser)

}

object RocketEventPing {
  def apply(message: Message): RocketEventPing = {
    implicit val formats = DefaultFormats

    val ping = Ping.messageToPing(message, List(), eventLocations)
    ping.extract[RocketEventPing]
  }

  val eventLocations = List("events" :: Nil)
}
