/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import org.json4s.JsonDSL._
import org.json4s._

case class FocusEventPing(clientId: String,
                          created: Long,
                          events: Seq[Event],
                          v: String,
                          seq: Integer,
                          os: String,
                          osversion: String,
                          settings: FocusSettings,
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
      ("pref_privacy_block_ads" -> settings.blockAds) ~
      ("pref_locale" -> settings.pref_locale) ~
      ("pref_privacy_block_social" -> settings.blockSocial) ~
      ("pref_secure" -> settings.secure) ~
      ("pref_privacy_block_analytics" -> settings.blockAnalytics) ~
      ("pref_search_engine" -> settings.pref_search_engine) ~
      ("pref_privacy_block_other" -> settings.blockOther) ~
      ("pref_default_browser" -> settings.defaultBrowser) ~
      ("pref_performance_block_webfonts" -> settings.blockWebfonts) ~
      ("pref_performance_block_images" -> settings.blockImages) ~
      ("pref_autocomplete_installed" -> settings.autocompleteInstalled) ~
      ("pref_autocomplete_custom" -> settings.autocompleteCustom) ~
      ("pref_key_tips" -> settings.prefKeyTips))
  }
}

case class FocusSettings(pref_privacy_block_ads: Option[String],
                         pref_locale: Option[String],
                         pref_privacy_block_social: Option[String],
                         pref_secure: Option[String],
                         pref_privacy_block_analytics: Option[String],
                         pref_search_engine: Option[String],
                         pref_privacy_block_other: Option[String],
                         pref_default_browser: Option[String],
                         pref_performance_block_webfonts: Option[String],
                         pref_performance_block_images: Option[String],
                         pref_autocomplete_installed: Option[String],
                         pref_autocomplete_custom: Option[String],
                         pref_key_tips: Option[String]) {

  def blockAds: Option[Boolean] = asBool(pref_privacy_block_ads)

  def blockSocial: Option[Boolean] = asBool(pref_privacy_block_social)

  def secure: Option[Boolean] = asBool(pref_secure)

  def blockAnalytics: Option[Boolean] = asBool(pref_privacy_block_analytics)

  def asBool(param: Option[String]): Option[Boolean] = param.map(_ == "true")

  def blockOther: Option[Boolean] = asBool(pref_privacy_block_other)

  def defaultBrowser: Option[Boolean] = asBool(pref_default_browser)

  def blockWebfonts: Option[Boolean] = asBool(pref_performance_block_webfonts)

  def blockImages: Option[Boolean] = asBool(pref_performance_block_images)

  def autocompleteInstalled: Option[Boolean] = asBool(pref_autocomplete_installed)

  def autocompleteCustom: Option[Boolean] = asBool(pref_autocomplete_custom)

  def prefKeyTips: Option[Boolean] = asBool(pref_key_tips)
}

object FocusEventPing {
  def apply(message: Message): FocusEventPing = {
    implicit val formats = DefaultFormats

    val ping = Ping.messageToPing(message, List(), eventLocations)
    ping.extract[FocusEventPing]
  }

  val eventLocations = List("events" :: Nil)
}
