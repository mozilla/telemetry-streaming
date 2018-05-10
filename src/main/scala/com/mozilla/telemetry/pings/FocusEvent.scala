/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.Ping.messageToPing
import com.mozilla.telemetry.streaming.EventsToAmplitude.{AmplitudeEvent, Config}
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
  def sessionStart: Long = created

  def getSessionId: String = (events.map(_.timestamp).max).toString

  def getOs: String = os

  def getOsVersion: String = osversion

  override def eventToAmplitudeEvent(config: Config, e: Event, es: AmplitudeEvent): JObject = {
    super.eventToAmplitudeEvent(config, e, es) ~
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
          ("pref_autocomplete_custom" -> settings.autocompleteCustom))
  }
}


case class Event(timestamp: Int,
                 category: String,
                 method: String,
                 `object`: String,
                 value: Option[String],
                 extra: Option[Map[String, String]]) {

  def getProperties(properties: Option[Map[String, String]]): JObject = {
    properties.getOrElse(Map.empty).map { case (k, v) =>
      k -> (v match {
        case "timestamp" => timestamp.toString
        case "category" => category
        case "method" => method
        case "object" => `object`
        case "value" => value.getOrElse("") // TODO - log if empty
        case e if e.startsWith("extra") => extra.getOrElse(Map.empty).getOrElse(e.stripPrefix("extra."), "")
        case _ => ""
      })
    }.foldLeft(JObject())(_ ~ _)
  }

  def getId: String = timestamp.toString + category + method + `object`
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
                         pref_autocomplete_custom: Option[String]) {

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
}
