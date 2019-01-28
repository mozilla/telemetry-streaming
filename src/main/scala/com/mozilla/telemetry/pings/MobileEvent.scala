/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.MobileEventPing.OptionToBoolean
import org.json4s.JsonDSL._
import org.json4s._

case class MobileEventPing(arch: String,
                           clientId: String,
                           created: Long,
                           device: String,
                           events: Seq[Event],
                           locale: String,
                           os: String,
                           osversion: String,
                           seq: Integer,
                           settings: MobileSettings,
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
    ("user_properties" ->
      ("pref_defaultSearchEngine" -> settings.defaultSearchEngine) ~
      ("pref_prefKeyAutomaticSliderValue" -> settings.prefKeyAutomaticSliderValue) ~
      ("pref_prefKeyAutomaticSwitchOnOff" -> settings.prefKeyAutomaticSwitchOnOff) ~
      ("pref_prefKeyThemeName" -> settings.prefKeyThemeName) ~
      ("pref_profile.ASBookmarkHighlightsVisible" -> settings.`profile.ASBookmarkHighlightsVisible`.asBool) ~
      ("pref_profile.ASPocketStoriesVisible" -> settings.`profile.ASPocketStoriesVisible`.asBool) ~
      ("pref_profile.ASRecentHighlightsVisible" -> settings.`profile.ASRecentHighlightsVisible`.asBool) ~
      ("pref_profile.blockPopups" -> settings.`profile.blockPopups`.asBool) ~
      ("pref_profile.prefkey.trackingprotection.enabled" -> settings.`profile.prefkey.trackingprotection.enabled`) ~
      ("pref_profile.prefkey.trackingprotection.normalbrowsing" -> settings.`profile.prefkey.trackingprotection.normalbrowsing`) ~
      ("pref_profile.prefkey.trackingprotection.privatebrowsing" -> settings.`profile.prefkey.trackingprotection.privatebrowsing`) ~
      ("pref_profile.prefkey.trackingprotection.strength" -> settings.`profile.prefkey.trackingprotection.strength`) ~
      ("pref_profile.saveLogins" -> settings.`profile.saveLogins`.asBool) ~
      ("pref_profile.settings.closePrivateTabs" -> settings.`profile.settings.closePrivateTabs`.asBool) ~
      ("pref_profile.show-translation" -> settings.`profile.show-translation`.asBool) ~
      ("pref_profile.showClipboardBar" -> settings.`profile.showClipboardBar`.asBool) ~
      ("pref_windowHeight" -> settings.windowHeight) ~
      ("pref_windowWidth" -> settings.windowWidth))
  }
}

case class MobileSettings(defaultSearchEngine: Option[String],
                          prefKeyAutomaticSliderValue: Option[String],
                          prefKeyAutomaticSwitchOnOff: Option[String],
                          prefKeyThemeName: Option[String],
                          `profile.ASBookmarkHighlightsVisible`: Option[String],
                          `profile.ASPocketStoriesVisible`: Option[String],
                          `profile.ASRecentHighlightsVisible`: Option[String],
                          `profile.blockPopups`: Option[String],
                          `profile.prefkey.trackingprotection.enabled`: Option[String],
                          `profile.prefkey.trackingprotection.normalbrowsing`: Option[String],
                          `profile.prefkey.trackingprotection.privatebrowsing`: Option[String],
                          `profile.prefkey.trackingprotection.strength`: Option[String],
                          `profile.saveLogins`: Option[String],
                          `profile.settings.closePrivateTabs`: Option[String],
                          `profile.show-translation`: Option[String],
                          `profile.showClipboardBar`: Option[String],
                          windowHeight: Option[String],
                          windowWidth: Option[String])


object MobileEventPing {
  def apply(message: Message): MobileEventPing = {
    implicit val formats = DefaultFormats

    val ping = Ping.messageToPing(message, List(), eventLocations)
    ping.extract[MobileEventPing]
  }

  val eventLocations = List("events" :: Nil)

  implicit class OptionToBoolean(val opt: Option[String]) extends AnyVal {
    def asBool: Option[Boolean] = opt.map(_ == "true")
  }

}
