/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import java.security.MessageDigest

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.streaming.EventsToAmplitude.Config
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

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

  def asBool(param: Option[String]): Option[Boolean] = param.map(_ == "true")

  def secure: Option[Boolean] = asBool(pref_secure)

  def blockAnalytics: Option[Boolean] = asBool(pref_privacy_block_analytics)

  def blockOther: Option[Boolean] = asBool(pref_privacy_block_other)

  def defaultBrowser: Option[Boolean] = asBool(pref_default_browser)

  def blockWebfonts: Option[Boolean] = asBool(pref_performance_block_webfonts)

  def blockImages: Option[Boolean] = asBool(pref_performance_block_images)

  def autocompleteInstalled: Option[Boolean] = asBool(pref_autocomplete_installed)

  def autocompleteCustom: Option[Boolean] = asBool(pref_autocomplete_custom)
}


case class FocusEventPing(clientId: String,
                          created: Long,
                          events: Seq[Event],
                          v: String,
                          seq: Integer,
                          os: String,
                          osversion: String,
                          settings: FocusSettings,
                          meta: Meta) {

  def getEvents(config: Config): String = {
    implicit val formats = DefaultFormats

    val factory = JsonSchemaFactory.byDefault
    val schemas = config.events.map(e => factory.getJsonSchema(asJsonNode(e.schema)))

    val clientid = getHashedClientId
    val sessionId = getSessionId

    val eventsList = events.map { e => e -> asJsonNode(Extraction.decompose(e)): (Event, JsonNode) }
      .map { case (e, es) => // for each event, try each schema
        e -> schemas.map(ts => ts.validateUnchecked(es).isSuccess)
          .zip(config.events)
          .filter(_._1)
      }
      .filter { case (e, es) => !es.isEmpty } // only keep those with a match
      .map { case (e, es) => e -> es.head._2 } // take the first match (head._1 is the bool)
      .map { case (e, es) =>
      ("device_id" -> clientId) ~
        ("session_id" -> sessionId) ~
        ("insert_id" -> (clientId + sessionId.toString + e.getId)) ~
        ("event_type" -> getFullEventName(config.eventGroupName, es.name)) ~
        ("time" -> (e.timestamp + created)) ~
        ("event_properties" -> e.getProperties(es.amplitudeProperties)) ~
        ("app_version" -> meta.appVersion) ~
        ("os_name" -> os) ~
        ("os_version" -> osversion) ~
        ("country" -> meta.geoCountry) ~
        ("city" -> meta.geoCity) ~
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

    compact(render(eventsList))
  }

  def getFullEventName(groupName: String, eventName: String): String = groupName + " - " + eventName

  def getSessionId: Long = events.map(_.timestamp).max

  def getHashedClientId: String = {
    val messageDigest = MessageDigest.getInstance("SHA-256")
    new String(messageDigest.digest(clientId.getBytes()))
  }

  def includePing(sample: Double, config: Config): Boolean = {
    val keepClient = meta.sampleId.getOrElse(sample * 100) < (sample * 100)

    if (!keepClient) {
      // for maybe a slight perf increase
      return false // scalastyle:ignore
    }

    val currentProps = this.getClass
      .getDeclaredFields.map { e =>
      e.setAccessible(true)
      e.getName -> e.get(this).toString
    }.toMap

    config.filters.map { case (prop, allowedVals) =>
      allowedVals.contains(currentProps.getOrElse(prop, allowedVals.head))
    }.foldLeft(true)(_ & _)
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
