// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.pings

import org.json4s._

import java.security.MessageDigest
import scala.util.hashing.MurmurHash3

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.streaming.EventsToAmplitude.Config

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}
import com.github.fge.jsonschema.core.exceptions.ProcessingException

import scala.util.{Try, Success, Failure}

case class FocusEventPing(
    clientId: String,
    created: Long,
    events: Seq[Event],
    v: String,
    seq: Integer,
    os: String,
    osversion: String,
    meta: Meta) {

  def getEvents(config: Config): String = {
    implicit val formats = DefaultFormats

    val factory = JsonSchemaFactory.byDefault
    val schemas = config.events.map(e => factory.getJsonSchema(asJsonNode(e.schema)))

    val clientid = getHashedClientId
    val sessionId = getSessionId

    val eventsList = events.map{ e => e -> asJsonNode(Extraction.decompose(e)): (Event, JsonNode) }
        .map{ case(e, es) => // for each event, try each schema
          e -> schemas.map( ts => ts.validateUnchecked(es).isSuccess )
            .zip(config.events)
            .filter(_._1)
      }
      .filter{ case(e, es) => !es.isEmpty } // only keep those with a match
      .map{ case(e, es) => e -> es.head._2 } // take the first match (head._1 is the bool)
      .map{ case(e, es) =>
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
        ("city" -> meta.geoCity)
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

    if(!keepClient){
      // for maybe a slight perf increase
      return false // scalastyle:ignore
    }

    val currentProps = this.getClass
      .getDeclaredFields.map{ e =>
        e.setAccessible(true)
        e.getName -> e.get(this).toString
      }.toMap

    config.filters.map{ case(prop, allowedVals) =>
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
