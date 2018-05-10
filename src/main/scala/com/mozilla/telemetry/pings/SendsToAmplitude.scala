package com.mozilla.telemetry.pings

import java.security.MessageDigest

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.streaming.EventsToAmplitude.{AmplitudeEvent, Config}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{asJsonNode, compact, render}
import org.json4s.{DefaultFormats, Extraction, JObject}


trait SendsToAmplitude {
  val clientId: String
  val events: Seq[Event]
  val meta: Meta

  def getHashedClientId: String = {
    val messageDigest = MessageDigest.getInstance("SHA-256")
    new String(messageDigest.digest(clientId.getBytes()))
  }

  def sessionStart: Long
  def getSessionId: String
  def getOs: String
  def getOsVersion: String

  def eventToAmplitudeEvent(config: Config, e: Event, es: AmplitudeEvent): JObject = {
    val hashedClientId = getHashedClientId
    val sessionId = getSessionId

    ("device_id" -> hashedClientId) ~
      ("session_id" -> sessionId) ~
      ("insert_id" -> (hashedClientId + sessionId.toString + e.getId)) ~
      ("event_type" -> getFullEventName(config.eventGroupName, es.name)) ~
      ("time" -> (e.timestamp + sessionStart)) ~
      ("event_properties" -> e.getProperties(es.amplitudeProperties)) ~
      ("app_version" -> meta.appVersion) ~
      ("os_name" -> getOs) ~
      ("os_version" -> getOsVersion) ~
      ("country" -> meta.geoCountry) ~
      ("city" -> meta.geoCity)
  }

  def getAmplitudeEvents(config: Config): String = {
    implicit val formats = DefaultFormats

    val factory = JsonSchemaFactory.byDefault
    val schemas = config.events.map(e => factory.getJsonSchema(asJsonNode(e.schema)))

    val eventsList = events.map{ e => e -> asJsonNode(Extraction.decompose(e)): (Event, JsonNode) }
      .map{ case(e, es) => // for each event, try each schema
        e -> schemas.map( ts => ts.validateUnchecked(es).isSuccess )
          .zip(config.events)
          .filter(_._1)
      }
      .filter{ case(e, es) => !es.isEmpty } // only keep those with a match
      .map{ case(e, es) => e -> es.head._2 } // take the first match (head._1 is the bool)
      .map{ case(e, es) => eventToAmplitudeEvent(config, e, es) }

    compact(render(eventsList))
  }

  def getFullEventName(groupName: String, eventName: String): String = groupName + " - " + eventName

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

object SendsToAmplitude {
  def apply(message: Message): SendsToAmplitude = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List()
    message.fieldsAsMap.get("docType") match {
      case Some("focus-event") => {
        val ping = messageToPing(message, jsonFieldNames, List("events" :: Nil))
        ping.extract[FocusEventPing]
      }
      case Some(x) => throw new IllegalArgumentException(s"Unexpected doctype $x")
      case _ => throw new IllegalArgumentException(s"No doctype found")
    }
  }
}
