/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import com.mozilla.telemetry.heka.Message
import org.json4s._
import org.json4s.jackson.JsonMethods._

package object pings {

  def messageToPing(message: Message, jsonFieldNames: List[String] = List(), eventPaths: List[List[String]] = List()): JValue = {
    implicit val formats = DefaultFormats
    val fields = message.fieldsAsMap ++ Map("Timestamp" -> message.timestamp)
    val jsonObj = Extraction.decompose(fields)
    // Transform json fields into JValues
    val meta = jsonObj transformField {
      case JField(key, JString(s)) if jsonFieldNames contains key => (key, parse(s))
    }
    val submission = if (message.payload.isDefined) message.payload else fields.get("submission")
    val json = submission match {
      case Some(value: String) => parse(value)
      case _ => JObject()
    }

    replaceEvents(json, eventPaths) ++ JObject(List(JField("meta", meta)))
  }

  /**
    * Events come in as arrays, but to extract them to Event case classes
    * we need them as key-value json blobs. This takes in a list of event
    * paths (since some pings may hold events in multiple places), and
    * converts each array to a json event that can be extracted.
    */
  def replaceEvents(json: JValue, eventPaths: List[List[String]]): JValue = {
    eventPaths.foldLeft(json) {
      case (currentJson, path) =>
        val currentEvents = path.foldLeft(currentJson)(_ \ _)
        val newEvents = currentEvents match {
          case JArray(x) => JArray(x.map { e =>
            e match {
              case JArray(event) =>
                JObject(
                  JField("timestamp", event(0)) ::
                    JField("category", event(1)) ::
                    JField("method", event(2)) ::
                    JField("object", event(3)) ::
                    JField("value", event.lift(4).getOrElse(JNull)) ::
                    JField("extra", event.lift(5).getOrElse(JNull)) ::
                    Nil)

              case o => throw new java.io.InvalidObjectException(
                s"Expected JArray for event at ${path.mkString("\\")}, got ${o.getClass}")
            }
          })

          case o => throw new java.io.InvalidObjectException(
            s"Expected JArray for events container at ${path.mkString("\\")}, got ${o.getClass}")
        }

        currentJson.replace(path, newEvents)
    }
  }
}
