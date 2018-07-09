/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.Meta._
import com.mozilla.telemetry.streaming.EventsToAmplitude.{AmplitudeEvent, Config}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{parse, _}
import org.json4s.{DefaultFormats, Extraction, JArray, JField, JNull, JObject, JValue, _}

import scala.util.Try

trait Ping {
  val meta: Meta

  def getExperiments: Array[(Option[String], Option[String])]

  def getVersion: Option[String]

  def getDisplayVersion: Option[String]

  def getOsName: Option[String]

  def getOsVersion: Option[String]

  def getArchitecture: Option[String]

  // TODO: check if we can use appBuildId for all pings
  def getNormalizedBuildId: Option[String] = {
    getRawBuildId match {
      case Some(buildId: String) =>
        val buildIdDay = buildId.slice(0, 8)
        val buildDateTime = LocalDate.parse(buildIdDay, DateFormatter)
        val submissionDateTime = LocalDate.parse(meta.submissionDate, DateFormatter)

        ChronoUnit.MONTHS.between(buildDateTime, submissionDateTime) match {
          case m if (0 <= m) && (m <= 6) => Some(buildId)
          case _ => None
        }
      case _ => None
    }
  }

  protected def getRawBuildId: Option[String]
}

object Ping {
  private[pings] val SecondsPerHour = 3600

  private[pings] def messageToPing(message: Message, jsonFieldNames: List[String] = List(), eventPaths: List[List[String]] = List()): JValue = {
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
  private[this] def replaceEvents(json: JValue, eventPaths: List[List[String]]): JValue = {
    reshapeEvents(json, eventPaths)
      .foldLeft(json) {
        case (currentJson, (path, newEvents)) =>
          currentJson.replace(path, newEvents)
      }
  }

  private[pings] def extractEvents(json: JValue, eventPaths: List[List[String]]): Seq[Event] = {
    implicit val formats = DefaultFormats
    reshapeEvents(json, eventPaths)
      .flatMap {
        case (_, events) => events.extract[Seq[Event]]
      }
  }

  private[this] def reshapeEvents(json: JValue, eventPaths: List[List[String]]): List[(List[String], JValue)] = {
    eventPaths.map { path: List[String] =>
      val eventValue = path.foldLeft(json)(_ \ _)
      val newEvents = eventValue match {
        case JArray(x) => JArray(x.map {
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
        })
        case JNothing => JNothing

        case o => throw new java.io.InvalidObjectException(
          s"Expected JArray for events container at ${path.mkString("\\")}, got ${o.getClass}")
      }

      (path, newEvents)
    }
  }
}

case class Meta(Host: Option[String],
                Hostname: Option[String],
                Size: Option[Double],
                Timestamp: Long,
                Type: Option[String],
                appBuildId: String,
                appName: String,
                appUpdateChannel: Option[String],
                appVendor: Option[String],
                appVersion: String,
                clientId: Option[String],
                creationTimestamp: Option[Float],
                docType: Option[String],
                documentId: Option[String],
                geoCity: Option[String],
                geoCountry: String,
                normalizedChannel: String,
                os: Option[String],
                sampleId: Option[Double],
                sourceName: Option[String],
                sourceVersion: Option[Int],
                submissionDate: String,
                telemetryEnabled: Option[Boolean],
                // Common fields preparsed by hindsight
                `environment.build`: Option[EnvironmentBuild],
                `environment.settings`: Option[Settings],
                `environment.system`: Option[System],
                `environment.addons`: Option[Addons],
                `environment.experiments`: Option[Map[String, NewStyleExperiment]],
                // Main ping fields preparsed by hindsight
                `payload.simpleMeasurements`: JValue,
                `payload.keyedHistograms`: JValue,
                `payload.histograms`: JValue,
                `payload.info`: JValue) {
  // Some of the fields are not present in all ping types (e.g. `environment.*`, `payload.*`
  // This class contains only extractor methods for common fields, sent with all pings

  /**
    * Returns a java Timestamp obj with millisecond resolution.
    * The source Timestamp field has nanoseconds resolution
    */
  def normalizedTimestamp(): Timestamp = {
    new Timestamp(this.Timestamp / 1000000)
  }
}

object Meta {
  val DateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
}

case class EnvironmentBuild(version: Option[String],
                            buildId: Option[String],
                            architecture: Option[String])

case class System(os: SystemOs)

case class SystemOs(name: String, version: String) {
  val normalizedVersion: String = OS(Option(name), Option(version)).normalizedVersion
}

case class OS(name: Option[String], version: Option[String]) {
  val versionRegex = "(\\d+(\\.\\d+)?(\\.\\d+)?)?.*".r
  val normalizedVersion: String = {
    version match {
      case Some(v) =>
        val versionRegex(normalized, _, _) = v
        normalized
      case None =>
        null
    }
  }
}

case class Addons(activeAddons: Option[Map[String, ActiveAddon]],
                  activeExperiment: Option[OldStyleExperiment],
                  theme: Option[Theme])

case class ActiveAddon(isSystem: Option[Boolean], isWebExtension: Option[Boolean])

case class OldStyleExperiment(id: String, branch: String)

case class NewStyleExperiment(branch: String)

case class Theme(id: String) {
  def isOld: Boolean = !Theme.newThemes.contains(this.id)
}

object Theme {
  val newThemes = List(
    "{972ce4c6-7e08-4474-a285-3208198ce6fd}",
    "firefox-compact-light@mozilla.org",
    "firefox-compact-dark@mozilla.org"
  )
}

case class Settings(blocklistEnabled: Option[Boolean],
                    isDefaultBrowser: Option[Boolean],
                    locale: Option[String],
                    telemetryEnabled: Option[Boolean])

trait HasEnvironment {
  this: Ping =>

  override def getExperiments: Array[(Option[String], Option[String])] = {
    val oldStyleExperiment = for {
      addons <- meta.`environment.addons`
      experiment <- addons.activeExperiment
    } yield (Some(experiment.id), Some(experiment.branch))

    val newStyleExperiments = for {
      experiments <- meta.`environment.experiments`.toSeq
      (experimentId, experiment) <- experiments
    } yield (Some(experimentId), Some(experiment.branch))

    // add a null experiment_id and experiment_branch for each ping
    (newStyleExperiments ++ oldStyleExperiment :+ (None, None)).toSet.toArray
  }

  override def getVersion: Option[String] = meta.`environment.build`.flatMap(_.version)

  override def getOsName: Option[String] = meta.`environment.system`.map(_.os.name)

  override def getOsVersion: Option[String] = meta.`environment.system`.map(_.os.normalizedVersion)

  override def getArchitecture: Option[String] = meta.`environment.build`.flatMap(_.architecture)

  override protected def getRawBuildId: Option[String] = meta.`environment.build`.flatMap(_.buildId)
}

trait HasApplication {
  this: Ping =>

  val application: Application

  override def getDisplayVersion: Option[String] = application.displayVersion
}

case class Application(architecture: String,
                       buildId: String,
                       channel: String,
                       name: String,
                       platformVersion: String,
                       vendor: String,
                       version: String,
                       displayVersion: Option[String],
                       xpcomAbi: String)


trait SendsToAmplitude {
  val events: Seq[Event]
  val meta: Meta

  def getClientId: Option[String]
  def sessionStart: Long
  def getOsName: Option[String]
  def getOsVersion: Option[String]
  def getCreated: Option[Long]
  private def filterProperties = Map("os" -> getOsName.getOrElse(""), "created" -> getCreated.getOrElse(0).toString)

  def pingAmplitudeProperties: JObject = JObject()

  def eventToAmplitudeEvent(eventGroup: String, e: Event, es: AmplitudeEvent): JObject = {
    val sessionIdOffset = Try(es.sessionIdOffset.map(o => e.getField(o).toLong).getOrElse(0L)).getOrElse(0L)

    pingAmplitudeProperties merge
      ("session_id" -> (sessionStart + sessionIdOffset)) ~
      ("insert_id" -> (getClientId.getOrElse("None") + sessionStart.toString + e.getAmplitudeId)) ~
      ("event_type" -> getFullEventName(eventGroup, es.name)) ~
      ("time" -> (e.timestamp + sessionStart)) ~
      ("event_properties" -> e.getProperties(es.amplitudeProperties)) ~
      ("user_properties" -> e.getProperties(es.userProperties)) ~
      ("app_version" -> meta.appVersion) ~
      ("os_name" -> getOsName) ~
      ("os_version" -> getOsVersion) ~
      ("country" -> meta.geoCountry) ~
      ("city" -> meta.geoCity)
  }

  def getAmplitudeEvents(config: Config): Option[String] = {
    implicit val formats = DefaultFormats

    val factory = JsonSchemaFactory.byDefault
    val schemas = config.eventGroups.flatMap(g => g.events.map(e => factory.getJsonSchema(asJsonNode(e.schema))))

    val eventsList = events.map{ e => e -> asJsonNode(Extraction.decompose(e)): (Event, JsonNode) }
      .map{ case(e, es) => // for each event, try each schema
        e -> schemas.map( ts => ts.validateUnchecked(es).isSuccess )
          .zip(config.eventGroups.flatMap(g => g.events.map((g.eventGroupName, _))))
          .filter(_._1)
      }
      .filter{ case(_, em) => !em.isEmpty } // only keep those with a match
      .map{ case(e, em) => e -> em.head._2 } // take the first match (head._1 is the bool)
      .map{ case(e, (gn, es)) => eventToAmplitudeEvent(gn, e, es) }

    if (eventsList.isEmpty) {
      None
    } else {
      Some(compact(render(eventsList)))
    }
  }

  def getFullEventName(groupName: String, eventName: String): String = groupName + " - " + eventName


  def includePing(sample: Double, config: Config): Boolean = {
    val keepClient = meta.sampleId.getOrElse(sample * 100) < (sample * 100)

    if(!keepClient){
      // for maybe a slight perf increase
      return false // scalastyle:ignore
    }

    config.nonTopLevelFilters.map{ case(prop, allowedVals) =>
      allowedVals.contains(filterProperties(prop))
    }.foldLeft(true)(_ & _)
  }
}

object SendsToAmplitude {
  def apply(message: Message): SendsToAmplitude = {
    message.fieldsAsMap.get("docType") match {
      case Some("focus-event") => FocusEventPing(message)
      case Some("main") => MainPing(message)
      case Some("event") => EventPing(message)
      case Some(x) => throw new IllegalArgumentException(s"Unexpected doctype $x")
      case _ => throw new IllegalArgumentException(s"No doctype found")
    }
  }
}

trait SendsToAmplitudeWithEnvironment extends SendsToAmplitude {
  def getExperiments: Array[(Option[String], Option[String])]
  val meta: Meta
  def getClientId: Option[String]

  override def pingAmplitudeProperties: JObject = {
    val experimentsArray = getExperiments.flatMap {
      case (Some(exp), Some(branch)) => Some(s"${exp}_$branch")
      case _ => None
    }.toSeq

    ("user_properties" ->
      ("channel" -> meta.normalizedChannel) ~
        ("app_build_id" -> meta.appBuildId) ~
        ("locale" -> meta.`environment.settings`.map(_.locale)) ~
        ("is_default_browser" -> meta.`environment.settings`.map(_.isDefaultBrowser)) ~
        ("experiments" -> experimentsArray)) ~
      ("user_id" -> getClientId)
  }
}

case class Event(timestamp: Int,
                 category: String,
                 method: String,
                 `object`: String,
                 value: Option[String],
                 extra: Option[Map[String, String]]) {

  def getField(field: String): String = field match {
    case "timestamp" => timestamp.toString
    case "category" => category
    case "method" => method
    case "object" => `object`
    case "value" => value.getOrElse ("")
    case e if e.startsWith ("extra") => extra.getOrElse (Map.empty).getOrElse (e.stripPrefix ("extra."), "")
    case _ => ""
  }

  def getProperties(properties: Option[Map[String, String]]): JObject = {
    properties.getOrElse(Map.empty).map { case (k, v) => k -> getField(v) }.foldLeft(JObject())(_ ~ _)
  }

  def getAmplitudeId: String = timestamp.toString + category + method + `object`
}
