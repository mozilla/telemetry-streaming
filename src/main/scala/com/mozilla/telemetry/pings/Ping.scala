/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import java.sql.Timestamp

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.Meta._
import org.joda.time.Months
import org.joda.time.format.DateTimeFormat
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, Extraction, JArray, JField, JNull, JObject, JValue, _}

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
        val buildDateTime = BuildDateFormat.parseDateTime(buildIdDay)
        val submissionDateTime = SubmissionDateFormat.parseDateTime(meta.submissionDate)

        Months.monthsBetween(buildDateTime, submissionDateTime).getMonths match {
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
    eventPaths.foldLeft(json) {
      case (currentJson, path) =>
        val currentEvents = path.foldLeft(currentJson)(_ \ _)
        val newEvents = currentEvents match {
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

        currentJson.replace(path, newEvents)
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
    * Returns a java Timestamp obj with microseconds resolution.
    * The source Timestamp field has nanoseconds resolution
    */
  def normalizedTimestamp(): Timestamp = {
    new Timestamp(this.Timestamp / 1000000)
  }
}

object Meta {
  private[pings] val BuildDateFormat = DateTimeFormat.forPattern("yyyyMMdd")
  private[pings] val SubmissionDateFormat = DateTimeFormat.forPattern("yyyyMMdd")
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
        val versionRegex(normalized, b, c) = v
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
