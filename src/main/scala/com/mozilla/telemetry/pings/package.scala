// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry

import java.sql.Timestamp

import com.mozilla.telemetry.heka.Message
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

package object pings {
  case class Application(
      architecture: String,
      buildId: String,
      channel: String,
      name: String,
      platformVersion: String,
      vendor: String,
      version: String,
      xpcomAbi: String)

  case class Build(
      applicationId: Option[String],
      applicationName: Option[String],
      architecture: String,
      buildId: String,
      platformVersion: String,
      vendor: String,
      version: String,
      xpcomAbi: String)

  case class SystemOs(name: String, version: String)

  case class SystemGfxFeatures(compositor: Option[String])

  case class SystemGfx(
      D2DEnabled: Option[Boolean],
      DWriteEnabled: Option[Boolean],
      features: Option[SystemGfxFeatures])

  case class System(os: SystemOs, gfx: Option[SystemGfx])

  case class OldStyleExperiment(id: String, branch: String)

  case class NewStyleExperiment(branch: String)

  case class Addons(activeExperiment: Option[OldStyleExperiment])

  case class Settings(
      blocklistEnabled: Option[Boolean],
      isDefaultBrowser: Option[Boolean],
      e10sEnabled: Option[Boolean],
      e10sCohort: Option[String],
      locale: Option[String],
      telemetryEnabled: Option[Boolean])

  case class Profile(creationDate: Option[Int], resetDate: Option[Int])

  case class Meta(
      Host: Option[String],
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
      os: String,
      sampleId: Option[Double],
      sourceName: Option[String],
      sourceVersion: Option[Int],
      submissionDate: String,
      telemetryEnabled: Option[Boolean],
      // Common fields preparsed by hindsight
      `environment.build`: Option[EnvironmentBuild],
      `environment.settings`: Option[Settings],
      `environment.system`: Option[System],
      `environment.profile`: Option[Profile],
      `environment.addons`: Option[Addons],
      `environment.experiments`: Option[Map[String, NewStyleExperiment]],
      // Main ping fields preparsed by hindsight
      `payload.simpleMeasurements`: JValue,
      `payload.keyedHistograms`: JValue,
      `payload.histograms`: JValue,
      `payload.info`: JValue) {

    /**
      * Returns a java Timestamp obj with microseconds resolution.
      * The source Timestamp field has nanoseconds resolution
      */
    def normalizedTimestamp(): Timestamp = {
      new Timestamp(this.Timestamp / 1000000)
    }

    /**
      * Returns a Tuple2(experimentId, experimentBranch).
      *
      * This information could be in 2 different locations depending on whether the
      * user is enrolled in a new style experiment or in a old style one.
      *
      * To further complicate things, in case of a new style experiment, the user
      * could be enrolled in more than one experiment. In that case we arbitrary decide
      * to pick the first experiment.
      */
    def experiment: Option[(String, String)] = {
      val oldStyleExperiment = for {
        addons <- this.`environment.addons`
        experiment <- addons.activeExperiment
      } yield (experiment.id, experiment.branch)

      val newStyleExperiment = for {
        experiments <- this.`environment.experiments`
        (experimentId, experiment) <- experiments.headOption
      } yield (experimentId, experiment.branch)
      oldStyleExperiment orElse newStyleExperiment
    }
  }

  case class CrashPayload(
      crashDate: String,
      processType: Option[String],
      hasCrashEnvironment: Option[Boolean],
      metadata: Option[Map[String, String]],
      version: Option[Int])

  case class CrashPing(
      application: Application,
      clientId: Option[String],
      payload: CrashPayload,
      // Environment is omitted it's partially available under meta
      meta: Meta) {

    def isMainCrash: Boolean = {
      payload.processType.getOrElse("main") == "main"
    }
  }

  case class PayloadInfo(subsessionLength: Option[Int])

  case class MainPing(
      application: Application,
      clientId: Option[String],
      // Environment omitted because it's mostly available under meta
      meta: Meta) {
    def getCountHistogramValue(histogram_name: String): Option[Int] = {
      try {
        this.meta.`payload.histograms` \ histogram_name \ "values" \ "0" match {
          case JInt(count) => Some(count.toInt)
          case _ => None
        }
      } catch { case _: Throwable => None }
    }

    def getCountKeyedHistogramValue(histogram_name: String, key: String): Option[Int] = {
      try {
        this.meta.`payload.keyedHistograms` \ histogram_name \ key \ "values" \ "0" match {
          case JInt(count) => Some(count.toInt)
          case _ => None
        }
      } catch { case _: Throwable => None }
    }

    def usageHours: Option[Float] = {
      val seconds_per_hour = 3600
      val max_hours = 25
      val min_hours = 0
      try {
        this.meta.`payload.info` \ "subsessionLength" match {
          case JInt(length) => Some(Math.min(max_hours, Math.max(min_hours, length.toFloat / seconds_per_hour)))
          case _ => None
        }
      } catch { case _: Throwable => None }
    }
  }

  case class Environment(build: EnvironmentBuild, system: EnvironmentSystem)

  case class EnvironmentBuild(
      version: Option[String],
      buildId: Option[String],
      architecture: Option[String])

  case class EnvironmentSystem(os: OS)

  case class OS(name: Option[String], version: Option[String])

  def messageToCrashPing(message: Message): CrashPing = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List(
      "environment.build",
      "environment.settings",
      "environment.system",
      "environment.profile",
      "environment.addons"
    )
    val ping = messageToPing(message, jsonFieldNames)
    ping.extract[CrashPing]
  }

  def messageToMainPing(message: Message): MainPing = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List(
      "environment.build",
      "environment.settings",
      "environment.system",
      "environment.profile",
      "environment.addons",
      "payload.simpleMeasurements",
      "payload.keyedHistograms",
      "payload.histograms",
      "payload.info"
    )
    val ping = messageToPing(message, jsonFieldNames)
    ping.extract[MainPing]
  }

  def messageToPing(message:Message, jsonFieldNames: List[String]): JValue = {
    implicit val formats = DefaultFormats
    val fields = message.fieldsAsMap ++ Map("Timestamp" -> message.timestamp)
    val jsonObj = Extraction.decompose(fields)
    // Transform json fields into JValues
    val meta = jsonObj transformField {
      case JField(key, JString(s)) if jsonFieldNames contains key => (key, parse(s))
    }
    val submission = if(message.payload.isDefined) message.payload else fields.get("submission")
    submission match {
      case Some(value: String) => parse(value) ++ JObject(List(JField("meta", meta)))
      case _ => JObject()
    }
  }
}
