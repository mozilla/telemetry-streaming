// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry

import scala.util.{Success, Try}
import java.sql.Timestamp

import com.mozilla.telemetry.heka.Message
import org.joda.time.{DateTime, Duration, Months}
import org.joda.time.format.DateTimeFormat
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

  case class SystemOs(name: String, version: String) {
    def normalizedVersion: String = OS(Option(name), Option(version)).normalizedVersion
  }

  case class SystemGfxFeatures(compositor: Option[String])

  case class SystemGfx(
      D2DEnabled: Option[Boolean],
      DWriteEnabled: Option[Boolean],
      features: Option[SystemGfxFeatures])

  case class System(os: SystemOs, gfx: Option[SystemGfx])

  case class OldStyleExperiment(id: String, branch: String)

  case class NewStyleExperiment(branch: String)

  case class ActiveAddon(isSystem: Option[Boolean], isWebExtension: Option[Boolean]) {
    def isQuantumReady: Boolean = {
      (this.isSystem contains true) || (this.isWebExtension contains true)
    }
  }

  object Theme {
    val newThemes = List(
      "{972ce4c6-7e08-4474-a285-3208198ce6fd}",
      "firefox-compact-light@mozilla.org",
      "firefox-compact-dark@mozilla.org"
    )
  }

  case class Theme(id: String) {
    def isOld: Boolean = ! Theme.newThemes.contains(this.id)
  }

  case class Addons(
      activeAddons: Option[Map[String, ActiveAddon]],
      activeExperiment: Option[OldStyleExperiment],
      theme: Option[Theme]
  ) {
    def isQuantumReady: Option[Boolean] = {
      for {
        activeAddons <- this.activeAddons
        theme <- this.theme
      } yield activeAddons.values.forall(_.isQuantumReady) && ! theme.isOld
    }
  }

  case class Settings(
      blocklistEnabled: Option[Boolean],
      isDefaultBrowser: Option[Boolean],
      e10sEnabled: Option[Boolean],
      e10sCohort: Option[String],
      locale: Option[String],
      telemetryEnabled: Option[Boolean])

  case class Profile(creationDate: Option[Int], resetDate: Option[Int]){
    def ageDays(today: DateTime): Option[Int] ={
      creationDate match {
        case Some(days) if days >= 0 => {
          val creationDateTime = new DateTime(0).plusDays(days)
          val age = new Duration(creationDateTime, today)
          if (age.getMillis > 0) Some(age.getStandardDays.toInt) else None
        }
        case _ => None
      }
    }

    /**
     * Return the profile age binned using the following logic:
     * up to 6 weeks -> daily resolution
     * up to 1 year -> weekly resolution
     * over 1 year -> bin 366
     */
    def ageDaysBin(today: DateTime): Option[Int] = {
      ageDays(today) match {
        case Some(d) if d <= 42 => Some(d)
        case Some(d) if d <= 364 => Some(((d / 7.0).ceil * 7).toInt)
        case Some(d) => Some(365)
        case _ => None
      }
    }
  }

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

    def normalizedBuildId(): Option[String] = {
      `environment.build`.flatMap(_.buildId) match {
        case Some(buildId: String) => {
          val buildIdDay = buildId.slice(0, 8).toString()
          val buildDateFormat = DateTimeFormat.forPattern("yyyyMMdd")
          val buildDateTime = buildDateFormat.parseDateTime(buildIdDay)
          val submissionDateTime = DateTime.parse(submissionDate)
          val p = Months.monthsBetween(buildDateTime, submissionDateTime)

          p.getMonths() match {
            case p if 0 to 6 contains(p) => Some(buildId)
            case _ => None
          }
        }
        case _ => None
      }
    }

    def isE10sEnabled: Option[Boolean] = this.`environment.settings`.flatMap(_.e10sEnabled)

    def experiments: Seq[(Option[String], Option[String])] = {
      val oldStyleExperiment = for {
        addons <- this.`environment.addons`
        experiment <- addons.activeExperiment
      } yield (Some(experiment.id), Some(experiment.branch))

      val newStyleExperiments = for {
        experiments <- this.`environment.experiments`.toSeq
        (experimentId, experiment) <- experiments
      } yield (Some(experimentId), Some(experiment.branch))

      newStyleExperiments ++ oldStyleExperiment
    }

    def isQuantumReady: Option[Boolean] = {
      for {
        addons <- this.`environment.addons`
        addonsReady <- addons.isQuantumReady
        e10sEnabled <- this.isE10sEnabled
      } yield addonsReady && e10sEnabled
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

  object CrashPing {
    def apply(message: Message): CrashPing = {
      implicit val formats = DefaultFormats
      val jsonFieldNames = List(
        "environment.build",
        "environment.settings",
        "environment.system",
        "environment.profile",
        "environment.addons",
        "environment.experiments"
      )
      val ping = messageToPing(message, jsonFieldNames)
      ping.extract[CrashPing]
    }
  }

  case class PayloadInfo(subsessionLength: Option[Int])

  case class MainPingPayload(processes: JValue)

  case class MainPing(
      application: Application,
      clientId: Option[String],
      // Environment omitted because it's mostly available under meta
      meta: Meta,
      payload: MainPingPayload
  ) {
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

    // Return the number of values greater than threshold
    def histogramThresholdCount(histogramName: String, threshold: Int, processType: String): Long = {
      implicit val formats = org.json4s.DefaultFormats
      val histogram = processType match {
        case "main" => this.meta.`payload.histograms`
        case _ => this.payload.processes \ processType \ "histograms"
      }

      histogram \ histogramName \ "values" match {
        case JNothing => 0
        case v => Try(v.extract[Map[String, Int]]) match {
          case Success(m) =>
            m.filterKeys(s => Try(s.toInt).toOption match {
              case Some(key) => key >= threshold
              case None => false
            }).foldLeft(0)(_ + _._2)
          case _ => 0
        }
      }
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

    /*
    * firstPaint is tricky because we only want to know this value if it
    * comes from the first subsession.
    */
    def firstPaint: Option[Int] = {
      this.isFirstSubsession match {
        case Some(true) => this.meta.`payload.simpleMeasurements` \ "firstPaint" match {
          case JInt(value) => Some(value.toInt)
          case _ => None
        }
        case _ => None
      }
    }

    def isFirstSubsession: Option[Boolean] = {
      this.meta.`payload.info` \ "subsessionCounter" match {
        case JInt(v)  => Some(v == 1)
        case _ => None
      }
    }

    def sessionId: Option[String] = {
      this.meta.`payload.info` \ "sessionId" match {
        case JString(v) => Some(v)
        case _ => None
      }
    }

  }
  object MainPing {

    val processTypes = ("main", "content")

    def apply(message: Message): MainPing = {
      implicit val formats = DefaultFormats
      val jsonFieldNames = List(
        "environment.build",
        "environment.settings",
        "environment.system",
        "environment.profile",
        "environment.addons",
        "environment.experiments",
        "payload.simpleMeasurements",
        "payload.keyedHistograms",
        "payload.histograms",
        "payload.info"
      )
      val ping = messageToPing(message, jsonFieldNames)
      ping.extract[MainPing]
    }
  }

  case class Environment(build: EnvironmentBuild, system: EnvironmentSystem)

  case class EnvironmentBuild(
      version: Option[String],
      buildId: Option[String],
      architecture: Option[String])

  case class EnvironmentSystem(os: OS)

  case class OS(name: Option[String], version: Option[String]){
    val versionRegex = "(\\d+(\\.\\d+)?(\\.\\d+)?)?.*".r
    def normalizedVersion: String = {
      version match {
        case Some(v) =>
          val versionRegex(normalized, b, c) = v
          normalized
        case None =>
          null
      }
    }
  }

  def messageToPing(message: Message, jsonFieldNames: List[String]): JValue = {
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
