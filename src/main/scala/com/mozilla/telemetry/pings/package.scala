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
                          xpcomAbi: String
                        )

  case class Build(
                    applicationId: Option[String],
                    applicationName: Option[String],
                    architecture: String,
                    buildId: String,
                    platformVersion: String,
                    vendor: String,
                    version: String,
                    xpcomAbi: String
                  )

  case class SystemOs(name: String, version: String)

  case class SystemGfxFeatures(compositor: Option[String])

  case class SystemGfx(
                        D2DEnabled: Option[Boolean],
                        DWriteEnabled: Option[Boolean],
                        features: Option[SystemGfxFeatures]
                      )


  case class System(os: SystemOs, gfx: Option[SystemGfx])

  case class ActiveExperiment(id: String, branch: String)

  case class Addons(activeExperiment: Option[ActiveExperiment])

  case class Settings(
                       blocklistEnabled: Option[Boolean],
                       isDefaultBrowser: Option[Boolean],
                       e10sEnabled: Option[Boolean],
                       e10sCohort: Option[String],
                       locale: String,
                       telemetryEnabled: Boolean
                     )

  case class Profile(
                      creationDate: Option[Int],
                      resetDate: Option[Int]
                    )

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
                   //`environment.addons`: Option[Addons],
                   // Main ping fields preparsed by hindsight
                   `payload.simpleMeasurements`: JValue,
                   `payload.keyedHistograms`: JValue,
                   `payload.histograms`: JValue,
                   `payload.info`: JValue
                 )

  case class CrashPayload(
                           crashDate: String,
                           processType: Option[String],
                           hasCrashEnvironment: Option[Boolean],
                           metadata: Option[Map[String, String]],
                           version: Option[Int]
                         )

  case class CrashPing(
                        application: Application,
                        clientId: Option[String],
                        payload: CrashPayload,
                        // Environment is omitted it's partially available under meta
                        meta: Meta
                      ){

    def isMain(): Boolean = {
      payload.processType.getOrElse("main") == "main"
    }

    def timestamp(): Timestamp = {
      new Timestamp(this.meta.Timestamp / 1000000)
    }
  }

  case class PayloadInfo(subsessionLength: Option[Int])

  case class MainPing(
                       application: Application,
                       clientId: Option[String],
                       // Environment omitted because it's mostly available under meta
                       meta: Meta
                     ){
    def getCountHistogramValue(histogram_name: String): Int = {
      try {
        this.meta.`payload.histograms` \ histogram_name \ "values" \ "0" match {
          case JInt(count) => count.toInt
          case _ => 0
        }
      } catch { case _: Throwable => 0 }
    }

    def getCountKeyedHistogramValue(histogram_name: String, key: String): Int = {
      try {
        this.meta.`payload.keyedHistograms` \ histogram_name \ key \ "values" \ "0" match {
          case JInt(count) => count.toInt
          case _ => 0
        }
      } catch { case _: Throwable => 0 }
    }


    def usageHours(): Float = {
      try {
        this.meta.`payload.info` \ "subsessionLength" match {
          case JInt(length) => Math.min(25, Math.max(0, length.toFloat / 3600))
          case _ => 0
        }
      } catch { case _: Throwable => 0 }
    }

    def timestamp(): Timestamp = {
      new Timestamp(this.meta.Timestamp / 1000000)
    }
  }

  case class Environment(build: EnvironmentBuild, system: EnvironmentSystem)

  case class EnvironmentBuild(
                               version: Option[String],
                               buildId: Option[String],
                               architecture: Option[String]
                             )

  case class EnvironmentSystem(os: OS)

  case class OS(
                 name: Option[String],
                 version: Option[String])

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
