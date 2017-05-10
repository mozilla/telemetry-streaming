package com.mozilla.telemetry

import java.sql.Timestamp


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
                   appVersion: Double,
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
                   `environment.build`: Option[Build],
                   `environment.settings`: Option[Settings],
                   `environment.system`: System,
                   `environment.profile`: Option[Profile],
                   `environment.addons`: Option[Addons],
                   // Main ping fields preparsed by hindsight
                   `payload.simpleMeasurements`: Option[Map[String, Any]],
                   `payload.keyedHistograms`: Option[Map[String, Any]],
                   `payload.histograms`: Option[Map[String, Any]],
                   `payload.info`: Option[PayloadInfo]
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
    def getCountHistogramValue(histogramName: String): Long ={
      type StringMap = Map[String, Any]
      this.meta.`payload.histograms` match {
        case Some(payloadHistogram: StringMap) =>
          payloadHistogram.get(histogramName.toUpperCase) match {
            case Some(histogram: StringMap) => histogram.get("values") match {
              case Some(bucket: StringMap) => {
                bucket.get("0") match {
                  case Some(count: BigInt) => count.toLong
                  case _ => 0
                }
              }
              case _ => 0
            }
            case _ => 0
          }
        case _ => 0
      }
    }
    def usageHours(): Float = {
      this.meta.`payload.info` match {
        case Some(payloadInfo: PayloadInfo)=> {
          val sessionLength = payloadInfo.subsessionLength.get.toFloat
          Math.min(25, Math.max(0, sessionLength / 3600))
        }
        case _ => 0
      }
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

}
