// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.{Message, RichMessage}
import com.mozilla.telemetry.pings
import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods.{compact, render}


object TestUtils {
  implicit val formats = DefaultFormats
  val application = pings.Application(
    "x86", "20170101000000", "release", "Firefox", "42.0", "Mozilla", "42.0", "x86-msvc"
  )
  private val applicationJson = compact(render(Extraction.decompose(application)))
  val scalarValue = 42
  val testTimestampNano = 1460036116829920000L
  val testTimestampMillis = testTimestampNano / 1000000

  def generateCrashMessages(size: Int, fieldsOverride: Option[Map[String, Any]]=None): Seq[Message] = {
    val defaultMap = Map(
      "docType" -> "crash",
      "normalizedChannel" -> application.channel,
      "appName" -> application.name,
      "appVersion" -> application.version.toDouble,
      "appBuildId" -> application.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      "submissionDate" -> "2017-01-01",
      "environment.build" ->
        s"""
           |{
           |  "architecture": "${application.architecture}",
           |  "buildId": "${application.buildId}",
           |  "version": "${application.version}"
           |}""".stripMargin,
      "environment.settings" ->"""{"e10sEnabled": true, "e10sCohort": "test"}""",
      "environment.system" ->
        """
          |{
          | "os": {"name": "Linux", "version": "42"},
          | "gfx": {"features": {"compositor": "opengl"}}
          |}""".stripMargin,
      "environment.addons" ->
        """
          |{
          | "activeExperiment": {"id": "experiment1", "branch": "control"},
          | "activeAddons": {"my-addon": {"isSystem": true}},
          | "theme": {"id": "firefox-compact-dark@mozilla.org"}
          |}""".stripMargin
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }
    1.to(size) map { index =>
      RichMessage(s"crash-${index}",
        outputMap,
        Some(
          s"""
             |{
             |  "payload": {
             |    "crashDate": "2017-01-01"
             |  },
             |  "application": ${applicationJson}
             |}""".stripMargin),
        timestamp=testTimestampNano
      )
    }
  }
  def generateMainMessages(size: Int, fieldsOverride: Option[Map[String, Any]]=None): Seq[Message] = {
    val defaultMap = Map(
      "docType" -> "main",
      "normalizedChannel" -> application.channel,
      "appName" -> application.name,
      "appVersion" -> application.version.toDouble,
      "appBuildId" -> application.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      "submissionDate" -> "2017-01-01",
      "environment.settings" ->"""{"e10sEnabled": true, "e10sCohort": "test"}""",
      "environment.system" ->
        """
          |{
          | "os": {"name": "Linux", "version": "42"},
          | "gfx": {"features": {"compositor": "opengl"}}
          |}""".stripMargin,
      "environment.addons" ->
        """
          |{
          | "activeExperiment": {"id": "experiment1", "branch": "control"},
          | "activeAddons": {"my-addon": {"isSystem": true}},
          | "theme": {"id": "firefox-compact-dark@mozilla.org"}
          |}""".stripMargin,
      "environment.build" ->
        s"""
           |{
             "architecture": "${application.architecture}",
           |  "buildId": "${application.buildId}",
           |  "version": "${application.version}"
           |}""".stripMargin,
      "payload.histograms" ->
            """{
          |  "BROWSER_SHIM_USAGE_BLOCKED": {"values": {"0": 1}},
          |  "INPUT_EVENT_RESPONSE_COALESCED_MS": {
          |    "values": {
          |      "1": 1,
          |      "150": 2,
          |      "250": 3,
          |      "2500": 4,
          |      "10000": 5
          |    }
          |  }
          |}""".stripMargin,
      "payload.keyedHistograms" ->
        """
          |{
          |  "SUBPROCESS_CRASHES_WITH_DUMP": {
          |    "content": {"values": {"0": 1}},
          |    "gpu": {"values": {"0": 1}},
          |    "plugin": {"values": {"0": 1}},
          |    "gmplugin": {"values": {"0": 1}}
          |  },
          |  "SUBPROCESS_KILL_HARD": {
          |    "ShutDownKill": {"values": {"0": 1}}
          |  }
          |}""".stripMargin,
      "payload.info" -> """{"subsessionLength": 3600}"""
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }
    1.to(size) map { index =>
      RichMessage(s"main-${index}",
        outputMap,
        Some(s"""{
             |  "application": ${applicationJson},
             |  "payload": {
             |    "processes": {
             |      "content": {
             |        "histograms": {
             |          "INPUT_EVENT_RESPONSE_COALESCED_MS": {
             |            "values": {
             |              "1": 1,
             |              "150": 1,
             |              "250": 1,
             |              "2500": 1,
             |              "10000": 1
             |            }
             |          }
             |        }
             |      }
             |    }
             |  }
             |}""".stripMargin),
        timestamp=testTimestampNano
      )
    }
  }
}
