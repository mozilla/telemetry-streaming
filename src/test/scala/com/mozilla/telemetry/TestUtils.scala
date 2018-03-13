// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.{Message, RichMessage}
import com.mozilla.telemetry.pings
import org.joda.time.{DateTime, Duration}
import org.json4s.{DefaultFormats, Extraction, JField}
import org.json4s.jackson.JsonMethods.{compact, render}


object TestUtils {
  implicit val formats = DefaultFormats
  val application = pings.Application(
    "x86", "20170101000000", "release", "Firefox", "42.0", "Mozilla", "42.0", Some("42.0b1"), "x86-msvc"
  )
  val scalarValue = 42
  val testTimestampNano = 1460036116829920000L
  val testTimestampMillis = testTimestampNano / 1000000
  val today = new DateTime(testTimestampMillis)
  val todayDays = new Duration(new DateTime(0), today).getStandardDays().toInt

  def generateCrashMessages(size: Int, fieldsOverride: Option[Map[String, Any]]=None, timestamp: Option[Long]=None): Seq[Message] = {
    val defaultMap = Map(
      "clientId" -> "client1",
      "docType" -> "crash",
      "normalizedChannel" -> application.channel,
      "appName" -> application.name,
      "appVersion" -> application.version.toDouble,
      "displayVersion" -> application.displayVersion.getOrElse(null),
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
      "environment.settings" ->"""{"e10sEnabled": true}""",
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
      "environment.profile" ->
        s"""
          |{
          | "creationDate": ${todayDays-70}
          | }""".stripMargin,
      "environment.experiments" ->
        """
          |{
          |  "experiment2": {"branch": "chaos"}
          |}""".stripMargin
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }
    val applicationJson = compact(render(Extraction.decompose(application)))
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
        timestamp=timestamp.getOrElse(testTimestampNano)
      )
    }
  }

  // scalastyle:off methodLength
  def generateMainMessages(size: Int, fieldsOverride: Option[Map[String, Any]]=None, timestamp: Option[Long]=None,
                           fieldsToRemove: List[String] = List[String]()): Seq[Message] = {
    val defaultMap = Map(
      "clientId" -> "client1",
      "docType" -> "main",
      "normalizedChannel" -> application.channel,
      "appName" -> application.name,
      "appVersion" -> application.version.toDouble,
      "displayVersion" -> application.displayVersion.getOrElse(null),
      "appBuildId" -> application.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      "submissionDate" -> "2017-01-01",
      "environment.settings" ->"""{"e10sEnabled": true}""",
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
      "environment.profile" ->
        s"""
           |{
           | "creationDate": ${todayDays-70}
           | }""".stripMargin,
      "environment.experiments" ->
        """
          |{
          |  "experiment2": {"branch": "chaos"}
          |}""".stripMargin,
      "environment.build" ->
        s"""
          |{
          |  "architecture": "${application.architecture}",
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
      "payload.simpleMeasurements" -> """{"firstPaint": 1200}""",
      "payload.info" -> """{"subsessionLength": 3600, "subsessionCounter": 1, "sessionId": "sample-session-id"}"""
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }
    val applicationData = Extraction.decompose(application) removeField {
      case JField(x, _) if fieldsToRemove.contains(x) => true
      case _ => false
    }
    val applicationJson = compact(render(applicationData))
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
        timestamp=timestamp.getOrElse(testTimestampNano)
      )
    }
  }
  // scalastyle:on methodLength

  def generateFocusEventMessages(size: Int, fieldsOverride: Option[Map[String, Any]]=None, timestamp: Option[Long]=None): Seq[Message] = {
    val defaultMap = Map(
      "clientId" -> "client1",
      "documentId" -> "doc-id",
      "docType" -> "focus-event",
      "normalizedChannel" -> "release",
      "appName" -> "Focus",
      "appVersion" -> 1.1,
      "appBuildId" -> "6",
      "geoCountry" -> "CA",
      "geoCity" -> "Victoria",
      "sampleId" -> 73L,
      "submissionDate" -> "2017-01-01",
      "submission" -> """
      |{
      |  "v":1,
      |  "clientId":"client1",
      |  "seq":162,
      |  "locale":"pt-CA",
      |  "os":"Android",
      |  "osversion":"23",
      |  "created":1506024685632,
      |  "tz":-180,
      |  "settings":{
      |    "pref_privacy_block_ads":"true",
      |    "pref_locale":"",
      |    "pref_privacy_block_social":"true",
      |    "pref_secure":"true",
      |    "pref_privacy_block_analytics":"true",
      |    "pref_search_engine":null,
      |    "pref_privacy_block_other":"false",
      |    "pref_default_browser":"true",
      |    "pref_performance_block_webfonts":"false"},
      |  "events":[
      |    [
      |      176078022,
      |      "action",
      |      "foreground",
      |      "app"
      |    ],[
      |      176127806,
      |      "action",
      |      "type_query",
      |      "search_bar"
      |    ],[
      |      176151285,
      |      "action",
      |      "click",
      |      "back_button",
      |      "erase_home"
      |    ],[
      |      176151591,
      |      "action",
      |      "background",
      |      "app",
      |      "",
      |      { "sessionLength": "1000" }
      |    ]
      |  ]
      |}
      """.stripMargin
    )

    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }

    1.to(size) map { index =>
      RichMessage(s"focus-event-${index}",
        outputMap,
        None,
        timestamp=timestamp.getOrElse(testTimestampNano)
      )
    }
  }
}
