/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.mozilla.telemetry.heka.{Message, RichMessage}
import com.mozilla.telemetry.pings
import com.mozilla.telemetry.pings.Application
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.{DefaultFormats, Extraction, JField}


object TestUtils {
  implicit val formats = DefaultFormats
  val defaultFirefoxApplication = pings.Application(
    "x86", "20170101000000", "release", "Firefox", "42.0", "Mozilla", "42.0", Some("42.0b1"), "x86-msvc"
  )
  val defaultFennecApplication = pings.Application(
    "arm64-v8a", "20170101000000", "release", "Fennec", "59.0", "Mozilla", "59.0", Some("59.0b1"), "arm-eabi-gcc3"
  )

  val scalarValue = 42
  val testTimestampNano = 1460036116829920000L
  val testTimestampMillis = testTimestampNano / 1000000
  val todayDays = ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.now())

  def generateCrashMessages(size: Int,
                            fieldsOverride: Option[Map[String, Any]] = None,
                            customMetadata: Option[String] = None,
                            customPayload: Option[String] = None,
                            timestamp: Option[Long] = None,
                            appType: AppType = Firefox): Seq[Message] = {

    val application = appType match {
      case Fennec => defaultFennecApplication
      case _ => defaultFirefoxApplication
    }

    val defaultMap = Map(
      "clientId" -> "client1",
      "docType" -> "crash",
      "normalizedChannel" -> application.channel,
      "appName" -> application.name,
      "appVersion" -> application.version.toDouble,
      "displayVersion" -> application.displayVersion.orNull,
      "appBuildId" -> application.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      "submissionDate" -> "20170101",
      "environment.build" ->
        s"""
           |{
           |  "architecture": "${application.architecture}",
           |  "buildId": "${application.buildId}",
           |  "version": "${application.version}"
           |}""".stripMargin,
      "environment.system" ->
        """
          |{
          | "os": {"name": "Linux", "version": "42"}
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
    val payload =
      s""""crashDate": "2017-01-01",
        |"metadata":{
        |  ${customMetadata.getOrElse("")}
        |}
        |${customPayload.map("," + _).getOrElse("")}
      """.stripMargin
    1.to(size) map { index =>
      RichMessage(s"crash-$index",
        outputMap,
        Some(
          s"""
             |{
             |  "payload": { $payload },
             |  "application": $applicationJson
             |}""".stripMargin),
        timestamp=timestamp.getOrElse(testTimestampNano)
      )
    }
  }

  // scalastyle:off methodLength
  def generateMainMessages(size: Int, fieldsOverride: Option[Map[String, Any]] = None, timestamp: Option[Long] = None,
                           fieldsToRemove: List[String] = List[String](), customPayload: Option[String] = None): Seq[Message] = {
    val defaultMap = Map(
      "clientId" -> "client1",
      "docType" -> "main",
      "normalizedChannel" -> defaultFirefoxApplication.channel,
      "appName" -> defaultFirefoxApplication.name,
      "appVersion" -> defaultFirefoxApplication.version.toDouble,
      "displayVersion" -> defaultFirefoxApplication.displayVersion.getOrElse(null),
      "appBuildId" -> defaultFirefoxApplication.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      "submissionDate" -> "20170101",
      "sampleId" -> 73L,
      "environment.system" ->
        """
          |{
          | "os": {"name": "Linux", "version": "42"}
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
          |  "architecture": "${defaultFirefoxApplication.architecture}",
          |  "buildId": "${defaultFirefoxApplication.buildId}",
          |  "version": "${defaultFirefoxApplication.version}"
          |}""".stripMargin,
      "environment.settings" ->
        """
          |{
          |  "locale": "it_IT",
          |  "isDefaultBrowser": true
          |}
        """.stripMargin,
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
      "payload.info" ->
        """
          |{
          |  "subsessionLength": 3600,
          |  "subsessionCounter": 1,
          |  "sessionId": "sample-session-id",
          |  "sessionStartDate": "2018-05-30T11:00:00.0-05:00"
          |}""".stripMargin
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }
    val applicationData = Extraction.decompose(defaultFirefoxApplication) removeField {
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
             |      ${customPayload.getOrElse("")}
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
      "submissionDate" -> "20170101",
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
      |    "pref_privacy_block_ads": "true",
      |    "pref_locale": "",
      |    "pref_privacy_block_social": "true",
      |    "pref_secure": "true",
      |    "pref_privacy_block_analytics": "true",
      |    "pref_search_engine": "custom",
      |    "pref_privacy_block_other": "false",
      |    "pref_default_browser": "true",
      |    "pref_performance_block_webfonts": "false",
      |    "pref_performance_block_images": "false",
      |    "pref_autocomplete_installed": "true",
      |    "pref_autocomplete_custom": "false"},
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
      |      "erase_home",
      |      { "host": "side" }
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

  // scalastyle:off methodLength
  def generateFennecCoreMessages(size: Int, fieldsOverride: Option[Map[String, Any]] = None, timestamp: Option[Long] = None,
                                 app: Application = defaultFennecApplication): Seq[Message] = {
    val defaultMap = Map(
      "appBuildId" -> app.buildId,
      "appName" -> app.name,
      "appUpdateChannel" -> app.channel,
      "appVersion" -> app.version.toDouble,
      "clientId" -> "ca7fb81d-5deb-4ea6-8b74-797b8e58cfae",
      "Date" -> "Sun, 29 Apr 2018 15:35:38 GMT+00:00",
      "docType" -> "core",
      "documentId" -> "befd87f9-472d-4a38-9ae1-2f4668e10c62",
      "geoCity" -> "Modena",
      "geoCountry" -> "IT",
      "geoSubdivision1" -> "45",
      "geoSubdivision2" -> "MO",
      "Host" -> "incoming.telemetry.mozilla.org",
      "normalizedChannel" -> "release",
      "sampleId" -> 22L,
      "sourceName" -> "telemetry",
      "sourceVersion" -> "9",
      "submissionDate" -> "20170101",
      "submission" ->
        s"""
           |{
           |  "durations": 3600,
           |  "device": "samsung-SM-G930F",
           |  "displayVersion": ${app.displayVersion.map("\"" + _ + "\"").orNull},
           |  "experiments": [
           |    "experiment1",
           |    "experiment2"
           |  ],
           |  "tz": 120,
           |  "flashUsage": 0,
           |  "locale": "en-US",
           |  "arch": "arm64-v8a",
           |  "os": "Android",
           |  "defaultSearch": "google",
           |  "seq": 1,
           |  "v": 9,
           |  "clientId": "ca7fb81d-5deb-4ea6-8b74-797b8e58cfae",
           |  "osversion": "42",
           |  "sessions": 1,
           |  "profileDate": 17622,
           |  "defaultBrowser": false,
           |  "created": "2018-04-29",
           |  "searches": {
           |    "google.actionbar": 1
           |  }
           |}
        """.stripMargin
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }

    1.to(size) map { index =>
      RichMessage(s"core-ping-$index",
        outputMap,
        None,
        timestamp = timestamp.getOrElse(testTimestampNano)
      )
    }
  }
  // scalastyle:on methodLength

  def generateEventMessages(size: Int,
                            fieldsOverride: Option[Map[String, Any]] = None,
                            timestamp: Option[Long] = None): Seq[Message] = {
    val defaultMap = Map(
      "clientId" -> "client1",
      "docType" -> "event",
      "documentId" -> "an_id",
      "normalizedChannel" -> defaultFirefoxApplication.channel,
      "appName" -> defaultFirefoxApplication.name,
      "appVersion" -> defaultFirefoxApplication.version.toDouble,
      "displayVersion" -> defaultFirefoxApplication.displayVersion.orNull,
      "appBuildId" -> defaultFirefoxApplication.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      "submissionDate" -> "20170101",
      "environment.build" ->
        s"""
           |{
           |  "architecture": "${defaultFirefoxApplication.architecture}",
           |  "buildId": "${defaultFirefoxApplication.buildId}",
           |  "version": "${defaultFirefoxApplication.version}"
           |}""".stripMargin,
      "environment.system" ->
        """
          |{
          | "os": {"name": "Linux", "version": "42"}
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
    val applicationJson = compact(render(Extraction.decompose(defaultFirefoxApplication)))
    val payload =
      s"""
         |    "reason": "periodic",
         |    "processStartTimestamp": 1530291900000,
         |    "sessionId": "dd302e9d-569b-4058-b7e8-02b2ff83522c",
         |    "subsessionId": "79a2728f-af12-4ed3-b56d-0531a03c2f26",
         |    "lostEventsCount": 0,
         |    "events": {
         |      "parent": [
         |        [
         |          4118829,
         |          "activity_stream",
         |          "end",
         |          "session",
         |          "909",
         |          {
         |            "addon_version": "2018.06.22.1337-8d599e17",
         |            "user_prefs": "63",
         |            "session_id": "{78fe2428-15fb-4448-b517-cbb85f22def0}",
         |            "page": "about:newtab"
         |          }
         |        ]
         |      ],
         |      "dynamic": [
         |        [
         |          4203541,
         |          "test",
         |          "no",
         |          "string_value",
         |          null,
         |          {
         |            "hello": "world"
         |          }
         |        ],
         |        [
         |          4203542,
         |          "test",
         |          "no",
         |          "extras"
         |        ],
         |        [
         |          4203540,
         |          "normandy",
         |          "enroll",
         |          "preference_study",
         |          "awesome-experiment",
         |          {
         |            "branch": "control",
         |            "experimentType": "exp"
         |          }
         |        ]
         |      ]
         |    }
       """.stripMargin
    1.to(size) map { index =>
      RichMessage(s"event-$index",
        outputMap,
        Some(
          s"""
             |{
             |  "payload": { $payload },
             |  "application": $applicationJson
             |}""".stripMargin),
        timestamp=timestamp.getOrElse(testTimestampNano)
      )
    }
  }

  def generateFrecencyUpdateMessages(size: Int,
                                     fieldsOverride: Option[Map[String, Any]] = None,
                                     timestamp: Option[Long] = None): Seq[Message] = {
    val defaultMap = Map(
      "clientId" -> "client1",
      "docType" -> "frecency-update",
      "documentId" -> "an_id",
      "normalizedChannel" -> defaultFirefoxApplication.channel,
      "appName" -> defaultFirefoxApplication.name,
      "appVersion" -> defaultFirefoxApplication.version.toDouble,
      "displayVersion" -> defaultFirefoxApplication.displayVersion.orNull,
      "appBuildId" -> defaultFirefoxApplication.buildId,
      "geoCountry" -> "IT",
      "os" -> "Linux",
      "submissionDate" -> "20170101"
    )
    val outputMap = fieldsOverride match {
      case Some(m) => defaultMap ++ m
      case _ => defaultMap
    }
    val applicationJson = compact(render(Extraction.decompose(defaultFirefoxApplication)))
    val payload =
      s"""
         |    "frecency_scores": [
         |      38223,
         |      3933.4,
         |      304933.3,
         |      21
         |    ],
         |    "loss": 291989.21,
         |    "model_version": 3,
         |    "num_chars_typed": 5,
         |    "num_suggestions_displayed": 6,
         |    "rank_selected": 2,
         |    "study_variation": "treatment",
         |    "update": [
         |      1.2,
         |      3.2,
         |      -3.1,
         |      4.4,
         |      0.5,
         |      0.234,
         |      -0.98,
         |      0.33,
         |      0.34,
         |      0.28,
         |      0.302,
         |      0.4,
         |      -0.8,
         |      0.25,
         |      0.9,
         |      -0.8,
         |      0.29,
         |      0.42,
         |      0.89,
         |      0.39,
         |      0.54,
         |      0.78
         |    ]
       """.stripMargin
    1.to(size) map { index =>
      RichMessage(s"event-$index",
        outputMap,
        Some(
          s"""
             |{
             |  "payload": { $payload },
             |  "application": $applicationJson
             |}""".stripMargin),
        timestamp = timestamp.getOrElse(testTimestampNano)
      )
    }
  }

  abstract class AppType
  case object Firefox extends AppType
  case object Fennec extends AppType
}
