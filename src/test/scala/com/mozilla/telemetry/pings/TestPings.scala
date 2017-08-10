// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import org.scalatest.{FlatSpec, Matchers}
import com.mozilla.telemetry.pings._
import org.joda.time.{DateTime, Duration}


class TestPings extends FlatSpec with Matchers{

  val message = TestUtils.generateMainMessages(1).head
  val mainPing = MainPing(message)
  val ts = TestUtils.testTimestampMillis

  "MainPing" should "return the value of a count histogram" in {
    mainPing.getCountHistogramValue("foo").isEmpty should be (true)
    mainPing.getCountHistogramValue("BROWSER_SHIM_USAGE_BLOCKED").get should be (1)
  }
  it should "return the value of a keyed count histogram" in {
    mainPing.getCountKeyedHistogramValue("foo", "bar").isEmpty should be (true)
    mainPing.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "foo").isEmpty should be (true)
    mainPing.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "content").get should be (1)
  }
  it should "return the value of its usage hours" in {
    mainPing.usageHours.get should be (1.0)
    val messageNoUsageHours = TestUtils.generateMainMessages(1, Some(Map("payload.info" -> "{}"))).head
    val pingNoUsageHours = MainPing(messageNoUsageHours)
    pingNoUsageHours.usageHours.isEmpty should be (true)
  }
  it should "return its timestamp" in {
    mainPing.meta.normalizedTimestamp() should be (new Timestamp(ts))
  }
  it should "return the right threshold count" in {
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 150, "main") should be (14)
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 250, "main") should be (12)
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 2500, "main") should be (9)

    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 150, "content") should be (4)
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 250, "content") should be (3)
    mainPing.histogramThresholdCount("INPUT_EVENT_RESPONSE_COALESCED_MS", 2500, "content") should be (2)
  }
  it should "return its firstPaint value" in {
    mainPing.firstPaint should be (Some(1200))
  }

  it should "detect if it's the first subsession" in {
    mainPing.isFirstSubsession should be (Some(true))
  }

  it should "not return its firstPaint value if non-first subsession" in {
    val subsequentMessage = TestUtils.generateMainMessages(1, Some(Map(
      "payload.info" -> """{"subsessionLength": 3600, "subsessionCounter": 2}"""
    ))).head
    val subsequentPing = MainPing(subsequentMessage)
    subsequentPing.firstPaint should be(None)
  }

  it should "return its sessionId" in {
    mainPing.sessionId should be (Some("sample-session-id"))
  }

  val recentTheme = new Theme("firefox-compact-dark@mozilla.org")
  val oldTheme = new Theme("the-oldest-theme-ever")

  val webExtension = ActiveAddon(None, Some(true))
  val systemAddon = ActiveAddon(Some(true), None)
  val legacyAddon = ActiveAddon(None, None)

  val validAddonMap = Map("new-addon" -> webExtension)
  val invalidAddonMap = Map("new-addon" -> webExtension, "old-addon" -> legacyAddon)

  val quantumReadyPing = TestUtils.generateMainMessages(1, Some(Map(
    "environment.settings" -> """{"e10sEnabled": true}""",
    "environment.addons" -> """{
        |"activeAddons": {"mySystemAddon": {"isSystem": true}},
        |"theme": {"id": "firefox-compact-dark@mozilla.org"}
     }""".stripMargin)
  )).head
  val notQuantumReadyPing = TestUtils.generateMainMessages(1, Some(Map(
    "environment.settings" -> """{"e10sEnabled": false}""",
    "environment.addons" -> """{
        |"activeAddons": {"mySystemAddon": {"isSystem": true}},
        |"theme": {"id": "firefox-compact-dark@mozilla.org"}
     }""".stripMargin)
  )).head
  val unknownThemeQuantumReadyPing = TestUtils.generateMainMessages(1, Some(Map(
    "environment.settings" -> """{"e10sEnabled": true}""",
    "environment.addons" -> """{
        |"activeAddons": {"mySystemAddon": {"isSystem": true}}
     }""".stripMargin)
  )).head
  val unknownE10sQuantumReadyPing = TestUtils.generateMainMessages(1, Some(Map(
    "environment.settings" -> """{}""",
    "environment.addons" -> """{
        |"activeAddons": {"mySystemAddon": {"isSystem": true}},
        |"theme": {"id": "firefox-compact-dark@mozilla.org"}
     }""".stripMargin)
  )).head

  "A Theme instance" should "know whether it's old or not" in {
    recentTheme.isOld should be (false)
    oldTheme.isOld should be (true)
  }

  "An ActiveAddon instance" should "know whether it's quantumReady or not" in {
    webExtension.isQuantumReady should be (true)
    systemAddon.isQuantumReady should be (true)
    legacyAddon.isQuantumReady should be (false)
  }

  "An empty Addons instance" should "have an unknown quantumReady" in {
    Addons(None, None, None).isQuantumReady should be (None)
  }

  "An Addons instance with no theme" should "have unknown quantumReady" in {
    Addons(Some(validAddonMap), None, None).isQuantumReady should be (None)
  }

  "An Addons instance with no addons" should "be quantumReady" in {
    Addons(None, None, Some(recentTheme)).isQuantumReady should be (None)
  }

  "An Addons instance with valid Addons and old theme" should "not be quantum ready" in {
    Addons(Some(validAddonMap), None, Some(oldTheme)).isQuantumReady should be (Some(false))
  }

  "An Addons instance with invalid Addons and new theme" should "not be quantum ready" in {
    Addons(Some(invalidAddonMap), None, Some(recentTheme)).isQuantumReady should be (Some(false))
  }

  "An Addons instance with valid Addons and new theme" should "be quantum ready" in {
    Addons(Some(validAddonMap), None, Some(recentTheme)).isQuantumReady should be (Some(true))
  }

  "A Meta instance with unknown e10sEnable and quantumReady addons" should "be quantumReady unknown" in {
    MainPing(unknownE10sQuantumReadyPing).meta.isQuantumReady should be (None)
  }

  "A Meta instance with e10s disabled and quantumReady addons" should "not be quantumReady" in {
    MainPing(notQuantumReadyPing).meta.isQuantumReady should be (Some(false))
  }

  "A Meta instance with e10s enabled and quantumReady addons" should "be quantumReady" in {
    MainPing(quantumReadyPing).meta.isQuantumReady should be (Some(true))
  }
  "A Meta instance with e10s enabled and unknown quantumReady addons" should "be quantumReady unknown" in {
    MainPing(unknownThemeQuantumReadyPing).meta.isQuantumReady should be (None)
  }

  "A Profile instance" should "return its age in days" in {
    val today = TestUtils.today
    val todayDays = TestUtils.todayDays
    // Profile with age zero
    Profile(Some(todayDays), None).ageDays(today) should be (Some(0))
    // Profile with positive age
    Profile(Some(todayDays - 10), None).ageDays(today) should be (Some(10))
    // Profile with negative age
    Profile(Some(todayDays + 10), None).ageDays(today) should be (None)

    Profile(Some(todayDays - 42), None).ageDaysBin(today) should be (Some(42))
    Profile(Some(todayDays - 43), None).ageDaysBin(today) should be (Some(49))
    Profile(Some(todayDays - 49), None).ageDaysBin(today) should be (Some(49))
    Profile(Some(todayDays - 364), None).ageDaysBin(today) should be (Some(364))
    Profile(Some(todayDays - 367), None).ageDaysBin(today) should be (Some(365))
    Profile(Some(todayDays - 3000), None).ageDaysBin(today) should be (Some(365))

  }
}
