/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.holdenkarau.spark.testing.StructuredStreamingBase
import org.json4s.jackson.Serialization.write
import org.scalatest._
import com.mozilla.telemetry.streaming.UptakeEvents._

class UptakeEventsToDatadogTest extends FlatSpec with Matchers with GivenWhenThen with StructuredStreamingBase with BeforeAndAfterEach {
  val k = 2

  "UptakeEventsToDatadog" should "parse events into DogStatsDMetric messages" in {
    import spark.implicits._

    val uptakeEvents = Seq(
      NormandyRecipe(123),
      NormandyRecipe(123, "custom_2_error"),
      NormandyAction("ShowHeartbeatAction"),
      NormandyRunner(),
      RemoteSettings(None, None, "blocklists/certificates"),
      RemoteSettings(Some(4778), None, "settings-changes-monitoring", "success"),
      RemoteSettings(None, Some(1401), "main/normandy-recipes", "success")
    )

    val pings = (
        TestUtils.generateEventMessages(k, customPayload=EnrollmentEvents.eventPingEnrollmentEventsJson(EnrollmentEvents.ExperimentA, Some("six"),
        enroll = true, process="parent"))
        ++ TestUtils.generateEventMessages(k, customPayload = eventPingUptakeEventsJson(uptakeEvents))
      ).map(_.toByteArray).seq

    val pingsDf = spark.createDataset(pings).toDF()
    val metrics = UptakeEventsToDatadog.eventsToMetrics(pingsDf, raiseOnError = true)
    val expected = Seq(
      "telemetry.normandy.preference_study.enroll:1|c|#experiment:pref-flip-timer-speed-up-60-1443940,branch:six",
      "telemetry.normandy.preference_study.enroll:1|c|#experiment:pref-flip-timer-speed-up-60-1443940,branch:six",
      "telemetry.uptake.uptake.normandy.success:1|c|#source_type:normandy,source_subtype:recipe,source_details:123",
      "telemetry.uptake.uptake.normandy.success:1|c|#source_type:normandy,source_subtype:recipe,source_details:123",
      "telemetry.uptake.uptake.normandy.custom_2_error:1|c|#source_type:normandy,source_subtype:recipe,source_details:123",
      "telemetry.uptake.uptake.normandy.custom_2_error:1|c|#source_type:normandy,source_subtype:recipe,source_details:123",
      "telemetry.uptake.uptake.normandy.success:1|c|#source_type:normandy,source_subtype:action,source_details:ShowHeartbeatAction",
      "telemetry.uptake.uptake.normandy.success:1|c|#source_type:normandy,source_subtype:action,source_details:ShowHeartbeatAction",
      "telemetry.uptake.uptake.normandy.success:1|c|#source_type:normandy,source_subtype:runner",
      "telemetry.uptake.uptake.normandy.success:1|c|#source_type:normandy,source_subtype:runner",
      "telemetry.uptake.uptake.remotesettings.up_to_date:1|c|#source_type:blocklists,source_subtype:certificates",
      "telemetry.uptake.uptake.remotesettings.success:1|c|#source_type:settings-changes-monitoring",
      "telemetry.uptake.uptake.remotesettings.up_to_date:1|c|#source_type:blocklists,source_subtype:certificates",
      "telemetry.uptake.uptake.remotesettings.success.age:4778|ms|#source_type:settings-changes-monitoring",
      "telemetry.uptake.uptake.remotesettings.success:1|c|#source_type:main,source_subtype:normandy-recipes",
      "telemetry.uptake.uptake.remotesettings.success:1|c|#source_type:settings-changes-monitoring",
      "telemetry.uptake.uptake.remotesettings.success.duration:1401|ms|#source_type:main,source_subtype:normandy-recipes",
      "telemetry.uptake.uptake.remotesettings.success.age:4778|ms|#source_type:settings-changes-monitoring",
      "telemetry.uptake.uptake.remotesettings.success:1|c|#source_type:main,source_subtype:normandy-recipes",
      "telemetry.uptake.uptake.remotesettings.success.duration:1401|ms|#source_type:main,source_subtype:normandy-recipes"
    )

    metrics.map(_.format()).collect should contain theSameElementsAs expected
  }

  it should "handle missing source correctly" in {
    import spark.implicits._

    val pings = TestUtils.generateEventMessages(
      k, customPayload = eventPingUptakeEventsJson(Seq(TestNoSource(), TestNoSource(emptyMap = false)))
    ).map(_.toByteArray).seq

    val pingsDf = spark.createDataset(pings).toDF()
    val metrics = UptakeEventsToDatadog.eventsToMetrics(pingsDf, raiseOnError = true)
    val expected = Seq(
      "telemetry.uptake.uptake.normandy.success:1|c",
      "telemetry.uptake.uptake.normandy.success:1|c",
      "telemetry.uptake.uptake.normandy.success:1|c",
      "telemetry.uptake.uptake.normandy.success:1|c"
    )

    metrics.map(_.format()).collect should contain theSameElementsAs expected
  }
}

object UptakeEvents {
  sealed trait UptakeType {
    val eventObject: String
    val eventStringValue: String
    val eventMap: Option[Map[String, String]]
    override def toString: String = {
      implicit val formats = org.json4s.DefaultFormats
      (s"""[1234, "uptake.remotecontent.result", "$eventObject", "uptake", "$eventStringValue""""
        + eventMap.map(m => s", ${write(m)}").getOrElse("") + "]")
    }
  }
  case class NormandyRecipe(id: Int, status: String = "success") extends UptakeType {
    override val eventObject = "normandy"
    override val eventStringValue = status
    override val eventMap = Some(Map("source" -> s"normandy/recipe/$id"))
  }

  case class NormandyAction(action: String, status: String = "success") extends UptakeType {
    override val eventObject = "normandy"
    override val eventStringValue = status
    override val eventMap = Some(Map("source" -> s"normandy/action/$action"))
  }

  case class NormandyRunner(status: String = "success") extends UptakeType {
    override val eventObject = "normandy"
    override val eventStringValue = status
    override val eventMap = Some(Map("source" -> "normandy/runner"))
  }

  case class RemoteSettings(age: Option[Int], duration: Option[Int], source: String, status: String = "up_to_date")
    extends UptakeType {
    override val eventObject = "remotesettings"
    override val eventStringValue = status
    override val eventMap = Some((
      Seq("source" -> source)
      ++ age.map(a => Seq("age" -> a.toString)).getOrElse(Nil)
      ++ duration.map(d => Seq("duration" -> d.toString)).getOrElse(Nil)
    ).toMap)
  }

  case class TestNoSource(status: String = "success", emptyMap: Boolean = true) extends UptakeType {
    override val eventObject = "normandy"
    override val eventStringValue: String = status
    // If this codebase ever gets upgraded to scala 2.13 (unlikely), we could use Option.when
    override val eventMap = if (emptyMap) Some(Map.empty[String, String]) else None
  }

  def eventPingUptakeEventsJson(uptakeEvents: Seq[UptakeType]): Option[String] = {
    val events = uptakeEvents.map(_.toString).mkString(",")
    Some(
      s"""
         |"reason": "periodic",
         |"processStartTimestamp": 1530291900000,
         |"sessionId": "dd302e9d-569b-4058-b7e8-02b2ff83522c",
         |"subsessionId": "79a2728f-af12-4ed3-b56d-0531a03c2f26",
         |"lostEventsCount": 0,
         |"events": {
         |  "parent": [
         |    $events
         |  ]
         |}
      """.stripMargin)
  }
}
