/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.streaming.EnrollmentEvents.{ExperimentA, ExperimentB, enrollmentEventJson,
  eventPingEnrollmentEventsJson}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
class ExperimentEnrollmentsAggregatorTest extends FlatSpec with Matchers with GivenWhenThen with DataFrameSuiteBase {

  val k = 10 //
  val checkedColumns = Array("window_start", "window_end", "object", "experiment_id", "branch_id", "enroll_count", "unenroll_count")
  val ExpectedWindowStart = new Timestamp(LocalDateTime.parse("2016-04-07T13:35:00").toInstant(ZoneOffset.UTC).toEpochMilli)
  val ExpectedWindowEnd = new Timestamp(LocalDateTime.parse("2016-04-07T13:40:00").toInstant(ZoneOffset.UTC).toEpochMilli)

  override def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    def prepare(df: DataFrame) =
      df.select(checkedColumns.head, checkedColumns.tail: _*)
        .orderBy("window_start", "experiment_id", "branch_id")

    super.assertDataFrameEquals(prepare(expected), prepare(result))
  }

  def prepareExpectedAggregate(rows: (String, String, Long, Long)*): DataFrame = {
    import spark.implicits._
    spark.sparkContext.parallelize(List[(Timestamp, Timestamp, String, String, String, Long, Long)](
      rows.map(r => (ExpectedWindowStart, ExpectedWindowEnd, "preference_study", r._1, r._2, r._3, r._4)): _*
    )).toDF(checkedColumns: _*)
  }

  "Experiment Enrollment Aggregator" should "aggregate enrollment events" in {
    import spark.implicits._

    Given("set of main pings with experiment enrollment events")
    val mainPings = (
      TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = true))
        ++ TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = false))
        ++ TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentB, Some("one"), enroll = true))
      ).map(_.toByteArray).seq
    val pingsDf = spark.createDataset(mainPings).toDF()

    When("pings are aggregated")
    val aggregates = ExperimentEnrollmentsAggregator.aggregate(pingsDf)

    Then("resulting aggregate has expected schema")
    val expectedSchema = StructType(List(
      StructField("window_start", TimestampType, nullable = true),
      StructField("window_end", TimestampType, nullable = true),
      StructField("experiment_id", StringType, nullable = true),
      StructField("branch_id", StringType, nullable = true),
      StructField("object", StringType, nullable = true),
      StructField("enroll_count", LongType, nullable = false),
      StructField("unenroll_count", LongType, nullable = false),
      StructField("submission_date_s3", StringType, nullable = true)
    ))
    aggregates.schema.fields should contain theSameElementsAs expectedSchema.fields

    And("events are aggregated by experiment name and branch")
    val expected = prepareExpectedAggregate((ExperimentA, "six", k, k), (ExperimentB, "one", k, 0))
    assertDataFrameEquals(aggregates, expected)
  }

  it should "handle unenroll events without experiment branch" in {
    import spark.implicits._

    Given("set of main pings with experiment enrollment events and some unenroll events without the branch")
    val mainPings = (
      TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = true))
        ++ TestUtils.generateMainMessages(k / 2, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = false))
        ++ TestUtils.generateMainMessages(k / 2, customPayload = enrollmentEventJson(ExperimentA, None, enroll = false))
      ).map(_.toByteArray).seq
    val pingsDf = spark.createDataset(mainPings).toDF()

    When("pings are aggregated")
    val aggregates = ExperimentEnrollmentsAggregator.aggregate(pingsDf)

    Then("events are aggregated, there is an aggregate with empty branch")
    val expected = prepareExpectedAggregate((ExperimentA, "six", k, k / 2), (ExperimentA, null, 0, k / 2))
    assertDataFrameEquals(aggregates, expected)
  }

  it should "ignore non -main and -event pings and main pings without events" in {
    import spark.implicits._

    Given("a main ping with experiment enrollment events, main pings without events and a crash ping")
    val mainPings = (
      TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = true))
        ++ TestUtils.generateMainMessages(k, customPayload = Some(""" "dynamic": {"events": []} """))
        ++ TestUtils.generateMainMessages(k)
        ++ TestUtils.generateCrashMessages(k)
      ).map(_.toByteArray).seq
    val pingsDf = spark.createDataset(mainPings).toDF()

    When("pings are aggregated")
    val aggregates = ExperimentEnrollmentsAggregator.aggregate(pingsDf)

    Then("resulting aggregate contains data from events with events")
    val expected = prepareExpectedAggregate((ExperimentA, "six", k, 0))
    assertDataFrameEquals(aggregates, expected)
  }

  it should "read enrollment events from event pings" in {
    import spark.implicits._

    Given("set of main and event pings with experiment enrollment events")
    val mainPings = (
      TestUtils.generateMainMessages(k, customPayload = enrollmentEventJson(ExperimentA, Some("six"), enroll = true))
        ++ TestUtils.generateEventMessages(k)
      ).map(_.toByteArray).seq
    val pingsDf = spark.createDataset(mainPings).toDF()

    When("pings are aggregated")
    val aggregates = ExperimentEnrollmentsAggregator.aggregate(pingsDf)

    And("events are aggregated by experiment name and branch")
    val expected = prepareExpectedAggregate((ExperimentA, "six", k, 0), ("awesome-experiment", "control", k, 0))
    assertDataFrameEquals(aggregates, expected)
  }

  it should "read enrollment events from all processes (bug 1538793)" in {
    import spark.implicits._

    Given("event pings with experiment enrollment events in different processes")
    val pings = (
        TestUtils.generateEventMessages(k, customPayload=eventPingEnrollmentEventsJson(ExperimentA, Some("six"),
          enroll = true, process="dynamic"))
          ++ TestUtils.generateEventMessages(k, customPayload=eventPingEnrollmentEventsJson(ExperimentA, Some("six"),
          enroll = true, process="parent"))
          ++ TestUtils.generateEventMessages(k, customPayload=eventPingEnrollmentEventsJson(ExperimentA, Some("six"),
          enroll = true, process="extension"))
      ).map(_.toByteArray).seq
    val pingsDf = spark.createDataset(pings).toDF()

    When("pings are aggregated")
    val aggregates = ExperimentEnrollmentsAggregator.aggregate(pingsDf)

    And("events are aggregated by experiment name and branch")
    val expected = prepareExpectedAggregate((ExperimentA, "six", k * 3, 0))
    assertDataFrameEquals(aggregates, expected)

  }
}

object EnrollmentEvents {
  val ExperimentA = "pref-flip-timer-speed-up-60-1443940"
  val ExperimentB = "pref-flip-search-composition-57-release-1413565"

  def enrollmentEventJson(experiment_id: String, experimentBranch: Option[String], enroll: Boolean): Option[String] = {
    val branchKv = experimentBranch.map(b => s""" "branch": "$b" """).getOrElse("")
    Some(
      s"""
         |"processes": {
         |  "dynamic": {
         |    "events": [
         |      [554879, "normandy", "${if (enroll) "enroll" else "unenroll"}", "preference_study", "$experiment_id", {$branchKv}]
         |    ]
         |  }
         |}
      """.stripMargin)
  }

  def eventPingEnrollmentEventsJson(experiment_id: String, experimentBranch: Option[String], enroll: Boolean,
                                     process: String = "dynamic"): Option[String] = {
    val branchKv = experimentBranch.map(b => s""" "branch": "$b" """).getOrElse("")
    Some(
      s"""
         |"reason": "periodic",
         |"processStartTimestamp": 1530291900000,
         |"sessionId": "dd302e9d-569b-4058-b7e8-02b2ff83522c",
         |"subsessionId": "79a2728f-af12-4ed3-b56d-0531a03c2f26",
         |"lostEventsCount": 0,
         |"events": {
         |  "$process": [
         |    [554879, "normandy", "${if (enroll) "enroll" else "unenroll"}", "preference_study", "$experiment_id", {$branchKv}]
         |  ]
         |}
      """.stripMargin)
  }
}
