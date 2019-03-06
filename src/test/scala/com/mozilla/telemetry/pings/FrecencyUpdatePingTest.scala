/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.RichMessage
import org.scalatest.{FlatSpec, Matchers}

class FrecencyUpdatePingTest extends FlatSpec with Matchers {
  "Frecency update ping" should "be properly deserialized" in {
    val rawPing =
      """{
       |  "clientId": "e00d072c-db5c-42be-a159-a2e95466a7da",
       |  "type": "frecency-update",
       |  "payload": {
       |    "model_version": 140,
       |    "frecency_scores": [38223, 3933.4, 304933.3, 21],
       |    "loss": 291989.21,
       |    "update": [
       |    1.2,
       |    3.2,
       |    -3.1,
       |    4.4,
       |    0.5,
       |    0.234,
       |    -0.98,
       |    0.33,
       |    0.34,
       |    0.28,
       |    0.302,
       |    0.4,
       |    -0.8,
       |    0.25,
       |    0.9,
       |    -0.8,
       |    0.29,
       |    0.42,
       |    0.89,
       |    0.39,
       |    0.54,
       |    0.78
       |    ],
       |    "num_suggestions_displayed": 1,
       |    "rank_selected": 0,
       |    "bookmark_and_history_num_suggestions_displayed": 1,
       |    "bookmark_and_history_rank_selected": 0,
       |    "num_key_down_events_at_selecteds_first_entry": 8,
       |    "num_key_down_events": 14,
       |    "time_start_interaction": 0,
       |    "time_end_interaction": 2275,
       |    "time_at_selecteds_first_entry": 1458,
       |    "search_string_length": 13,
       |    "selected_style": "autofill heuristic",
       |    "selected_url_was_same_as_search_string": 0,
       |    "enter_was_pressed": 1,
       |    "study_variation": "training",
       |    "study_addon_version": "2.1.1"
       }}
      """.stripMargin

    val fields = Map(
      "appBuildId" -> "62.0",
      "appVersion" -> "62.0",
      "appName" -> "Firefox",
      "geoCountry" -> "US",
      "normalizedChannel" -> "beta",
      "submissionDate" -> "20180701"
    )

    val message = RichMessage("0", fields, Some(rawPing))
    val parsedPing = FrecencyUpdatePing(message)
    val payload = parsedPing.payload

    payload.model_version shouldBe 140
    payload.frecency_scores should contain theSameElementsAs Array(38223.0, 3933.4, 304933.3, 21.0)
    payload.loss shouldBe 291989.21
    payload.update should contain theSameElementsAs Array(1.2, 3.2, -3.1, 4.4, 0.5, 0.234, -0.98, 0.33, 0.34, 0.28, 0.302, 0.4, -0.8, 0.25, 0.9, -0.8, 0.29, 0.42, 0.89, 0.39, 0.54, 0.78)
    payload.num_suggestions_displayed shouldBe 1
    payload.rank_selected shouldBe 0
    payload.bookmark_and_history_num_suggestions_displayed shouldBe 1
    payload.bookmark_and_history_rank_selected shouldBe 0
    payload.num_key_down_events_at_selecteds_first_entry shouldBe 8
    payload.num_key_down_events shouldBe 14
    payload.time_start_interaction shouldBe 0
    payload.time_end_interaction shouldBe 2275
    payload.time_at_selecteds_first_entry shouldBe 1458
    payload.search_string_length shouldBe 13
    payload.selected_style shouldBe "autofill heuristic"
    payload.selected_url_was_same_as_search_string shouldBe 0
    payload.enter_was_pressed shouldBe 1
    payload.study_variation shouldBe "training"
    payload.study_addon_version shouldBe "2.1.1"
  }
}
