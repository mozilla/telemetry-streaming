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
        |    ]}
        |}
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
    payload.model_version shouldBe 3
    payload.study_variation shouldBe "treatment"
    payload.loss shouldBe 291989.21
    payload.num_chars_typed shouldBe 5
    payload.num_suggestions_displayed shouldBe 6
    payload.update should contain theSameElementsAs Array(1.2, 3.2, -3.1, 4.4, 0.5, 0.234, -0.98, 0.33, 0.34, 0.28,
      0.302, 0.4, -0.8, 0.25, 0.9, -0.8, 0.29, 0.42, 0.89, 0.39, 0.54, 0.78)
  }
}
