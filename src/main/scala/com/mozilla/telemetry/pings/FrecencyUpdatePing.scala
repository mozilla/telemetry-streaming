/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.Ping.messageToPing
import org.json4s.DefaultFormats

case class FrecencyUpdatePing(meta: Meta,
                              payload: FrecencyUpdatePayload)

object FrecencyUpdatePing {
  def apply(message: Message): FrecencyUpdatePing = {
    implicit val formats = DefaultFormats
    val ping = messageToPing(message)
    ping.extract[FrecencyUpdatePing]
  }
}

case class FrecencyUpdatePayload(model_version: Long,
                                 study_variation: String,
                                 update: Array[Double],
                                 loss: Double,
                                 num_chars_typed: Long,
                                 rank_selected: Long,
                                 num_suggestions_displayed: Long)



