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
                                 frecency_scores: Array[Double],
                                 loss: Double,
                                 update: Array[Double],
                                 num_suggestions_displayed: Long,
                                 rank_selected: Long,
                                 bookmark_and_history_num_suggestions_displayed: Long,
                                 bookmark_and_history_rank_selected: Long,
                                 num_key_down_events_at_selecteds_first_entry: Long,
                                 num_key_down_events: Long,
                                 time_start_interaction: Long,
                                 time_end_interaction: Long,
                                 time_at_selecteds_first_entry: Long,
                                 search_string_length: Long,
                                 selected_style: String,
                                 selected_url_was_same_as_search_string: Long,
                                 enter_was_pressed: Long,
                                 study_variation: String,
                                 study_addon_version: String)
