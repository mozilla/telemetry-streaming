/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.util

import java.io.Serializable
import java.time.{Clock, Instant, ZoneId}

class ManualClock(var instant: Instant, val zone: ZoneId) extends Clock with Serializable {

  private val initialInstant = instant

  override def getZone: ZoneId = zone

  override def withZone(zone: ZoneId): Clock = {
    if (zone == this.zone) {
      this
    } else {
      Clock.fixed(instant, zone)
    }
  }

  override def millis: Long = instant.toEpochMilli

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: ManualClock =>
        instant == other.instant && zone == other.zone
      case _ =>
        false
    }
  }

  override def hashCode: Int = instant.hashCode ^ zone.hashCode

  override def toString: String = "ManualClock[" + instant + "," + zone + "]"

  def advance(millis: Long): Unit = {
    instant = instant.plusMillis(millis)
  }

  def reset(): Unit = {
    instant = initialInstant
  }
}
