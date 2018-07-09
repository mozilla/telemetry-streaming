/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.pings

import java.time.OffsetDateTime

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.Ping.{SecondsPerHour, messageToPing}
import com.mozilla.telemetry.pings.main.Processes
import org.json4s.{DefaultFormats, JNothing, JValue, _}

import scala.util.{Success, Try}

case class MainPing(application: Application,
                    clientId: Option[String],
                    // Environment omitted because it's mostly available under meta
                    meta: Meta,
                    payload: MainPingPayload
                   ) extends Ping with HasEnvironment with HasApplication with SendsToAmplitudeWithEnvironment {
  def getCountHistogramValue(histogram_name: String): Option[Int] = {
    try {
      this.meta.`payload.histograms` \ histogram_name \ "values" \ "0" match {
        case JInt(count) => Some(count.toInt)
        case _ => None
      }
    } catch {
      case _: Throwable => None
    }
  }

  def getCountKeyedHistogramValue(histogram_name: String, key: String): Option[Int] = {
    try {
      this.meta.`payload.keyedHistograms` \ histogram_name \ key \ "values" \ "0" match {
        case JInt(count) => Some(count.toInt)
        case _ => None
      }
    } catch {
      case _: Throwable => None
    }
  }

  // Return the number of values greater than threshold
  def histogramThresholdCount(histogramName: String, threshold: Int, processType: String): Long = {
    implicit val formats = org.json4s.DefaultFormats
    val histogram = processType match {
      case "main" => this.meta.`payload.histograms`
      case _ => this.payload.processes \ processType \ "histograms"
    }

    histogram \ histogramName \ "values" match {
      case JNothing => 0
      case v => Try(v.extract[Map[String, Int]]) match {
        case Success(m) =>
          m.filterKeys(s => Try(s.toInt).toOption match {
            case Some(key) => key >= threshold
            case None => false
          }).foldLeft(0)(_ + _._2)
        case _ => 0
      }
    }
  }

  def usageHours: Option[Float] = {
    val max_hours = 25
    val min_hours = 0
    try {
      this.meta.`payload.info` \ "subsessionLength" match {
        case JInt(length) => Some(Math.min(max_hours, Math.max(min_hours, length.toFloat / SecondsPerHour)))
        case _ => None
      }
    } catch {
      case _: Throwable => None
    }
  }

  /*
  * firstPaint is tricky because we only want to know this value if it
  * comes from the first subsession.
  */
  def firstPaint: Option[Int] = {
    this.isFirstSubsession match {
      case Some(true) => this.meta.`payload.simpleMeasurements` \ "firstPaint" match {
        case JInt(value) => Some(value.toInt)
        case _ => None
      }
      case _ => None
    }
  }

  def isFirstSubsession: Option[Boolean] = {
    this.meta.`payload.info` \ "subsessionCounter" match {
      case JInt(v) => Some(v == 1)
      case _ => None
    }
  }

  def sessionId: Option[String] = {
    this.meta.`payload.info` \ "sessionId" match {
      case JString(v) => Some(v)
      case _ => None
    }
  }

  def getNormandyEvents: Seq[Event] = {
    val dynamicProcessEvents = Ping.extractEvents(this.payload.processes, List("dynamic" :: "events" :: Nil))
    dynamicProcessEvents.filter(_.category == "normandy")
  }

  // Make events lazy so we don't extract them in error aggregates, which doesn't use them
  lazy val events: Seq[Event] = Ping.extractEvents(payload.processes, MainPing.eventLocations())

  override def getClientId: Option[String] = meta.clientId

  override def getCreated: Option[Long] = meta.creationTimestamp.map(t => (t / 1e9).toLong)

  def sessionStart: Long = meta.`payload.info` \ "sessionStartDate" match {
    case JString(d) => OffsetDateTime.parse(d).toEpochSecond * 1000
    // sessionStartDate is truncated to the hour anyway, so we just want to get somewhere near the correct timeframe
    case _ => ((meta.Timestamp / 1e9) - events.map(_.timestamp).max).toLong
  }
}

object MainPing {

  def apply(message: Message): MainPing = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List(
      "environment.build",
      "environment.settings",
      "environment.system",
      "environment.profile",
      "environment.addons",
      "environment.experiments",
      "payload.simpleMeasurements",
      "payload.keyedHistograms",
      "payload.histograms",
      "payload.info"
    )

    val ping = messageToPing(message, jsonFieldNames)
    ping.extract[MainPing]
  }

  def eventLocations(prefix: List[String] = List()): List[List[String]] = {
    Processes.names.map(prefix ++ List(_, "events")).toList
  }
}

case class MainPingPayload(processes: JValue)
