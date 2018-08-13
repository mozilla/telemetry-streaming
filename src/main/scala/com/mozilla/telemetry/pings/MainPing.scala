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

  def getScalarValue(processType: String, scalarName: String): Option[Long] = {
    this.payload.processes \ processType \ "scalars" \ scalarName match {
      case JInt(v) => Some(v.toLong)
      case _ => None
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

  def searchCount: Long = meta.`payload.keyedHistograms` \ "SEARCH_COUNTS" match {
    case JObject(hists) =>
      hists
        .filter { case (name, _) =>
          name.split('.').toList match {
            case _ :: source :: _ => MainPing.directSearchSources.contains(source)
            case _ => false
          }
        }
        .foldLeft(0L) { case (sum, (name, hist)) =>
        val count = hist \ "sum" match {
          case JInt(x) => x.toLong
          case _ => 0L
        }
        sum + count
      }
    case _ => 0L
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

  override def sessionSplitEvents: Seq[Event] = {
    val extra: Map[String, String] = Map(
      "subsession_length" -> Some(subsessionLength),
      "active_ticks" -> activeTicks,
      "uri_count" -> getScalarValue("parent", "browser.engagement.total_uri_count"),
      "search_count" -> Some(searchCount),
      "reason" -> reason)
      .flatMap {
        case (k, Some(v)) => Some(k -> v.toString)
        case _ => None
      }
    val event = Event(sessionLength, "meta", "session_split", "", None, Some(extra))
    Seq(event)
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

  def sessionLength: Int = meta.`payload.info` \ "sessionLength" match {
    case JInt(v) => v.toInt
    case _ => 0
  }

  def subsessionLength: Int = meta.`payload.info` \ "subsessionLength" match {
    case JInt(v) => v.toInt
    case _ => 0
  }

  def activeTicks: Option[Long] = {
    // Prefer the scalar over the simpleMeasurement due to bug through at least FF 61;
    // See bug 1482924
    getScalarValue("parent", "browser.engagement.active_ticks")
      .orElse(
        meta.`payload.simpleMeasurements` \ "activeTicks" match {
          case JInt(v) => Some(v.toLong)
          case _ => None
        })
  }

  def reason: Option[String] = meta.`payload.info` \ "reason" match {
    case JString(v) => Some(v)
    case _ => None
  }

}

object MainPing {

  val directSearchSources = Set("urlbar", "searchbar", "newtab", "abouthome", "contextmenu", "system")

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
