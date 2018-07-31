/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import com.mozilla.telemetry.monitoring.{DogStatsDMetric, DogStatsDCounter}
import org.apache.spark.sql.ForeachWriter
import java.net.{InetSocketAddress, DatagramPacket, DatagramSocket}


sealed abstract class DogStatsDMetricSink[T <: DogStatsDMetric](host: String, port: Int, sampleRate: Option[Double] = None) extends ForeachWriter[T] {
  private val address = new InetSocketAddress(host, port)
  private var socket: DatagramSocket = null

  def open(partitionId: Long, version: Long): Boolean = {
    socket = new DatagramSocket()
    true
  }

  def process(value: T): Unit = {
    val bytes = value.format(sampleRate).getBytes
    val datagram = new DatagramPacket(bytes, bytes.length, address)
    socket.send(datagram)
  }

  def close(errorOrNull: Throwable): Unit = {
    socket.close()
  }
}

class DogStatsDCounterSink(host: String, port: Int, sampleRate: Option[Double] = None) extends DogStatsDMetricSink[DogStatsDCounter](host, port, sampleRate)
