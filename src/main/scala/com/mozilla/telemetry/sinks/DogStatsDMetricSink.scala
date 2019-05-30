/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import org.apache.spark.sql.ForeachWriter
import java.net.{InetSocketAddress, DatagramPacket, DatagramSocket}
import com.mozilla.telemetry.monitoring.DogStatsDMetric


class DogStatsDMetricSink(host: String, port: Int, sampleRate: Option[Double] = None)
  extends ForeachWriter[DogStatsDMetric] {
  private val address = new InetSocketAddress(host, port)
  private var socket: DatagramSocket = null

  def open(partitionId: Long, version: Long): Boolean = {
    socket = new DatagramSocket()
    true
  }

  def process(value: DogStatsDMetric): Unit = {
    val bytes = value.format(sampleRate).getBytes
    val datagram = new DatagramPacket(bytes, bytes.length, address)
    socket.send(datagram)
  }

  def close(errorOrNull: Throwable): Unit = {
    socket.close()
  }
}
