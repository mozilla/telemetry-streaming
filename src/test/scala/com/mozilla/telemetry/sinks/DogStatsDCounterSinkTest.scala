/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import java.net.{DatagramPacket, DatagramSocket}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.monitoring.DogStatsDCounter
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

case class UDPReceiver(bufferLength: Int = 1024) {
  private val socket = new DatagramSocket()
  val port = socket.getLocalPort()

  def receiveData(numPackets: Int): Seq[String] = for {
    _ <- 0 until numPackets
    buffer = new Array[Byte](bufferLength)
    packet = new DatagramPacket(buffer, buffer.length)
  } yield {
    socket.receive(packet)
    new String(packet.getData, 0, packet.getLength)
  }
}

class DogStatsDCounterSinkTest extends FlatSpec with DataFrameSuiteBase with Matchers {
  "DogStatsDCounterSink" should "produce a properly formatted minimal datagram string" in {
    implicit def executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    import spark.implicits._
    val input = MemoryStream[DogStatsDCounter]

    val receiver = UDPReceiver()
    val f = Future[Seq[String]] {
      receiver.receiveData(4)
    }

    val sink = new DogStatsDCounterSink("localhost", receiver.port, Some(0.1))
    val query = input.toDS()
      .writeStream
      .queryName("DogStatsDCounterSinkTest")
      .foreach(sink)
      .start()

    input.addData(DogStatsDCounter("test.sink"))
    input.addData(DogStatsDCounter("test.sink", kvTags = Some(Map("hello" -> "world"))))
    input.addData(DogStatsDCounter("test.sink", bareTags = Some(Seq("what", "is:new"))))
    input.addData(DogStatsDCounter("test.sink", metricValue = 2))

    query.processAllAvailable()

    val test = f.map { actual =>
      actual should contain theSameElementsAs Seq(
        "test.sink:1|c|@0.1",
        "test.sink:1|c|@0.1|#hello:world",
        "test.sink:1|c|@0.1|#what,is_new",
        "test.sink:2|c|@0.1"
      )
    }

    Await.result(test, 5 seconds)
  }

}
