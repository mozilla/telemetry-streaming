/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import java.sql.Timestamp

import com.google.api.core.{ApiFutureCallback, ApiFutures}
import org.apache.spark.sql.ForeachWriter
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.Serialization
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.PubsubMessage
import com.google.protobuf.ByteString
import com.google.api.gax.batching.BatchingSettings
import org.threeten.bp.Duration

case class GCPMapValue(key: String, value: String)
object GCPMapValue {
  def mapToGCP(m: Map[String, String]): Seq[GCPMapValue] = m.map { case (k, v) => GCPMapValue(k, v) }.toSeq
}



class PubSubTopicSink[T  <: (Product with Serializable)](projectName: String, topicId: String) extends ForeachWriter[T] {
  val RequestBytesThreshold = 5000L
  val MessageCountBatchSize = 10L
  val PublishDelayThreshold = Duration.ofMillis(1000)

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger("HttpSink")

  protected[streaming] var publisher: Publisher = null

  override def open(partitionId: Long, version: Long): Boolean = {
    val batchingSettings = BatchingSettings.newBuilder
      .setElementCountThreshold(MessageCountBatchSize)
      .setRequestByteThreshold(RequestBytesThreshold)
      .setDelayThreshold(PublishDelayThreshold)
      .build()

    publisher = Publisher.newBuilder(topicId).setBatchingSettings(batchingSettings).build

    true
  }

  private def getCallback(): ApiFutureCallback[String] = {
    new ApiFutureCallback[String] {
      override def onFailure(t: Throwable): Unit = logger.warn(s"Message failed to send: $t")
      override def onSuccess(result: String): Unit = logger.info(s"Message sent: $result")
    }
  }

  def process(value: T): Unit = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val data = ByteString.copyFromUtf8(write(value))
    val msg = PubsubMessage.newBuilder.setData(data).build
    val future = publisher.publish(msg)
    // addCallback is marked as deprecated but the latest google cloud client library docs use this in their examples
    // and I literally cannot find a later version of the core docs
    ApiFutures.addCallback(future, getCallback)
  }

  override def close(errorOrNull: Throwable): Unit = {}
}

