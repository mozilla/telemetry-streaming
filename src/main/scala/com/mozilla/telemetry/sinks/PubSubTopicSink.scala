/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.sinks

import java.util.concurrent.{Executor, Executors}

import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.api.gax.batching.BatchingSettings
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.apache.spark.sql.ForeachWriter
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.threeten.bp.Duration

case class GCPMapValue(key: String, value: String)

object GCPMapValue {
  def mapToGCP(m: Map[String, String]): Seq[GCPMapValue] = m.map { case (k, v) => GCPMapValue(k, v) }.toSeq
}


class PubSubTopicSink[T <: (Product with Serializable with AnyRef)](projectName: String, topicId: String) extends ForeachWriter[T] {
  @transient lazy val logger = org.apache.log4j.LogManager.getLogger("HttpSink")
  val RequestBytesThreshold = 5000L
  val MessageCountBatchSize = 10L
  val PublishDelayThreshold = Duration.ofMillis(1000)
  protected[sinks] var publisher: Publisher = null
  @transient lazy val executor: Executor = Executors.newCachedThreadPool()

  override def open(partitionId: Long, version: Long): Boolean = {
    val batchingSettings = BatchingSettings.newBuilder
      .setElementCountThreshold(MessageCountBatchSize)
      .setRequestByteThreshold(RequestBytesThreshold)
      .setDelayThreshold(PublishDelayThreshold)
      .build()

    publisher = Publisher.newBuilder(topicId).setBatchingSettings(batchingSettings).build

    true
  }

  def process(value: T): Unit = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val data = ByteString.copyFromUtf8(write(value))
    val msg = PubsubMessage.newBuilder.setData(data).build
    val future = publisher.publish(msg)
    // addCallback is marked as deprecated but the latest google cloud client library docs use this in their examples
    // and I literally cannot find a later version of the core docs
    ApiFutures.addCallback(future, getCallback, executor)
  }

  private def getCallback(): ApiFutureCallback[String] = {
    new ApiFutureCallback[String] {
      override def onFailure(t: Throwable): Unit = logger.warn(s"Message failed to send: $t")

      override def onSuccess(result: String): Unit = logger.info(s"Message sent: $result")
    }
  }

  override def close(errorOrNull: Throwable): Unit = {}
}

