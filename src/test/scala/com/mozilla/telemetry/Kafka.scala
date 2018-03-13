// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.Tag

object Kafka {

  object DockerComposeTag extends Tag("DockerComposeTag")

  val zkHostInfo = "localhost:2181"
  val kafkaTopicPartitions = 1
  val kafkaBrokers = "localhost:9092"

  def topicExists(zkUtils: ZkUtils, topic: String): Boolean = {
    // taken from
    // https://github.com/apache/spark/blob/master/external/kafka-0-10-sql +
    // src/test/scala/org/apache/spark/sql/kafka010/KafkaTestUtils.scala#L350
    zkUtils.getAllTopics().contains(topic)
  }

  def createTopic(topic: String, numPartitions: Int = kafkaTopicPartitions): Unit = {
    val timeoutMs = 10000
    val isSecureKafkaCluster = false
    val replicationFactor = 1
    val topicConfig = new Properties
    val zkUtils = ZkUtils.apply(zkHostInfo, timeoutMs, timeoutMs, isSecureKafkaCluster)

    if(!topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, topicConfig)
    }

    zkUtils.close()
  }

  class InternalKafkaProducer(topic: String) {
    private val conf = new Properties()
    conf.put("bootstrap.servers", Kafka.kafkaBrokers)
    conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    conf.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    private val kafkaProducer = new KafkaProducer[String, Array[Byte]](conf)

    def send(message: Array[Byte], synchronous: Boolean = false): Unit = {
      val record = new ProducerRecord[String, Array[Byte]](topic, message)
      kafkaProducer.send(record)
      if (synchronous) {
        kafkaProducer.flush()
      }
    }

    def close: Unit = {
      kafkaProducer.close
    }
  }

  def makeProducer(topic: String): InternalKafkaProducer = {
    new InternalKafkaProducer(topic)
  }
}
