/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.mozilla.telemetry.StackTraceUtils
import com.mozilla.telemetry.sinks.BatchHttpSink
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class CrashesToInfluxTest extends FlatSpec with Matchers with BeforeAndAfterEach with DataFrameSuiteBase {

  private val host = "localhost"
  private val port = 9876
  private val path = "/crashes"

  private val wireMockServer = new WireMockServer(wireMockConfig().port(port))

  private val defaultMeasurementName = "crashes"

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    wireMockServer.stop()
  }

  override def beforeEach(): Unit = {
    WireMock.configureFor(host, port)
    stubFor(post(urlMatching(path)).willReturn(aResponse().withStatus(200)))
  }

  override def afterEach(): Unit = {
    WireMock.reset()
  }

  "Parse ping" should "create a crash to send via HTTP" in {
    val k = 23

    val httpSink = new BatchHttpSink(s"http://$host:$port$path")
    val crashes = TestUtils.generateCrashMessages(k)

    crashes
      .flatMap(m => CrashesToInflux.parsePing(m, CrashesToInflux.defaultChannels,
        CrashesToInflux.defaultAppNames, defaultMeasurementName,
        getSignature = false, usingDatabricks = false))
      .foreach(httpSink.process)

    verify(k, postRequestedFor(urlMatching(path)))
  }

  // crash signatures require python library so ignore in CI
  ignore should "get crash signature" in {
    val k1 = 4

    val httpSink = new BatchHttpSink(s"http://$host:$port$path")
    val crashes = TestUtils.generateCrashMessages(k1, customPayload = Some(StackTraceUtils.sampleStackTrace))

    val crashStrings = crashes
      .flatMap(m => CrashesToInflux.parsePing(m, CrashesToInflux.defaultChannels,
        CrashesToInflux.defaultAppNames, defaultMeasurementName,
        getSignature = true, usingDatabricks = false))

    crashStrings.foreach(httpSink.process)

    crashStrings.count(_.contains("crashSignature")) should be (k1)

    verify(k1, postRequestedFor(urlMatching(path)))
  }

  "Message parser" should "generate 1 crash to send for each valid crash ping" in {
    import spark.implicits._

    val k = 12
    val messages = TestUtils.generateCrashMessages(k).map(_.toByteArray).seq
    val parsedPings = CrashesToInflux.getParsedPings(spark.sqlContext.createDataset(messages).toDF,
      raiseOnError = true, defaultMeasurementName)

    parsedPings.count() should be (k)
  }

  it should "ignore pings that are not crashes" in {
    import spark.implicits._

    val k = 10
    val messages = TestUtils.generateMainMessages(k).map(_.toByteArray).seq
    val parsedPings = CrashesToInflux.getParsedPings(spark.sqlContext.createDataset(messages).toDF,
      raiseOnError = true, defaultMeasurementName)

    parsedPings.count() should be (0)
  }

  it should "ignore pings that are not from the given list of channels" in {
    import spark.implicits._

    val k = 5
    val messages = TestUtils.generateCrashMessages(k).map(_.toByteArray).seq
    val parsedPings = CrashesToInflux.getParsedPings(spark.sqlContext.createDataset(messages).toDF,
      raiseOnError = true, defaultMeasurementName, List[String]("non-channel"))

    parsedPings.count() should be (0)
  }

  it should "create strings that start with measurement name" in {
    import spark.implicits._

    val k = 5
    val messages = TestUtils.generateCrashMessages(k).map(_.toByteArray).seq
    val parsedPings = CrashesToInflux.getParsedPings(spark.sqlContext.createDataset(messages).toDF,
      raiseOnError = true,  defaultMeasurementName)
    parsedPings.collect().count(_.startsWith(defaultMeasurementName)) should be (k)
  }

  "Crashes to influx" should "stream crash pings via HTTP" taggedAs Kafka.DockerComposeTag in {
    Kafka.createTopic(StreamingJobBase.TelemetryKafkaTopic)
    val kafkaProducer = Kafka.makeProducer(StreamingJobBase.TelemetryKafkaTopic)

    def send(rs: Seq[Array[Byte]]): Unit = {
      rs.foreach{ kafkaProducer.send(_, synchronous = true) }
    }

    val k = 56

    val messages = TestUtils.generateCrashMessages(k).map(_.toByteArray)

    val listener = new StreamingQueryListener {
      var messagesSeen = 0L
      var sentMessages = false

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        messagesSeen += event.progress.numInputRows
        if(!sentMessages){
          send(messages)
          sentMessages = true
        }

        if(messagesSeen >= k){
          spark.streams.active.foreach(_.processAllAvailable)
          spark.streams.active.foreach(_.stop)
        }
      }
    }

    spark.streams.addListener(listener)

    val args = Array(
      "--kafkaBroker", Kafka.kafkaBrokers,
      "--url", s"http://$host:$port$path",
      "--measurementName", defaultMeasurementName)

    CrashesToInflux.main(args)

    kafkaProducer.close
    spark.streams.removeListener(listener)

    verify(k, postRequestedFor(urlMatching(path)))
  }
}
