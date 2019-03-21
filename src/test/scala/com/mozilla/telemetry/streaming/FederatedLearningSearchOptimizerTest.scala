/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.io.File
import java.time.{LocalDate, ZoneId}
import java.util.concurrent.TimeUnit

import com.holdenkarau.spark.testing.StructuredStreamingBase
import com.mozilla.telemetry.util.{ManualClock, S3TestUtil}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec, GivenWhenThen, Matchers}

class FederatedLearningSearchOptimizerTest extends FlatSpec with Matchers with GivenWhenThen with StructuredStreamingBase with BeforeAndAfterEach {

  val OutputBucket = "test-bucket"
  val OutputKey = "model"
  val CheckpointPath = "/tmp/state-checkpoint"
  val MockEndpointPort = 8001
  val MockEndpoint = s"http://localhost:$MockEndpointPort"

  val clock = new ManualClock(
    LocalDate.of(2018, 4, 5).atStartOfDay(ZoneId.of("UTC")).toInstant,
    ZoneId.of("UTC")
  )

  "Federated learning Optimizer" should "aggregate pings" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, 140)
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF(), clock, 28)
      .writeStream.format("memory").option("checkpointLocation", CheckpointPath + "/spark").queryName("updates").start()

    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("a set of aggregates is produced")
    val res = spark.sql("select * from updates").as[FrecencyUpdateAggregate]

    res.show(false)
    res.count shouldBe 1
  }

  it should "ignore the control branch" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, -1, variation="control")
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF(), clock, 28)
      .writeStream.format("memory").option("checkpointLocation", CheckpointPath + "/spark").queryName("updates").start()
    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("a set of aggregates is not produced")
    val res = spark.sql("select * from updates").as[FrecencyUpdateAggregate]

    res.show(false)
    res.count shouldBe 0
  }

  it should "aggregate dogfooding branch" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, 140, variation="dogfooding")
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF(), clock, 28)
      .writeStream.format("memory").option("checkpointLocation", CheckpointPath + "/spark").queryName("updates").start()
    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("a set of aggregates is not produced")
    val res = spark.sql("select * from updates").as[FrecencyUpdateAggregate]

    res.show(false)
    res.count shouldBe 1
  }

  it should "aggregate dogfooding-crazy branch" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, 140, variation="dogfooding-crazy")
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF(), clock, 28)
      .writeStream.format("memory").option("checkpointLocation", CheckpointPath + "/spark").queryName("updates").start()
    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("a set of aggregates is not produced")
    val res = spark.sql("select * from updates").as[FrecencyUpdateAggregate]

    res.show(false)
    res.count shouldBe 1
  }

  it should "aggregate non-dogfooding-training branch" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, -1, variation="non-dogfooding-training")
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF(), clock, 28)
      .writeStream.format("memory").option("checkpointLocation", CheckpointPath + "/spark").queryName("updates").start()
    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("a set of aggregates is not produced")
    val res = spark.sql("select * from updates").as[FrecencyUpdateAggregate]

    res.show(false)
    res.count shouldBe 1
  }

  it should "ignore non-dogfooding-validation branch" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, 140, variation="non-dogfooding-validation")
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF(), clock, 28)
      .writeStream.format("memory").option("checkpointLocation", CheckpointPath + "/spark").queryName("updates").start()
    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("a set of aggregates is not produced")
    val res = spark.sql("select * from updates").as[FrecencyUpdateAggregate]

    res.show(false)
    res.count shouldBe 0
  }

  it should "aggregate non-dogfooding-crazy-training branch" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, -1, variation="non-dogfooding-crazy-training")
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF(), clock, 28)
      .writeStream.format("memory").option("checkpointLocation", CheckpointPath + "/spark").queryName("updates").start()
    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("a set of aggregates is not produced")
    val res = spark.sql("select * from updates").as[FrecencyUpdateAggregate]

    res.show(false)
    res.count shouldBe 1
  }

  it should "ignore non-dogfooding-crazy-validation branch" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, 140, variation="non-dogfooding-crazy-validation")
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF(), clock, 28)
      .writeStream.format("memory").option("checkpointLocation", CheckpointPath + "/spark").queryName("updates").start()
    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("a set of aggregates is not produced")
    val res = spark.sql("select * from updates").as[FrecencyUpdateAggregate]

    res.show(false)
    res.count shouldBe 0
  }

  it should "optimize weight updates and save model" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10, 140)
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're processed")

    val s3 = S3TestUtil(MockEndpointPort, Some(OutputBucket))
    val query = FederatedLearningSearchOptimizer.optimize(pingsStream.toDF(), CheckpointPath + "/spark", OutputBucket,
      OutputKey, CheckpointPath, None, clock, 28, s3EndpointOverride = Some(MockEndpoint))
    pingsStream.addData(pings)
    query.processAllAvailable()

    clock.advance(TimeUnit.MINUTES.toNanos(45))

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5, 140,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("model is saved to configured location")
    s3.getObjectAsString(OutputBucket, OutputKey + "/latest.json") should startWith("""{"model":[""")
    s3.getObjectAsString(OutputBucket, OutputKey + "/1.json") should startWith("""{"model":[""")
  }


  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanupTestDirectories()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanupTestDirectories()
    clock.reset()
  }

  private def cleanupTestDirectories(): Unit = {
    FileUtils.deleteDirectory(new File(CheckpointPath))
  }
}
