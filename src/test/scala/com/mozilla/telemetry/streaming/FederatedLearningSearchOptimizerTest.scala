/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import com.holdenkarau.spark.testing.StructuredStreamingBase
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConversions._

class FederatedLearningSearchOptimizerTest extends FlatSpec with Matchers with GivenWhenThen with StructuredStreamingBase with BeforeAndAfterEach {

  val OutputPath = "/tmp/output"
  val CheckpointPath = "/tmp/state-checkpoint"

  "Federated learning Optimizer" should "aggregate pings" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10)
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're aggregated")
    val query = FederatedLearningSearchOptimizer.aggregate(pingsStream.toDF())
      .writeStream.format("memory").queryName("updates").start()
    pingsStream.addData(pings)
    query.processAllAvailable()

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5,
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

  it should "optimize weight updates and save model" in {
    import spark.implicits._

    Given("set of frecency update pings")
    val messages = TestUtils.generateFrecencyUpdateMessages(10)
    val pings = messages.map(_.toByteArray)
    val pingsStream = MemoryStream[Array[Byte]]

    When("they're processed")
    val query = FederatedLearningSearchOptimizer.optimize(pingsStream.toDF(), OutputPath, CheckpointPath)
    pingsStream.addData(pings)
    query.processAllAvailable()

    pingsStream.addData(TestUtils.generateFrecencyUpdateMessages(5,
      timestamp = Some(TestUtils.testTimestampNano + TimeUnit.MINUTES.toNanos(45))).map(_.toByteArray).seq)
    query.processAllAvailable()
    pingsStream.addData(Array[Byte]())
    query.processAllAvailable()
    query.stop()

    Then("model is saved to configured location")
    Files.readAllLines(Paths.get(OutputPath + "/latest.json")).mkString("") should startWith("""{"model":[""")
    Files.readAllLines(Paths.get(OutputPath + "/1.json")).mkString("") should startWith("""{"model":[""")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanupTestDirectories()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanupTestDirectories()
  }

  private def cleanupTestDirectories(): Unit = {
    FileUtils.deleteDirectory(new File(OutputPath))
    FileUtils.deleteDirectory(new File(CheckpointPath))
  }
}
