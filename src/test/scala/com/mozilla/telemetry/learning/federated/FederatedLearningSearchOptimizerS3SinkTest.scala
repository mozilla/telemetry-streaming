/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.learning.federated

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors

import com.holdenkarau.spark.testing.StructuredStreamingBase
import com.mozilla.telemetry.util.S3TestUtil
import org.apache.commons.io.FileUtils
import org.scalatest._

import scala.collection.JavaConversions._

class FederatedLearningSearchOptimizerS3SinkTest extends FlatSpec with Matchers with GivenWhenThen with StructuredStreamingBase with BeforeAndAfterEach {
  val OutputBucket = "test-bucket"
  val OutputKey = "model"
  val CheckpointPath = "/tmp/state-checkpoint"
  val MockEndpointPort = 8001
  val MockEndpoint = s"http://localhost:$MockEndpointPort"

  "Federated Learning Sink" should "initialize optimisation state if checkpoint directory is empty" in {
    val sink = new FederatedLearningSearchOptimizerS3Sink(OutputBucket, OutputKey, CheckpointPath)

    sink.state.iteration shouldBe 0
  }

  it should "initialize optimisation state with provided iteration number" in {
    val startingIterationNumber = 9

    val sink = new FederatedLearningSearchOptimizerS3Sink(OutputBucket, OutputKey, CheckpointPath,
      Some(CheckpointPath + "/bootstrap.json"), Some(startingIterationNumber))

    sink.state.iteration shouldBe startingIterationNumber
  }

  it should "read and write optimisation state" in {
    Given("empty state directory and a sink")
    val sink = new FederatedLearningSearchOptimizerS3Sink(OutputBucket, OutputKey, CheckpointPath, s3EndpointOverride = Some(MockEndpoint))

    When("it writes new state")
    val newState = OptimisationState(5, Array(0), Array(1), Some(Array(2)))
    sink.writeState(newState)

    Then("state file is written in checkpoint directory")
    val stateFiles = Files.list(Paths.get(CheckpointPath)).collect(Collectors.toList())
    stateFiles.size shouldBe 2
    val savedState = stateFiles.find(_.endsWith("STATE-5")).get
    Files.readAllLines(savedState).mkString("") shouldBe """{"iteration":5,"weights":[0.0],"learningRates":[1.0],"gradient":[2.0]}"""

    When("new sink is created")
    val sink2 = new FederatedLearningSearchOptimizerS3Sink(OutputBucket, OutputKey, CheckpointPath, s3EndpointOverride = Some(MockEndpoint))

    Then("it picks up last written state")
    sink2.state.iteration shouldBe newState.iteration
    sink2.state.weights should contain theSameElementsInOrderAs newState.weights
    sink2.state.learningRates should contain theSameElementsInOrderAs newState.learningRates
    sink2.state.gradient.get should contain theSameElementsInOrderAs newState.gradient.get
  }

  it should "write model" in {
    val model = ModelOutput(Array(100, 70, 50, 30, 10, 0, 0, 100, 2000, 75, 0, 0, 0, 25, 0, 140, 200, 0), 1)

    val s3 = S3TestUtil(MockEndpointPort, Some(OutputBucket))

    val sink = new FederatedLearningSearchOptimizerS3Sink(OutputBucket, OutputKey, CheckpointPath, s3EndpointOverride = Some(MockEndpoint))
    sink.writeModel(model)

    val expectedJson = """{"model":[100,70,50,30,10,0,0,100,2000,75,0,0,0,25,0,140,200,0],"iteration":1}"""
    s3.getObjectAsString(OutputBucket, OutputKey + "/latest.json") shouldBe expectedJson
    s3.getObjectAsString(OutputBucket, OutputKey + "/1.json") shouldBe expectedJson

    s3.shutdown
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanupTestDirectories()
  }

  private def cleanupTestDirectories(): Unit = {
    FileUtils.deleteDirectory(new File(CheckpointPath))
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanupTestDirectories()
  }
}
