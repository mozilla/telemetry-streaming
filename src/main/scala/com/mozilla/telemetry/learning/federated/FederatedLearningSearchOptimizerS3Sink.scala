/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.learning.federated

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.CannedAccessControlList.PublicRead
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, PutObjectResult}
import com.mozilla.telemetry.learning.federated.FederatedLearningSearchOptimizerConstants.{NumberOfFeatures, StartingLearningRate, StartingWeights}
import com.mozilla.telemetry.streaming.FrecencyUpdateAggregate
import com.mozilla.telemetry.util.PrettyPrint
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

object FederatedLearningSearchOptimizerConstants {
  // https://dxr.mozilla.org/mozilla-central/rev/085cdfb90903d4985f0de1dc7786522d9fb45596/browser/app/profile/firefox.js#901
  val StartingWeights: Array[Double] = Array(4, 14, 31, 90, 100, 70, 50, 30, 10, 0, 0, 100, 2000, 75, 0, 0, 0, 25, 0, 140, 200, 0)
  val NumberOfFeatures: Int = StartingWeights.length
  val StartingLearningRate: Int = 2
}

class FederatedLearningSearchOptimizerS3Sink(outputBucket: String, outputKey: String, stateCheckpointPath: String,
                                             s3EndpointOverride: Option[String] = None,
                                             startingIteration: Option[Long] = None) extends Sink {

  val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName)

  private[federated] var state: OptimisationState = initState()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    log.info(s"Starting addBatch with starting state $state")

    val aggregates = data.collect().map(FrecencyUpdateAggregate(_))

    val iteration: Long = state.iteration

    val ord = Ordering.by((_: FrecencyUpdateAggregate).modelVersion)
    aggregates.filter(_.modelVersion >= iteration).reduceOption(ord.min) match {
      case None =>
        val aggMsg = if (aggregates.isEmpty) "empty" else aggregates.mkString(",")
        log.info(s"No updates for iteration $iteration, aggregates are $aggMsg")

      case Some(aggregate) =>
        log.info(s"Updating model with data from iteration $iteration, aggregation $aggregate")

        val lastWeights: Array[Double] = state.weights
        val learningRates: Array[Double] = state.learningRates
        val previousGradient: Option[Array[Double]] = state.gradient

        val gradient = aggregate.avgUpdates
        val InternalResult(newWeights, newLearningRates) = FederatedLearningRPropOptimizer.fit(lastWeights, gradient, previousGradient, learningRates)

        val newIteration = iteration + 1

        val newState = OptimisationState(newIteration, newWeights, newLearningRates, Option(gradient))

        log.info(s"Update results: $newState")

        val rounded = newWeights.map(math.round(_).toInt)

        log.info(s"Weights after rounding: ${rounded.mkString(",")}")

        writeModel(ModelOutput(rounded, newIteration))

        writeState(newState)
        state = newState

        log.info(s"New state and model written, new iteration is $newIteration")
    }
  }

  private[federated] def writeModel(modelOutput: ModelOutput): Unit = {
    log.info(s"Writing model $modelOutput to s3://$outputBucket/$outputKey")
    implicit val formats = DefaultFormats
    val jsonModel = Serialization.write(modelOutput)

    val client = new S3ClientWrapper(s3EndpointOverride)
    client.putString(outputBucket, outputKey + "/latest.json", jsonModel)
    client.putString(outputBucket, outputKey + s"/${modelOutput.iteration}.json", jsonModel)
  }

  private[federated] def writeState(state: OptimisationState): Unit = {
    implicit val formats = DefaultFormats
    val jsonState = Serialization.write(state)

    val fileName = "STATE-" + state.iteration
    val path = new Path(stateCheckpointPath + "/" + fileName)

    val fs = path.getFileSystem(SparkHadoopUtil.get.conf)
    val outputStream = fs.create(path)
    outputStream.writeBytes(jsonState)

    outputStream.close()
    fs.close()
  }

  private[federated] def initState(): OptimisationState = {
    val conf = SparkHadoopUtil.get.conf
    val checkpointPath = new Path(stateCheckpointPath)
    val fs = checkpointPath.getFileSystem(conf)
    if (!fs.exists(checkpointPath)) {
      fs.mkdirs(checkpointPath)
    }

    try {
          val statuses = fs.listStatus(checkpointPath)
          val latestFileOpt = if (statuses != null) {
            val paths = statuses.map(_.getPath)
            val sorted = paths.sortWith { case (p1, p2) => p1.getName > p2.getName }
            sorted.headOption
          } else {
            None
          }
          latestFileOpt match {
            case Some(latestFile) =>
              val fis = fs.open(latestFile)
              val fileContents: String = IOUtils.toString(fis)
              fis.close()

              implicit val formats = DefaultFormats
              Serialization.read[OptimisationState](fileContents)
            case None =>
              OptimisationState(startingIteration.getOrElse(0), StartingWeights, Array.fill(NumberOfFeatures)(StartingLearningRate), None)
          }
    } finally {
      fs.close()
    }
  }

  private[federated] class S3ClientWrapper(endpointOverride: Option[String] = None) {
    private val builder = AmazonS3ClientBuilder
      .standard
      .withPathStyleAccessEnabled(true)

    private val client = endpointOverride.map { ep: String =>
      val endpoint = new EndpointConfiguration(ep, "us-west-2")
      builder.withEndpointConfiguration(endpoint)
    }.getOrElse(builder).build()

    private val metadata = new ObjectMetadata()
    metadata.setContentType("application/json")
    metadata.setCacheControl("no-cache, no-store, must-revalidate")

    def putString(bucket: String, key: String, contents: String): PutObjectResult = {
      val contentStream = new java.io.ByteArrayInputStream(contents.getBytes)

      val request = new PutObjectRequest(bucket, key, contentStream, metadata).withCannedAcl(PublicRead)
      client.putObject(request)
    }
  }
}

class FederatedLearningSearchOptimizerS3SinkProvider extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {

    val params = parameters.keySet
    require(params.contains("modelOutputBucket"), "modelOutputBucket is required")
    require(params.contains("modelOutputKey"), "modelOutputKey is required")
    require(params.contains("stateCheckpointPath"), "stateCheckpointPath is required")

    val outputBucket = parameters("modelOutputBucket")
    val outputKey = parameters("modelOutputKey")
    val stateCheckpointPath = parameters("stateCheckpointPath")
    val s3EndpointOverride = parameters.get("s3EndpointOverride")
    val startingIteration = parameters.get("startingIteration").map(_.toLong)

    new FederatedLearningSearchOptimizerS3Sink(outputBucket, outputKey, stateCheckpointPath, s3EndpointOverride, startingIteration)
  }
}

case class OptimisationState(iteration: Long, weights: Array[Double], learningRates: Array[Double], gradient: Option[Array[Double]]) extends PrettyPrint

case class ModelOutput(model: Array[Int], iteration: Long) extends PrettyPrint
