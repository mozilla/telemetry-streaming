/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.learning.federated

import com.mozilla.telemetry.learning.federated.FederatedLearningSearchOptimizerConstants.{NumberOfFeatures, StartingLearningRate, StartingWeights}
import com.mozilla.telemetry.streaming.FrecencyUpdateAggregate
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
  // https://github.com/florian/federated-learning-addon/blob/master/addon/prefs.js#L26
  val StartingWeights: Array[Double] = Array(4, 14, 31, 90, 100, 70, 50, 30, 10, 0, 0, 100, 2000, 75, 0, 0, 0, 25, 0, 140, 200, 0)
  val NumberOfFeatures: Int = StartingWeights.length
  val StartingLearningRate: Int = 2
}

class FederatedLearningSearchOptimizerS3Sink(outputPath: String, stateCheckpointPath: String) extends Sink {

  private[federated] var state: OptimisationState = initState()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val aggregates = data.collect().map(FrecencyUpdateAggregate(_))

    val iteration: Long = state.iteration

    val ord = Ordering.by((_: FrecencyUpdateAggregate).modelVersion)
    aggregates.filter(_.studyVariation == "treatment").filter(_.modelVersion.toLong >= iteration).reduceOption(ord.min) match {
      case None =>
      // no relevant updates, do nothing
      case Some(aggregate) =>

        val lastWeights: Array[Double] = state.weights
        val learningRates: Array[Double] = state.learningRates
        val previousGradient: Option[Array[Double]] = state.gradient

        val gradient = aggregate.avgUpdates
        val InternalResult(newWeights, newLearningRates) = FederatedLearningRPropOptimizer.fit(lastWeights, gradient, previousGradient, learningRates)

        val newIteration = iteration + 1

        // val log = Array(OptimisationLogLine(aggregate.window.start, aggregate.modelVersion.toLong, aggregate.avgLoss))

        val newState = OptimisationState(newIteration, newWeights, newLearningRates, Option(gradient))
        writeModel(ModelOutput(newWeights.map(math.round(_).toInt), newIteration))

        writeState(newState)
        state = newState
    }
  }

  private[federated] def writeModel(modelOutput: ModelOutput): Unit = {
    implicit val formats = DefaultFormats
    val jsonModel = Serialization.write(modelOutput)

    val conf = SparkHadoopUtil.get.conf
    val modelOutputPath = new Path(outputPath)

    val fs = modelOutputPath.getFileSystem(conf)
    if (!fs.exists(modelOutputPath)) {
      fs.mkdirs(modelOutputPath)
    }


    val latestStream = fs.create(new Path(outputPath + "/" + "latest.json"))
    latestStream.writeBytes(jsonModel)
    latestStream.close()

    val versionedStream = fs.create(new Path(outputPath + "/" + modelOutput.iteration + ".json"))
    versionedStream.writeBytes(jsonModel)
    versionedStream.close()

    fs.close()
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

    val statuses = fs.listStatus(checkpointPath)
    val latestFileOpt = if (statuses != null) {
      val paths = statuses.map(_.getPath)
      val sorted = paths.sortWith { case (p1, p2) => p1.getName > p2.getName }
      sorted.headOption
    } else {
      None
    }
    try {
      latestFileOpt match {
        case Some(latestFile) =>
          val fis = fs.open(latestFile)
          val fileContents: String = IOUtils.toString(fis)
          fis.close()

          implicit val formats = DefaultFormats
          Serialization.read[OptimisationState](fileContents)
        case None =>
          OptimisationState(0, StartingWeights, Array.fill(NumberOfFeatures)(StartingLearningRate), None)
      }
    } finally {
      fs.close()
    }
  }
}

class FederatedLearningSearchOptimizerS3SinkProvider extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {

    val params = parameters.keySet
    require(params.contains("modelOutputPath"), "modelOutputPath is required")
    require(params.contains("stateCheckpointPath"), "stateCheckpointPath is required")

    val outputPath = parameters("modelOutputPath")
    val stateCheckpointPath = parameters("stateCheckpointPath")

    new FederatedLearningSearchOptimizerS3Sink(outputPath, stateCheckpointPath)
  }
}

case class OptimisationState(iteration: Long, weights: Array[Double], learningRates: Array[Double], gradient: Option[Array[Double]])

case class ModelOutput(model: Array[Int], iteration: Long)
