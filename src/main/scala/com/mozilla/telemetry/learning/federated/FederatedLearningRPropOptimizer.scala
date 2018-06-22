/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.learning.federated

object FederatedLearningRPropOptimizer {

  val Alpha = 2.0
  val Beta = 0.6
  val MinValue = 1.0
  val MaxValue = 3.0

  def fit(weights: Array[Double],
          gradient: Array[Double],
          previousGradient: Option[Array[Double]],
          learningRates: Array[Double],
          alignTimeBuckets: Boolean = true): InternalResult = { // `alignTimeBuckets` exposed for testing

    val (updates, updatedLearningRates) = calculateUpdates(gradient, previousGradient, learningRates)
    val updatedWeights = weights.zip(updates).map { case (w, u) => w + u }
    InternalResult(applyConstraints(updatedWeights, alignTimeBuckets), updatedLearningRates)
  }

  private def calculateUpdates(gradient: Array[Double], previousGradientOpt: Option[Array[Double]],
                               learningRates: Array[Double]): (Array[Double], Array[Double]) = {
    val updatedLearningRates: Array[Double] = previousGradientOpt match {
      case Some(previousGradient) =>
        (for (i <- gradient.indices) yield {
          if (gradient(i) * previousGradient(i) > 0) {
            math.min(learningRates(i) * Alpha, MaxValue)
          } else if (gradient(i) * previousGradient(i) < 0) {
            math.max(learningRates(i) * Beta, MinValue)
          } else {
            learningRates(i)
          }
        }).toArray
      case None =>
        learningRates
    }

    val updates = updatedLearningRates.zip(gradient).map { case (lr, g) => lr * math.signum(g) }
    (updates, updatedLearningRates)
  }

  private def applyConstraints(weights: Array[Double], alignTimeBuckets: Boolean): Array[Double] = {
    val nonNegativeWeights = (weights.min match {
      case min if min < 0 => weights.map(_ - min)
      case _ => weights
    }).toBuffer

    if (alignTimeBuckets) {
      nonNegativeWeights(1) = math.max(nonNegativeWeights(1), nonNegativeWeights(0) + 1)
      nonNegativeWeights(2) = math.max(nonNegativeWeights(2), nonNegativeWeights(1) + 1)
      nonNegativeWeights(3) = math.max(nonNegativeWeights(3), nonNegativeWeights(2) + 1)
    }

    nonNegativeWeights.toArray
  }
}

case class InternalResult(weights: Array[Double], learningRates: Array[Double])
