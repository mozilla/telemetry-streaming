/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.learning.federated

import com.mozilla.telemetry.learning.federated.FederatedLearningSearchOptimizerConstants.StartingLearningRate
import org.scalactic.TolerantNumerics
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class FederatedLearningRPropOptimizerTest extends FlatSpec with Matchers {

  "Optimizer" should "correctly optimize" in {
    val weights = Source.fromURL(getClass.getResource("frecency-test-data/weights.csv")).getLines().toSeq.map(_.split(" ").map(_.toInt).toSeq)
    val startingWeights = weights.head.toArray.map(_.toDouble)

    var previousWeights: Array[Double] = startingWeights
    var previousLearningRates: Array[Double] = Array.fill(weights.size)(StartingLearningRate)
    var previousGradient: Option[Array[Double]] = None

    val optimizedWeights = for (iteration <- 0 to 29) yield {
      val updates = Source.fromURL(getClass.getResource(f"frecency-test-data/updates-${iteration}%02d.csv"))
        .getLines().toSeq.map(_.split(" ").map(_.toDouble).toSeq)
      val gradient = updates.reduce[Seq[Double]] { case (a, b) => a.zip(b).map { case (x, y) => x + y } }.toArray // average element-wise

      val InternalResult(newWeights, newLearningRates) =
        FederatedLearningRPropOptimizer.fit(previousWeights, gradient, previousGradient, previousLearningRates, false)

      previousWeights = newWeights
      previousLearningRates = newLearningRates
      previousGradient = Some(gradient)

      newWeights.toSeq.map(_.toInt)
    }

    // compare weights with +/-1 tolerance
    implicit val intEq = TolerantNumerics.tolerantIntEquality(1)

    val message = new StringBuilder
    var weightsEqual = true
    optimizedWeights.indices.foreach { i =>
      val expected = weights(i + 1)
      val calculated = optimizedWeights(i)

      expected.zip(calculated).foreach { case (e, c) =>
        if (!(e === c)) weightsEqual = false
      }

      message.append(s"Iteration $i\n")
      message.append(" expected: " + expected.mkString("", ", ", "\n"))
      message.append("optimized: " + calculated.mkString("", ", ", "\n\n"))
    }

    assert(weightsEqual, "\nCalculated weights did not match expected, results were: \n" + message)
  }
}
