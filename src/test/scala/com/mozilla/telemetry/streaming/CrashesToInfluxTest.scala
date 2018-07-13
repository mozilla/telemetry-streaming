/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

class CrashesToInfluxTest extends FlatSpec with Matchers with DataFrameSuiteBase {

  private val defaultMeasurementName = "crashes"

  "Parse ping" should "create strings that start with measurement name" in {
    import spark.implicits._

    val k = 5
    val messages = TestUtils.generateCrashMessages(k).map(_.toByteArray).seq
    val parsedPings = CrashesToInflux.getParsedPings(spark.sqlContext.createDataset(messages).toDF,
      raiseOnError = true,  defaultMeasurementName)
    parsedPings.collect.count(_.startsWith(defaultMeasurementName)) should be (k)
  }

  "Crash signature formatter" should "add a single backslash InfluxDB special characters" in {
    val crashSignature =
      """OOM | unknown | js::AutoEnterOOMUnsafeRegion::crash | F\\a,,k\e=Tr== ",a ce" | js::jit::ExceptionHandlerBailout"""

    val expected =
      """OOM\ |\ unknown\ |\ js::AutoEnterOOMUnsafeRegion::crash\ |\ F\\a\,\,k\e\=Tr\=\=\ \"\,a\ ce\"\ |\ js::jit::ExceptionHandlerBailout"""

    assert(CrashesToInflux.formatCrashSignature(crashSignature) == expected)
  }
}
