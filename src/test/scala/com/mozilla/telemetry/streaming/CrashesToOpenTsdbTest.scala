/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class CrashesToOpenTsdbTest extends FlatSpec with Matchers with BeforeAndAfterEach with DataFrameSuiteBase {

  private val defaultMeasurementName = "crashes"

  def isJsonObject(payload: String): Boolean = {
    try {
      parse(payload).isInstanceOf[JObject]
    } catch {
      case _: Throwable => false
    }
  }

  "Parse ping" should "create valid json object strings" in {
    import spark.implicits._

    val k = 5
    val messages = TestUtils.generateCrashMessages(k).map(_.toByteArray).seq
    val parsedPings = CrashesToOpenTsdb.getParsedPings(spark.sqlContext.createDataset(messages).toDF,
      raiseOnError = true, defaultMeasurementName)
    parsedPings.collect.count(isJsonObject) should be (k)
  }

  "Crash signature formatter" should "replace spaces, colons, and pipes with the correct replacements" in {
    val crashSignature =
      """mozilla::widget::WinUtils::WaitForMessage | nsAppShell::ProcessNextNative Event"""

    val expected =
      """mozilla-widget-WinUtils-WaitForMessage.nsAppShell-ProcessNextNative_Event"""

    assert(CrashesToOpenTsdb.formatCrashSignature(crashSignature) == expected)
  }

  it should "replace non-acceptable characters with forward slash" in {
    val crashSignature =
      """abcxyz01239ABCXYZ-.-_//@:||([])=+/.@"""

    val expected =
      """abcxyz01239ABCXYZ-.-_/////////////./"""

    assert(CrashesToOpenTsdb.formatCrashSignature(crashSignature) == expected)
  }
}
