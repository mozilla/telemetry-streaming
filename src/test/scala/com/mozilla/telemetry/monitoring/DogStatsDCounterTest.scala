/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.monitoring

import org.scalatest.{FlatSpec, Matchers}

class DogStatsDCounterTest extends FlatSpec with Matchers {
  "DogStatsDCounter" should "produce a properly formatted minimal datagram string" in {
    val actual = DogStatsDCounter("test.metric").format()
    actual should be("test.metric:1|c")
  }

  it should "produce a properly formatted fully-specified datagram string" in {
    val actual = DogStatsDCounter("test.metric", 3, Some(Map("key1" -> "value1", "key2" -> "value2")), Some(Seq("tag1", "tag2"))).format(Some(0.5))
    actual should be("test.metric:3|c|@0.5|#key1:value1,key2:value2,tag1,tag2")
  }

  it should "handle present key-value tags without non-key-value tags" in {
    val actual = DogStatsDCounter("test.metric", 3, Some(Map("key1" -> "value1", "key2" -> "value2")), None).format(Some(0.5))
    actual should be("test.metric:3|c|@0.5|#key1:value1,key2:value2")
  }

  it should "handle present non-key-value tags without key-value tags" in {
    val actual = DogStatsDCounter("test.metric", 3, None, Some(Seq("tag1", "tag2"))).format(Some(0.5))
    actual should be("test.metric:3|c|@0.5|#tag1,tag2")
  }

  it should "normalize illegal characters" in {
    val actual = DogStatsDCounter("test.me|t@r:ic", 3, None, Some(Seq("t|a@g:1", "t|a@g:2"))).format(Some(0.5))
    actual should be("test.me_t_r_ic:3|c|@0.5|#t_a_g_1,t_a_g_2")
  }
}
