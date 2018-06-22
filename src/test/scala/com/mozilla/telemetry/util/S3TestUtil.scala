/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.util

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.S3Object
import io.findify.s3mock.S3Mock


case class S3TestUtil(port: Int, defaultBucket: Option[String] = None) {
  private val s3mock = S3Mock(port = port)
  s3mock.start

  private val endpoint = new EndpointConfiguration(s"http://localhost:$port", "us-west-2")
  private val client = AmazonS3ClientBuilder
    .standard
    .withPathStyleAccessEnabled(true)
    .withEndpointConfiguration(endpoint)
    .build()

  defaultBucket.map(client.createBucket)

  private def getContent(s3Object: S3Object): String = scala.io.Source.fromInputStream(s3Object.getObjectContent, "UTF-8").mkString

  def getObjectAsString(bucket: String, key: String): String = {
    getContent(client.getObject(bucket, key))
  }

  def shutdown(): Unit = s3mock.shutdown
}
