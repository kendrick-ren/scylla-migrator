/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.kinesis

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  Worker
}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.amazonaws.services.kinesis.model.Record
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration

/** Spark Streaming receiver for a plain Amazon Kinesis Data Stream.
  *
  * Unlike [[KinesisDynamoDBReceiver]], this receiver does NOT wrap the stream through the DynamoDB
  * Streams Kinesis Adapter. It consumes JSON-encoded records directly from a Kinesis stream that
  * was enabled as a Kinesis Data Stream destination on the source DynamoDB table. Compared to
  * DynamoDB Streams, this gives up to 1 year retention and timestamp-based replay via
  * `AT_TIMESTAMP` initial position.
  *
  * The user-supplied `streamNameOrArn` may be either a bare stream name (e.g., `my-stream`) or a
  * full ARN (e.g., `arn:aws:kinesis:us-east-1:123456789012:stream/my-stream`). ARNs are normalized
  * to stream names because KCL 1.x's [[KinesisClientLibConfiguration]] expects a stream name.
  */
private[kinesis] class KinesisStreamReceiver[T](
  streamNameOrArn: String,
  endpointUrl: String,
  regionName: String,
  initialPosition: KinesisInitialPosition,
  checkpointAppName: String,
  checkpointInterval: Duration,
  storageLevel: StorageLevel,
  messageHandler: Record => T,
  kinesisCreds: SparkAWSCredentials,
  dynamoDBCreds: Option[SparkAWSCredentials],
  cloudWatchCreds: Option[SparkAWSCredentials],
  metricsLevel: MetricsLevel,
  metricsEnabledDimensions: Set[String]
) extends AbstractKCLReceiver[T](
      KinesisStreamReceiver.extractStreamName(streamNameOrArn),
      endpointUrl,
      regionName,
      initialPosition,
      checkpointAppName,
      checkpointInterval,
      storageLevel,
      messageHandler,
      kinesisCreds,
      dynamoDBCreds,
      cloudWatchCreds,
      metricsLevel,
      metricsEnabledDimensions
    ) {

  override protected def resolveStreamName(
    configured: String,
    dynamoDBClient: AmazonDynamoDB
  ): String = configured

  override protected def buildWorker(
    kclConfig: KinesisClientLibConfiguration,
    recordProcessorFactory: v2.IRecordProcessorFactory,
    kinesisProvider: AWSCredentialsProvider,
    dynamoDBClient: AmazonDynamoDB,
    cloudWatchClient: AmazonCloudWatch
  ): Worker = {
    val kinesisClient =
      AmazonKinesisClientBuilder.standard
        .withCredentials(kinesisProvider)
        .withRegion(regionName)
        .build()

    new Worker.Builder()
      .recordProcessorFactory(recordProcessorFactory)
      .config(kclConfig)
      .kinesisClient(kinesisClient)
      .dynamoDBClient(dynamoDBClient)
      .cloudWatchClient(cloudWatchClient)
      .build()
  }
}

private[kinesis] object KinesisStreamReceiver {

  /** Regex capturing the `<stream-name>` segment of a Kinesis ARN. The segment ends at either:
    *   - another `/` (enhanced fan-out consumer ARN: `.../stream/<name>/consumer/<consumer>`),
    *   - another `:` (consumer ARNs append `:<creation-timestamp>`), or
    *   - end of string.
    */
  private val StreamSegment = """:stream/([^/:]+)(?:/|$|:)""".r

  /** Extract the stream name from a Kinesis Data Stream ARN, or return the input unchanged if it
    * does not look like an ARN.
    *
    * Supported ARN shapes:
    *   - Data stream: `arn:aws:kinesis:<region>:<account>:stream/<stream-name>`
    *   - Enhanced fan-out consumer:
    *     `arn:aws:kinesis:<region>:<account>:stream/<stream-name>/consumer/<consumer>:<ts>`
    */
  def extractStreamName(arnOrName: String): String =
    StreamSegment.findFirstMatchIn(arnOrName) match {
      case Some(m) => m.group(1)
      case None    => arnOrName
    }
}
