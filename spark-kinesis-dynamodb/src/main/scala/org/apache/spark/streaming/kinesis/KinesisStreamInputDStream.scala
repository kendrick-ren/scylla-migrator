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

import com.amazonaws.services.kinesis.model.Record
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.KinesisInputDStream.{
  DEFAULT_KINESIS_ENDPOINT_URL,
  DEFAULT_METRICS_ENABLED_DIMENSIONS,
  DEFAULT_METRICS_LEVEL,
  DEFAULT_STORAGE_LEVEL
}
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag

/** Input DStream that consumes a plain Amazon Kinesis Data Stream using a
  * [[KinesisStreamReceiver]].
  *
  * Analogous to [[KinesisDynamoDBInputDStream]], except the receiver talks to Kinesis directly (via
  * [[com.amazonaws.services.kinesis.AmazonKinesisClientBuilder]] and
  * [[com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker.Builder]]) instead of going
  * through the DynamoDB Streams Kinesis Adapter.
  *
  * Use this when the source DynamoDB table has been configured with a "Kinesis data stream"
  * destination (see `EnableKinesisStreamingDestination`). Compared to the DynamoDB-Streams variant,
  * this supports up to 1 year of retention and `AT_TIMESTAMP` initial position, which means the
  * initial snapshot is free to take longer than 24 hours.
  */
class KinesisStreamInputDStream[T: ClassTag](
  ssc: StreamingContext,
  streamNameOrArn: String,
  regionName: String,
  initialPosition: KinesisInitialPosition,
  checkpointAppName: String,
  messageHandler: Record => T,
  kinesisCreds: SparkAWSCredentials,
  endpointUrl: String = DEFAULT_KINESIS_ENDPOINT_URL
) extends KinesisInputDStream[T](
      ssc,
      streamNameOrArn,
      endpointUrl,
      regionName,
      initialPosition,
      checkpointAppName,
      ssc.graph.batchDuration,
      DEFAULT_STORAGE_LEVEL,
      messageHandler,
      kinesisCreds,
      dynamoDBCreds   = None,
      cloudWatchCreds = None,
      DEFAULT_METRICS_LEVEL,
      DEFAULT_METRICS_ENABLED_DIMENSIONS
    ) {

  override def getReceiver(): Receiver[T] =
    new KinesisStreamReceiver(
      streamNameOrArn,
      endpointUrl,
      regionName,
      initialPosition,
      checkpointAppName,
      checkpointInterval,
      DEFAULT_STORAGE_LEVEL,
      messageHandler,
      kinesisCreds,
      dynamoDBCreds,
      cloudWatchCreds,
      metricsLevel,
      metricsEnabledDimensions
    )
}
