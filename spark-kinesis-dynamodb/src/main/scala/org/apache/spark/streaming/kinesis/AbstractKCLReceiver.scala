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

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.collection.immutable.ArraySeq

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.{ AmazonCloudWatch, AmazonCloudWatchClientBuilder }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  Worker
}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.amazonaws.services.kinesis.model.Record

import org.apache.spark.internal.Logging
import org.apache.spark.storage.{ StorageLevel, StreamBlockId }
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.AtTimestamp
import org.apache.spark.streaming.receiver.{ BlockGenerator, BlockGeneratorListener, Receiver }
import org.apache.spark.util.Utils

/** Shared KCL-based Spark Streaming receiver. Encapsulates all the book-keeping that the
  * DynamoDB-Streams receiver and the plain-Kinesis receiver have in common:
  *
  *   - worker lifecycle (`onStart` / `onStop`)
  *   - block generation + sequence-number tracking
  *   - checkpointer wiring
  *
  * Subclasses only provide:
  *
  *   - [[resolveStreamName]] -- how to translate the user-configured stream identifier into the
  *     string KCL expects (e.g., for DynamoDB Streams we must look up the latest stream ARN via
  *     `DescribeTable`).
  *   - [[buildWorker]] -- how to construct the KCL `Worker`. The DynamoDB-Streams path uses
  *     `StreamsWorkerFactory`; the plain-Kinesis path uses `new Worker.Builder()`.
  */
private[kinesis] abstract class AbstractKCLReceiver[T](
  val streamName: String,
  protected val endpointUrl: String,
  protected val regionName: String,
  protected val initialPosition: KinesisInitialPosition,
  protected val checkpointAppName: String,
  protected val checkpointInterval: Duration,
  storageLevel: StorageLevel,
  messageHandler: Record => T,
  protected val kinesisCreds: SparkAWSCredentials,
  protected val dynamoDBCreds: Option[SparkAWSCredentials],
  protected val cloudWatchCreds: Option[SparkAWSCredentials],
  protected val metricsLevel: MetricsLevel,
  protected val metricsEnabledDimensions: Set[String]
) extends Receiver[T](storageLevel) with Logging { receiver =>

  /** workerId is used by the KCL; it is based on the IP address of the Spark worker where this
    * receiver runs (not the driver's).
    */
  @volatile private var workerId: String = null

  @volatile private var worker: Worker = null
  @volatile private var workerThread: Thread = null
  @volatile private var blockGenerator: BlockGenerator = null

  @volatile private var kinesisCheckpointer: KinesisDynamoDBCheckpointer = null

  /** Sequence number ranges added to the currently active block (protected by the block generator
    * lock).
    */
  private val seqNumRangesInCurrentBlock = new mutable.ArrayBuffer[SequenceNumberRange]

  private val blockIdToSeqNumRanges =
    new ConcurrentHashMap[StreamBlockId, SequenceNumberRanges]

  private val shardIdToLatestStoredSeqNum = new ConcurrentHashMap[String, String]

  /** Translate the user-configured stream identifier into the value KCL's
    * [[KinesisClientLibConfiguration]] should use.
    *
    * The DynamoDB Streams subclass uses this hook to resolve the current stream ARN from the source
    * table name via `DescribeTable`. The plain-Kinesis subclass can return the argument unchanged.
    */
  protected def resolveStreamName(
    configured: String,
    dynamoDBClient: AmazonDynamoDB
  ): String

  /** Build the KCL [[Worker]] that will drive this receiver.
    *
    * Implementations differ by whether they wrap the KCL with the DynamoDB Streams Kinesis Adapter
    * or talk to a plain Kinesis stream directly.
    */
  protected def buildWorker(
    kclConfig: KinesisClientLibConfiguration,
    recordProcessorFactory: v2.IRecordProcessorFactory,
    kinesisProvider: AWSCredentialsProvider,
    dynamoDBClient: AmazonDynamoDB,
    cloudWatchClient: AmazonCloudWatch
  ): Worker

  /** This is called when the KinesisReceiver starts and must be non-blocking. The KCL creates and
    * manages the receiving/processing thread pool through Worker.run().
    */
  override def onStart(): Unit = {
    blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)

    workerId = Utils.localHostName() + ":" + UUID.randomUUID()

    kinesisCheckpointer = new KinesisDynamoDBCheckpointer(receiver, checkpointInterval, workerId)
    val kinesisProvider = kinesisCreds.provider

    val dynamoDBClient =
      AmazonDynamoDBClientBuilder
        .standard()
        .withCredentials(dynamoDBCreds.fold(kinesisProvider)(_.provider))
        .withRegion(regionName)
        .build()

    val actualStreamName = resolveStreamName(streamName, dynamoDBClient)

    val kinesisClientLibConfiguration = {
      val base = new KinesisClientLibConfiguration(
        checkpointAppName,
        actualStreamName,
        kinesisProvider,
        dynamoDBCreds.map(_.provider).getOrElse(kinesisProvider),
        cloudWatchCreds.map(_.provider).getOrElse(kinesisProvider),
        workerId
      )
        .withKinesisEndpoint(endpointUrl)
        .withTaskBackoffTimeMillis(500)
        .withRegionName(regionName)
        .withMetricsLevel(metricsLevel)
        .withMetricsEnabledDimensions(metricsEnabledDimensions.asJava)
        .withIdleTimeBetweenReadsInMillis(500)
        .withMaxRecords(1000)
        .withFailoverTimeMillis(60000)
        .withParentShardPollIntervalMillis(10000)

      initialPosition match {
        case ts: AtTimestamp =>
          base.withTimestampAtInitialPositionInStream(ts.getTimestamp)
        case _ =>
          base.withInitialPositionInStream(initialPosition.getPosition)
      }
    }

    val recordProcessorFactory = new v2.IRecordProcessorFactory {
      override def createProcessor(): v2.IRecordProcessor =
        new V1ToV2RecordProcessor(new KinesisDynamoDBRecordProcessor(receiver, workerId))
    }

    val cloudWatchClient = AmazonCloudWatchClientBuilder.standard
      .withCredentials(cloudWatchCreds.map(_.provider).getOrElse(kinesisProvider))
      .withRegion(regionName)
      .withClientConfiguration(kinesisClientLibConfiguration.getCloudWatchClientConfiguration)
      .build

    worker = buildWorker(
      kinesisClientLibConfiguration,
      recordProcessorFactory,
      kinesisProvider,
      dynamoDBClient,
      cloudWatchClient
    )

    workerThread = new Thread() {
      override def run(): Unit =
        try worker.run()
        catch {
          case NonFatal(e) =>
            restart("Error running the KCL worker in Receiver", e)
        }
    }

    blockIdToSeqNumRanges.clear()
    blockGenerator.start()

    workerThread.setName(s"Kinesis Receiver ${streamId}")
    workerThread.setDaemon(true)
    workerThread.start()

    logInfo(s"Started receiver with workerId $workerId")
  }

  /** This is called when the KinesisReceiver stops. The KCL worker.shutdown() method stops the
    * receiving/processing threads. The KCL will do its best to drain and checkpoint any in-flight
    * records upon shutdown.
    */
  override def onStop(): Unit = {
    if (workerThread != null) {
      if (worker != null) {
        worker.shutdown()
        worker = null
      }
      workerThread.join()
      workerThread = null
      logInfo(s"Stopped receiver for workerId $workerId")
    }
    workerId = null
    if (kinesisCheckpointer != null) {
      kinesisCheckpointer.shutdown()
      kinesisCheckpointer = null
    }
  }

  /** Add records of the given shard to the current block being generated */
  private[kinesis] def addRecords(shardId: String, records: java.util.List[Record]): Unit =
    if (records.size > 0) {
      val dataIterator = records.iterator().asScala.map(messageHandler)
      val metadata = SequenceNumberRange(
        streamName,
        shardId,
        records.get(0).getSequenceNumber(),
        records.get(records.size() - 1).getSequenceNumber(),
        records.size()
      )
      blockGenerator.addMultipleDataWithCallback(dataIterator, metadata)
    }

  /** Return the current rate limit defined in [[BlockGenerator]]. */
  private[kinesis] def getCurrentLimit: Int = {
    assert(blockGenerator != null)
    math.min(blockGenerator.getCurrentLimit, Int.MaxValue).toInt
  }

  /** Get the latest sequence number for the given shard that can be checkpointed through KCL */
  private[kinesis] def getLatestSeqNumToCheckpoint(shardId: String): Option[String] =
    Option(shardIdToLatestStoredSeqNum.get(shardId))

  /** Set the checkpointer that will be used to checkpoint sequence numbers to DynamoDB for the
    * given shardId.
    */
  def setCheckpointer(shardId: String, checkpointer: IRecordProcessorCheckpointer): Unit = {
    assert(kinesisCheckpointer != null, "Kinesis Checkpointer not initialized!")
    kinesisCheckpointer.setCheckpointer(shardId, checkpointer)
  }

  /** Remove the checkpointer for the given shardId. The provided checkpointer will be used to
    * checkpoint one last time for the given shard. If `checkpointer` is `null`, then we will not
    * checkpoint.
    */
  def removeCheckpointer(shardId: String, checkpointer: IRecordProcessorCheckpointer): Unit = {
    assert(kinesisCheckpointer != null, "Kinesis Checkpointer not initialized!")
    kinesisCheckpointer.removeCheckpointer(shardId, checkpointer)
  }

  private def rememberAddedRange(range: SequenceNumberRange): Unit =
    seqNumRangesInCurrentBlock += range

  private def finalizeRangesForCurrentBlock(blockId: StreamBlockId): Unit = {
    blockIdToSeqNumRanges.put(
      blockId,
      SequenceNumberRanges(seqNumRangesInCurrentBlock.to(ArraySeq))
    )
    seqNumRangesInCurrentBlock.clear()
    logDebug(s"Generated block $blockId has $blockIdToSeqNumRanges")
  }

  private def storeBlockWithRanges(
    blockId: StreamBlockId,
    arrayBuffer: mutable.ArrayBuffer[T]
  ): Unit = {
    val rangesToReportOption = Option(blockIdToSeqNumRanges.remove(blockId))
    if (rangesToReportOption.isEmpty) {
      stop(
        "Error while storing block into Spark, could not find sequence number ranges " +
          s"for block $blockId"
      )
      return
    }

    val rangesToReport = rangesToReportOption.get
    var attempt = 0
    var stored = false
    var throwable: Throwable = null
    while (!stored && attempt <= 3)
      try {
        store(arrayBuffer, rangesToReport)
        stored = true
      } catch {
        case NonFatal(th) =>
          attempt += 1
          throwable = th
      }
    if (!stored) {
      stop("Error while storing block into Spark", throwable)
    }

    rangesToReport.ranges.foreach { range =>
      shardIdToLatestStoredSeqNum.put(range.shardId, range.toSeqNumber)
    }
  }

  private class GeneratedBlockHandler extends BlockGeneratorListener {
    def onAddData(data: Any, metadata: Any): Unit =
      rememberAddedRange(metadata.asInstanceOf[SequenceNumberRange])

    def onGenerateBlock(blockId: StreamBlockId): Unit =
      finalizeRangesForCurrentBlock(blockId)

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit =
      storeBlockWithRanges(blockId, arrayBuffer.asInstanceOf[mutable.ArrayBuffer[T]])

    def onError(message: String, throwable: Throwable): Unit =
      reportError(message, throwable)
  }
}
