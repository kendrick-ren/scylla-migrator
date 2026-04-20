package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.scylladb.migrator.AttributeValueUtils
import com.scylladb.migrator.config.{
  AWSCredentials,
  SourceSettings,
  StreamChangesSetting,
  TargetSettings
}
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.{
  KinesisDynamoDBInputDStream,
  KinesisInitialPositions,
  KinesisStreamInputDStream,
  SparkAWSCredentials
}
import com.scylladb.migrator.DynamoUtils
import software.amazon.awssdk.services.dynamodb.model.{
  DeleteItemRequest,
  PutItemRequest,
  TableDescription
}

import java.time.Instant
import java.util
import scala.jdk.CollectionConverters._
import org.apache.spark.util.LongAccumulator

object DynamoStreamReplication {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoStreamReplication")

  type DynamoItem = util.Map[String, AttributeValueV1]

  // We enrich the table items with a column `operationTypeColumn` describing the type of change
  // applied to the item.
  // We have to deal with multiple representation of the data because `spark-kinesis-dynamodb`
  // uses the AWS SDK V1, whereas `emr-dynamodb-hadoop` uses the AWS SDK V2.
  // These are package-public (not `private`) because [[KinesisJsonDeserializer]] needs them to
  // emit the same shape of `DynamoItem` that the DynamoDB Streams path produces.
  val operationTypeColumn: String = "_dynamo_op_type"
  val putOperation: AttributeValueV1 = new AttributeValueV1().withBOOL(true)
  val deleteOperation: AttributeValueV1 = new AttributeValueV1().withBOOL(false)

  /** Per-migrator-run counters. Instantiate ONCE and reuse across all streaming micro-batches —
    * constructing new [[LongAccumulator]]s inside `foreachRDD` silently leaks driver metadata (each
    * accumulator registration is retained for the life of the SparkContext), which OOMs the driver
    * after days of streaming.
    */
  private[writers] final class Metrics(spark: SparkSession) {
    val putCount: LongAccumulator =
      spark.sparkContext.longAccumulator("migrator.putCount")
    val deleteCount: LongAccumulator =
      spark.sparkContext.longAccumulator("migrator.deleteCount")
    val droppedRecordsCount: LongAccumulator =
      spark.sparkContext.longAccumulator("migrator.droppedRecords")
  }

  /** Backward-compatible 4-arg overload. Kept because the integration test calls `run` directly
    * (one invocation total), so per-call accumulator allocation is harmless there. Real streaming
    * callers MUST use the 5-arg overload so a single [[Metrics]] is shared across every
    * micro-batch.
    */
  private[writers] def run(
    msgs: RDD[Option[DynamoItem]],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription
  )(implicit spark: SparkSession): Unit =
    run(msgs, target, renamesMap, targetTableDesc, new Metrics(spark))

  private[writers] def run(
    msgs: RDD[Option[DynamoItem]],
    target: TargetSettings.DynamoDB,
    renamesMap: Map[String, String],
    targetTableDesc: TableDescription,
    metrics: Metrics
  )(implicit spark: SparkSession): Unit = {
    val rdd = msgs.flatMap(_.toSeq)
    val putCount = metrics.putCount
    val deleteCount = metrics.deleteCount
    val keyAttributeNames = targetTableDesc.keySchema.asScala.map(_.attributeName).toSet

    rdd.foreachPartition { partition =>
      if (partition.nonEmpty) {
        val client =
          DynamoUtils.buildDynamoClient(
            target.endpoint,
            target.finalCredentials.map(_.toProvider),
            target.region,
            if (target.removeConsumedCapacity.getOrElse(true))
              Seq(new DynamoUtils.RemoveConsumedCapacityInterceptor)
            else Nil,
            target.alternator
          )
        try
          partition.foreach { item =>
            val isPut = item.get(operationTypeColumn) == putOperation

            val itemWithoutOp = item.asScala.collect {
              case (k, v) if k != operationTypeColumn => k -> AttributeValueUtils.fromV1(v)
            }.asJava

            if (isPut) {
              putCount.add(1)
              val finalItem = itemWithoutOp.asScala.map { case (key, value) =>
                renamesMap.getOrElse(key, key) -> value
              }.asJava
              try
                client.putItem(
                  PutItemRequest.builder().tableName(target.table).item(finalItem).build()
                )
              catch {
                case e: Exception =>
                  log.error(s"Failed to put item into ${target.table}", e)
              }
            } else {
              deleteCount.add(1)
              val keyToDelete = itemWithoutOp.asScala
                .filter { case (key, _) => keyAttributeNames.contains(key) }
                .map { case (key, value) => renamesMap.getOrElse(key, key) -> value }
                .asJava
              try
                client.deleteItem(
                  DeleteItemRequest.builder().tableName(target.table).key(keyToDelete).build()
                )
              catch {
                case e: Exception =>
                  log.error(s"Failed to delete item from ${target.table}", e)
              }
            }
          }
        finally
          client.close()
      }
    }

    if (putCount.value > 0 || deleteCount.value > 0) {
      log.info(
        s"""
           |Changes to be applied:
           |  - ${putCount.value} items to UPSERT
           |  - ${deleteCount.value} items to DELETE
           |  - ${metrics.droppedRecordsCount.value} Kinesis records dropped by deserialization (lifetime total)
           |""".stripMargin
      )
    } else {
      log.info("No changes to apply")
    }
  }

  /** Default KCL checkpoint application name. Dropping the `currentTimeMillis()` suffix used in
    * previous versions makes the name deterministic so that a restarted migrator will resume from
    * the KCL leases/checkpoints written by the previous run instead of starting a new lease table
    * and re-processing from `TrimHorizon`.
    */
  private[writers] def defaultCheckpointAppName(src: SourceSettings.DynamoDB): String =
    s"migrator_${src.table}"

  /** Create the streaming input DStream and wire it up to the replication sink.
    *
    * The concrete DStream chosen depends on `target.streamChanges`:
    *
    *   - [[StreamChangesSetting.DynamoDBStreams]] -- uses the existing
    *     [[KinesisDynamoDBInputDStream]] (KCL wrapped through the DynamoDB Streams Kinesis
    *     Adapter).
    *   - [[StreamChangesSetting.KinesisDataStreams]] -- uses [[KinesisStreamInputDStream]] to
    *     consume a Kinesis Data Stream directly. Records are JSON and parsed with
    *     [[KinesisJsonDeserializer]].
    *
    * The two branches are kept inline (rather than factored out) because `SparkAWSCredentials` is
    * `private[kinesis]` in Spark, so its type name cannot appear in method signatures outside the
    * `org.apache.spark.streaming.kinesis` package. Naming the credentials only through its
    * companion object (as a term) keeps this file compilable.
    *
    * @param snapshotStartTime
    *   Point in time captured just before the initial snapshot began. Used as the Kinesis
    *   `AT_TIMESTAMP` initial position when the user did not provide an explicit
    *   `initialTimestamp`, so that changes which land during a long snapshot are still replayed.
    */
  def createDStream(
    spark: SparkSession,
    streamingContext: StreamingContext,
    src: SourceSettings.DynamoDB,
    target: TargetSettings.DynamoDB,
    targetTableDesc: TableDescription,
    renamesMap: Map[String, String],
    snapshotStartTime: Instant
  ): Unit = {
    val kinesisCreds =
      src.credentials
        .map { case AWSCredentials(accessKey, secretKey, maybeAssumeRole) =>
          val builder =
            SparkAWSCredentials.builder
              .basicCredentials(accessKey, secretKey)
          for (assumeRole <- maybeAssumeRole)
            builder.stsCredentials(assumeRole.arn, assumeRole.getSessionName)
          builder.build()
        }
        .getOrElse(SparkAWSCredentials.builder.build())

    target.streamChanges match {
      case StreamChangesSetting.Disabled =>
        log.warn(
          "createDStream called with streamChanges=Disabled; this is a no-op. " +
            "AlternatorMigrator should not have invoked streaming for a disabled setting."
        )

      case StreamChangesSetting.DynamoDBStreams =>
        val ddbStreamsAppName = defaultCheckpointAppName(src)
        log.warn(
          s"Using deterministic KCL application (checkpoint) name '$ddbStreamsAppName'. " +
            "Two consequences operators should know about: (1) on restart the migrator resumes " +
            "from prior KCL leases instead of replaying from TrimHorizon; (2) if two migrators " +
            s"consume the same source table '${src.table}' concurrently (e.g. fan-out to two " +
            "targets) they will share this lease table and split shard ownership. This path " +
            "does not currently expose an appName override; see docs/stream-changes.rst."
        )
        // Allocate ONCE outside foreachRDD; see [[Metrics]] scaladoc for the leak it prevents.
        val ddbStreamsMetrics = new Metrics(spark)
        new KinesisDynamoDBInputDStream(
          streamingContext,
          streamName        = src.table,
          regionName        = src.region.orNull,
          initialPosition   = new KinesisInitialPositions.TrimHorizon,
          checkpointAppName = ddbStreamsAppName,
          messageHandler = {
            case recAdapter: RecordAdapter =>
              val rec = recAdapter.getInternalObject
              val newMap: DynamoItem = new util.HashMap[String, AttributeValueV1]()

              if (rec.getDynamodb.getNewImage ne null) {
                newMap.putAll(rec.getDynamodb.getNewImage)
              }

              newMap.putAll(rec.getDynamodb.getKeys)

              val operationType =
                rec.getEventName match {
                  case "INSERT" | "MODIFY" => putOperation
                  case "REMOVE"            => deleteOperation
                }
              newMap.put(operationTypeColumn, operationType)
              Some(newMap)

            case _ => None
          },
          kinesisCreds = kinesisCreds
        ).foreachRDD { msgs =>
          run(msgs, target, renamesMap, targetTableDesc, ddbStreamsMetrics)(spark)
        }

      case kinesis: StreamChangesSetting.KinesisDataStreams =>
        val initialPosition = kinesis.initialTimestamp match {
          case Some(ts) =>
            log.info(s"Kinesis initial position: AT_TIMESTAMP=$ts (from config)")
            new KinesisInitialPositions.AtTimestamp(java.util.Date.from(ts))
          case None =>
            log.info(
              s"Kinesis initial position: AT_TIMESTAMP=$snapshotStartTime " +
                "(defaulted to snapshot start time)"
            )
            new KinesisInitialPositions.AtTimestamp(java.util.Date.from(snapshotStartTime))
        }

        val appName = kinesis.appName.getOrElse(defaultCheckpointAppName(src))
        log.info(s"Kinesis KCL application (checkpoint) name: $appName")

        // Allocate ONCE outside foreachRDD; see [[Metrics]] scaladoc for the leak it prevents.
        val kinesisMetrics = new Metrics(spark)
        new KinesisStreamInputDStream(
          ssc               = streamingContext,
          streamNameOrArn   = kinesis.streamArn,
          regionName        = src.region.orNull,
          initialPosition   = initialPosition,
          checkpointAppName = appName,
          messageHandler = { rec =>
            // Count silent drops so a stream of malformed KDS payloads surfaces in the
            // per-batch log line instead of vanishing.
            val parsed = KinesisJsonDeserializer.parseRecord(rec)
            if (parsed.isEmpty) kinesisMetrics.droppedRecordsCount.add(1)
            parsed
          },
          kinesisCreds = kinesisCreds
        ).foreachRDD { msgs =>
          run(msgs, target, renamesMap, targetTableDesc, kinesisMetrics)(spark)
        }
    }
  }

}
