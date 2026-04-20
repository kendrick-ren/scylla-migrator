package com.scylladb.migrator.config

import io.circe.{ Decoder, DecodingFailure, Encoder, HCursor, Json }

import java.time.Instant

/** Describes how (or whether) to replicate live changes from a DynamoDB source table to the target.
  * It supports three variants:
  *
  *   - [[StreamChangesSetting.Disabled]] -- do not replicate any changes (batch migration only)
  *   - [[StreamChangesSetting.DynamoDBStreams]] -- use the source table's DynamoDB Stream. This is
  *     the original, backward-compatible behavior; its retention is capped at 24 hours.
  *   - [[StreamChangesSetting.KinesisDataStreams]] -- use a pre-existing Kinesis Data Stream that
  *     the source table emits change events to. Retention can be up to 1 year and the consumer
  *     supports `AT_TIMESTAMP` for snapshots > 24h.
  *
  * The YAML decoder is backward-compatible with the previous `streamChanges: Boolean` form: `true`
  * maps to [[DynamoDBStreams]] and `false` maps to [[Disabled]].
  */
sealed trait StreamChangesSetting {
  def isEnabled: Boolean
}

object StreamChangesSetting {
  case object Disabled extends StreamChangesSetting {
    val isEnabled: Boolean = false
  }

  case object DynamoDBStreams extends StreamChangesSetting {
    val isEnabled: Boolean = true
  }

  /** @param streamArn
    *   Full ARN of the Kinesis Data Stream that the DynamoDB source table emits change records to
    *   (e.g. `arn:aws:kinesis:us-east-1:123456789012:stream/my-stream`). Bare stream names are NOT
    *   accepted: DynamoDB's `EnableKinesisStreamingDestination` API strictly requires a full ARN,
    *   and the migrator has no account-id / region context with which to synthesize one. The stream
    *   must be pre-created; the migrator will ensure the source table's Kinesis streaming
    *   destination is enabled for this stream.
    * @param initialTimestamp
    *   Optional ISO-8601 instant used as the KCL `AT_TIMESTAMP` initial position. When not
    *   provided, the migrator defaults this to the moment snapshot transfer starts (so changes that
    *   land during the snapshot are replayed afterwards).
    * @param appName
    *   Optional override for the KCL application name (used for the lease / checkpoint table).
    *   Defaults to a deterministic value derived from the source table name.
    */
  case class KinesisDataStreams(
    streamArn: String,
    initialTimestamp: Option[Instant] = None,
    appName: Option[String] = None
  ) extends StreamChangesSetting {
    val isEnabled: Boolean = true
  }

  implicit val decoder: Decoder[StreamChangesSetting] = Decoder.instance { cursor =>
    val json = cursor.value
    if (json.isNull) Right(Disabled)
    else
      json.asBoolean match {
        case Some(true)  => Right(DynamoDBStreams)
        case Some(false) => Right(Disabled)
        case None =>
          if (json.isObject) decodeObject(cursor)
          else
            Left(
              DecodingFailure(
                "streamChanges must be a boolean or an object with a 'type' field",
                cursor.history
              )
            )
      }
  }

  private def decodeObject(cursor: HCursor): Decoder.Result[StreamChangesSetting] =
    cursor.get[String]("type").flatMap {
      case "dynamodb-streams" | "dynamodb" | "dynamo" =>
        Right(DynamoDBStreams)
      case "disabled" | "none" =>
        Right(Disabled)
      case "kinesis" | "kinesis-data-streams" =>
        for {
          streamArn <- cursor.get[String]("streamArn").flatMap { arn =>
                         val trimmed = arn.trim
                         // Deliberately narrow: standard AWS partition only. Relax the prefix to
                         // `arn:aws` (no trailing colon) if/when we need to support the AWS China
                         // or AWS GovCloud partitions.
                         if (trimmed.startsWith("arn:aws:kinesis:") && trimmed.contains(":stream/"))
                           Right(trimmed)
                         else
                           Left(
                             DecodingFailure(
                               s"streamChanges.streamArn must be a full Kinesis ARN " +
                                 "(e.g. arn:aws:kinesis:<region>:<account>:stream/<name>). " +
                                 "Bare stream names are not accepted because DynamoDB's " +
                                 "EnableKinesisStreamingDestination API requires an ARN. " +
                                 s"Got: '$arn'",
                               cursor.history
                             )
                           )
                       }
          initialTimestamp <- cursor.getOrElse[Option[Instant]]("initialTimestamp")(None)
          appName          <- cursor.getOrElse[Option[String]]("appName")(None)
        } yield KinesisDataStreams(streamArn, initialTimestamp, appName)
      case other =>
        Left(DecodingFailure(s"Unknown streamChanges type: '$other'", cursor.history))
    }

  implicit val encoder: Encoder[StreamChangesSetting] = Encoder.instance {
    case Disabled        => Json.False
    case DynamoDBStreams => Json.True
    case KinesisDataStreams(arn, ts, appName) =>
      val baseFields: List[(String, Json)] = List(
        "type"      -> Json.fromString("kinesis"),
        "streamArn" -> Json.fromString(arn)
      )
      val tsField = ts.map(t => "initialTimestamp" -> Json.fromString(t.toString)).toList
      val appField = appName.map(n => "appName" -> Json.fromString(n)).toList
      Json.obj(baseFields ++ tsField ++ appField: _*)
  }
}
