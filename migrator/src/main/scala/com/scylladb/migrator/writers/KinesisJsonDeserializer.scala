package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.amazonaws.services.kinesis.model.Record
import io.circe.{ Json, JsonObject }
import io.circe.parser
import org.apache.logging.log4j.LogManager

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64
import scala.jdk.CollectionConverters._

/** Deserializer for Amazon Kinesis Data Streams records emitted by a DynamoDB table that has been
  * configured with a Kinesis streaming destination.
  *
  * Each record is a UTF-8 JSON document shaped roughly like:
  *
  * {{{
  *   {
  *     "awsRegion": "us-east-1",
  *     "eventID": "...",
  *     "eventName": "INSERT" | "MODIFY" | "REMOVE",
  *     "recordFormat": "application/json",
  *     "tableName": "...",
  *     "dynamodb": {
  *       "ApproximateCreationDateTime": 1609459200000,
  *       "Keys":     { "pk": {"S": "..."} },
  *       "NewImage": { "pk": {"S": "..."}, "data": {"N": "42"} },
  *       "OldImage": { ... },
  *       "SizeBytes": 123
  *     },
  *     "eventSource": "aws:dynamodb"
  *   }
  * }}}
  *
  * Binary attributes (`B` / `BS`) are base64-encoded in the JSON (unlike the SDK v1 typed path used
  * by the DynamoDB-Streams adapter, where `ByteBuffer`s are already decoded).
  *
  * To match the behavior of [[DynamoStreamReplication]]'s DynamoDB-Streams message handler, this
  * deserializer merges `dynamodb.NewImage` (if present) with `dynamodb.Keys` into a single
  * [[DynamoStreamReplication.DynamoItem]] and tags it with a synthetic `_dynamo_op_type` column
  * (true = put/upsert, false = delete). Unknown / malformed records return `None` so the streaming
  * job does not crash on a single bad record.
  */
object KinesisJsonDeserializer {
  private val log = LogManager.getLogger("com.scylladb.migrator.writers.KinesisJsonDeserializer")

  /** Parse the body of a Kinesis record into a [[DynamoStreamReplication.DynamoItem]].
    *
    * Returns `None` if the record cannot be parsed or has an unrecognized event name.
    */
  def parseRecord(record: Record): Option[DynamoStreamReplication.DynamoItem] =
    Option(record).flatMap { r =>
      val byteBuffer = r.getData
      if (byteBuffer == null) None
      else {
        val jsonStr = StandardCharsets.UTF_8.decode(byteBuffer.duplicate()).toString
        parser.parse(jsonStr) match {
          case Right(json) =>
            parseJson(json) match {
              case Right(item) => Some(item)
              case Left(err) =>
                log.warn(s"Dropping malformed Kinesis record: $err; payload=$jsonStr")
                None
            }
          case Left(err) =>
            log.warn(s"Dropping non-JSON Kinesis record: ${err.getMessage}")
            None
        }
      }
    }

  /** Parse a top-level Kinesis Data Streams for DynamoDB event JSON into a `DynamoItem`.
    *
    * Visible for testing.
    */
  def parseJson(json: Json): Either[String, DynamoStreamReplication.DynamoItem] = {
    val cursor = json.hcursor
    cursor.get[String]("eventName").left.map(_.message).flatMap { eventName =>
      val operationType = eventName match {
        case "INSERT" | "MODIFY" => Right(DynamoStreamReplication.putOperation)
        case "REMOVE"            => Right(DynamoStreamReplication.deleteOperation)
        case other               => Left(s"Unknown eventName: '$other'")
      }
      operationType.flatMap { op =>
        val dynamodb = cursor.downField("dynamodb")
        if (dynamodb.failed)
          Left("Missing 'dynamodb' field")
        else {
          val merged = new util.HashMap[String, AttributeValueV1]()
          for {
            _ <- addImageTo(merged, dynamodb.downField("NewImage").focus)
            _ <- addImageTo(merged, dynamodb.downField("Keys").focus)
          } yield {
            merged.put(DynamoStreamReplication.operationTypeColumn, op)
            merged
          }
        }
      }
    }
  }

  private def addImageTo(
    target: util.Map[String, AttributeValueV1],
    maybeJson: Option[Json]
  ): Either[String, Unit] =
    maybeJson match {
      case None => Right(())
      case Some(json) if json.isNull =>
        Right(())
      case Some(json) =>
        json.asObject match {
          case None => Left(s"Expected object, got ${jsonKind(json)}")
          case Some(obj) =>
            obj.toMap.foldLeft[Either[String, Unit]](Right(())) {
              case (Right(_), (attr, v)) =>
                jsonToAttributeValue(v).map { av => target.put(attr, av); () }
              case (l @ Left(_), _) => l
            }
        }
    }

  /** Convert a DynamoDB JSON attribute value (e.g. `{"S":"foo"}`) to an SDK v1
    * [[AttributeValueV1]].
    *
    * Visible for testing.
    */
  def jsonToAttributeValue(json: Json): Either[String, AttributeValueV1] =
    json.asObject match {
      case None => Left(s"Expected attribute object, got ${jsonKind(json)}")
      case Some(obj) =>
        obj.toList match {
          case Nil =>
            Left("Empty attribute object")
          case (descriptor, value) :: _ =>
            convertDescriptor(descriptor, value)
        }
    }

  private def convertDescriptor(
    descriptor: String,
    value: Json
  ): Either[String, AttributeValueV1] =
    descriptor match {
      case "S" =>
        value.asString
          .toRight(s"Expected string for 'S' descriptor, got ${jsonKind(value)}")
          .map(new AttributeValueV1().withS(_))

      case "N" =>
        // DynamoDB encodes numbers as strings to preserve precision.
        value.asString
          .toRight(s"Expected string for 'N' descriptor, got ${jsonKind(value)}")
          .map(new AttributeValueV1().withN(_))

      case "B" =>
        value.asString
          .toRight(s"Expected base64 string for 'B' descriptor, got ${jsonKind(value)}")
          .flatMap(decodeBase64)
          .map(bytes => new AttributeValueV1().withB(ByteBuffer.wrap(bytes)))

      case "BOOL" =>
        value.asBoolean
          .toRight(s"Expected boolean for 'BOOL' descriptor, got ${jsonKind(value)}")
          .map(b => new AttributeValueV1().withBOOL(java.lang.Boolean.valueOf(b)))

      case "NULL" =>
        // The convention is `{"NULL": true}`; we accept any value here and ignore it.
        Right(new AttributeValueV1().withNULL(java.lang.Boolean.TRUE))

      case "SS" =>
        extractStringArray(value, "SS").map(xs => new AttributeValueV1().withSS(xs.asJava))

      case "NS" =>
        extractStringArray(value, "NS").map(xs => new AttributeValueV1().withNS(xs.asJava))

      case "BS" =>
        extractStringArray(value, "BS").flatMap { base64Strs =>
          val buffersOrError =
            base64Strs.foldRight[Either[String, List[ByteBuffer]]](Right(Nil)) {
              case (str, Right(acc)) => decodeBase64(str).map(bs => ByteBuffer.wrap(bs) :: acc)
              case (_, l @ Left(_))  => l
            }
          buffersOrError.map(bs => new AttributeValueV1().withBS(bs.asJava))
        }

      case "L" =>
        value.asArray match {
          case None =>
            Left(s"Expected array for 'L' descriptor, got ${jsonKind(value)}")
          case Some(arr) =>
            arr
              .foldRight[Either[String, List[AttributeValueV1]]](Right(Nil)) {
                case (j, Right(acc))  => jsonToAttributeValue(j).map(_ :: acc)
                case (_, l @ Left(_)) => l
              }
              .map(xs => new AttributeValueV1().withL(xs.asJava))
        }

      case "M" =>
        value.asObject match {
          case None =>
            Left(s"Expected object for 'M' descriptor, got ${jsonKind(value)}")
          case Some(obj) =>
            val builder = new util.HashMap[String, AttributeValueV1]()
            obj.toList
              .foldLeft[Either[String, Unit]](Right(())) {
                case (Right(_), (k, v)) =>
                  jsonToAttributeValue(v).map { av => builder.put(k, av); () }
                case (l @ Left(_), _) => l
              }
              .map(_ => new AttributeValueV1().withM(builder))
        }

      case other =>
        Left(s"Unknown AttributeValue descriptor: '$other'")
    }

  private def extractStringArray(value: Json, descriptor: String): Either[String, List[String]] =
    value.asArray match {
      case None =>
        Left(s"Expected array for '$descriptor' descriptor, got ${jsonKind(value)}")
      case Some(arr) =>
        arr
          .foldRight[Either[String, List[String]]](Right(Nil)) {
            case (j, Right(acc)) =>
              j.asString
                .toRight(s"Expected string element in '$descriptor' array, got ${jsonKind(j)}")
                .map(_ :: acc)
            case (_, l @ Left(_)) => l
          }
    }

  private def decodeBase64(s: String): Either[String, Array[Byte]] =
    try Right(Base64.getDecoder.decode(s))
    catch {
      case _: IllegalArgumentException => Left(s"Invalid base64 value: '$s'")
    }

  private def jsonKind(json: Json): String =
    json.fold(
      jsonNull    = "null",
      jsonBoolean = _ => "boolean",
      jsonNumber  = _ => "number",
      jsonString  = _ => "string",
      jsonArray   = _ => "array",
      jsonObject  = _ => "object"
    )
}
