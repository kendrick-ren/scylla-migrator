package com.scylladb.migrator.writers

import com.amazonaws.services.dynamodbv2.model.{ AttributeValue => AttributeValueV1 }
import com.amazonaws.services.kinesis.model.Record
import io.circe.parser

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.jdk.CollectionConverters._

/** Unit tests for [[KinesisJsonDeserializer]].
  *
  * These tests are fully local (no AWS / LocalStack dependency): they construct realistic Kinesis
  * Data Streams for DynamoDB JSON payloads, wrap them in an AWS SDK v1
  * [[com.amazonaws.services.kinesis.model.Record]], and verify the produced
  * [[DynamoStreamReplication.DynamoItem]] matches the shape emitted by the DynamoDB Streams path.
  */
class KinesisJsonDeserializerTest extends munit.FunSuite {

  private def makeRecord(json: String): Record = {
    val rec = new Record()
    rec.setData(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)))
    rec.setPartitionKey("pk")
    rec.setSequenceNumber("0")
    rec
  }

  private val insertJson =
    """{
      |  "awsRegion": "us-east-1",
      |  "eventID": "abc123",
      |  "eventName": "INSERT",
      |  "recordFormat": "application/json",
      |  "tableName": "MyTable",
      |  "dynamodb": {
      |    "ApproximateCreationDateTime": 1609459200000,
      |    "Keys": { "pk": {"S": "key1"} },
      |    "NewImage": {
      |      "pk":   {"S": "key1"},
      |      "name": {"S": "Alice"},
      |      "age":  {"N": "30"}
      |    },
      |    "SizeBytes": 30
      |  },
      |  "eventSource": "aws:dynamodb"
      |}""".stripMargin

  private val modifyJson =
    """{
      |  "awsRegion": "us-east-1",
      |  "eventID": "mod-1",
      |  "eventName": "MODIFY",
      |  "recordFormat": "application/json",
      |  "tableName": "MyTable",
      |  "dynamodb": {
      |    "Keys": { "pk": {"S": "key1"} },
      |    "NewImage": {
      |      "pk":   {"S": "key1"},
      |      "name": {"S": "Bob"}
      |    }
      |  },
      |  "eventSource": "aws:dynamodb"
      |}""".stripMargin

  private val removeJson =
    """{
      |  "awsRegion": "us-east-1",
      |  "eventID": "rem-1",
      |  "eventName": "REMOVE",
      |  "recordFormat": "application/json",
      |  "tableName": "MyTable",
      |  "dynamodb": {
      |    "Keys": { "pk": {"S": "key1"} }
      |  },
      |  "eventSource": "aws:dynamodb"
      |}""".stripMargin

  test("INSERT: merges NewImage and Keys and tags with put marker") {
    val maybeItem = KinesisJsonDeserializer.parseRecord(makeRecord(insertJson))
    assert(maybeItem.isDefined, "parseRecord returned None for a valid INSERT payload")

    val item = maybeItem.get.asScala
    assertEquals(item("pk").getS, "key1")
    assertEquals(item("name").getS, "Alice")
    assertEquals(item("age").getN, "30")
    assertEquals(
      item(DynamoStreamReplication.operationTypeColumn),
      DynamoStreamReplication.putOperation
    )
  }

  test("MODIFY is marked as a put operation") {
    val item = KinesisJsonDeserializer
      .parseRecord(makeRecord(modifyJson))
      .getOrElse(fail("parseRecord returned None for a valid MODIFY payload"))
      .asScala

    assertEquals(item("name").getS, "Bob")
    assertEquals(
      item(DynamoStreamReplication.operationTypeColumn),
      DynamoStreamReplication.putOperation
    )
  }

  test("REMOVE contains only Keys and is marked as a delete operation") {
    val item = KinesisJsonDeserializer
      .parseRecord(makeRecord(removeJson))
      .getOrElse(fail("parseRecord returned None for a valid REMOVE payload"))
      .asScala

    // No attribute beyond the key + op-type marker
    assertEquals(item.size, 2, s"Expected just pk + op marker, got ${item.keySet}")
    assertEquals(item("pk").getS, "key1")
    assertEquals(
      item(DynamoStreamReplication.operationTypeColumn),
      DynamoStreamReplication.deleteOperation
    )
  }

  test("Unknown eventName produces None") {
    val weirdJson = insertJson.replace("INSERT", "NOT_A_REAL_EVENT")
    assertEquals(KinesisJsonDeserializer.parseRecord(makeRecord(weirdJson)), None)
  }

  test("Non-JSON payload produces None and does not throw") {
    val rec = new Record()
    rec.setData(ByteBuffer.wrap("not-json".getBytes(StandardCharsets.UTF_8)))
    assertEquals(KinesisJsonDeserializer.parseRecord(rec), None)
  }

  test("Missing dynamodb field produces None") {
    val json = """{"eventName": "INSERT", "recordFormat": "application/json"}"""
    assertEquals(KinesisJsonDeserializer.parseRecord(makeRecord(json)), None)
  }

  // --- jsonToAttributeValue: all 10 DynamoDB type descriptors ---

  private def attrJson(raw: String): io.circe.Json =
    parser.parse(raw).getOrElse(throw new AssertionError(s"bad test JSON: $raw"))

  private def ok(
    raw: String
  )(check: AttributeValueV1 => Unit): Unit =
    KinesisJsonDeserializer.jsonToAttributeValue(attrJson(raw)) match {
      case Right(av) => check(av)
      case Left(err) => fail(s"Expected Right, got Left($err) for payload $raw")
    }

  test("jsonToAttributeValue: S") {
    ok("""{"S": "hello"}""") { av =>
      assertEquals(av.getS, "hello")
    }
  }

  test("jsonToAttributeValue: N") {
    ok("""{"N": "3.14159"}""") { av =>
      assertEquals(av.getN, "3.14159")
    }
  }

  test("jsonToAttributeValue: B (base64 decoded)") {
    val raw = Array[Byte](0, 1, 2, 3, 4, 5)
    val b64 = Base64.getEncoder.encodeToString(raw)
    ok(s"""{"B": "$b64"}""") { av =>
      val bb = av.getB
      val got = new Array[Byte](bb.remaining())
      bb.duplicate().get(got)
      assertEquals(got.toSeq, raw.toSeq)
    }
  }

  test("jsonToAttributeValue: BOOL true") {
    ok("""{"BOOL": true}""") { av =>
      assertEquals(av.getBOOL, java.lang.Boolean.TRUE)
    }
  }

  test("jsonToAttributeValue: BOOL false") {
    ok("""{"BOOL": false}""") { av =>
      assertEquals(av.getBOOL, java.lang.Boolean.FALSE)
    }
  }

  test("jsonToAttributeValue: NULL") {
    ok("""{"NULL": true}""") { av =>
      assertEquals(av.getNULL, java.lang.Boolean.TRUE)
    }
  }

  test("jsonToAttributeValue: SS") {
    ok("""{"SS": ["a", "b", "c"]}""") { av =>
      assertEquals(av.getSS.asScala.toList, List("a", "b", "c"))
    }
  }

  test("jsonToAttributeValue: NS") {
    ok("""{"NS": ["1", "2", "3"]}""") { av =>
      assertEquals(av.getNS.asScala.toList, List("1", "2", "3"))
    }
  }

  test("jsonToAttributeValue: BS (base64 decoded per element)") {
    val raw1 = Array[Byte](1, 2, 3)
    val raw2 = Array[Byte](4, 5, 6)
    val b64_1 = Base64.getEncoder.encodeToString(raw1)
    val b64_2 = Base64.getEncoder.encodeToString(raw2)
    ok(s"""{"BS": ["$b64_1", "$b64_2"]}""") { av =>
      val decoded = av.getBS.asScala.toList.map { bb =>
        val arr = new Array[Byte](bb.remaining())
        bb.duplicate().get(arr)
        arr.toSeq
      }
      assertEquals(decoded, List(raw1.toSeq, raw2.toSeq))
    }
  }

  test("jsonToAttributeValue: L (list) with mixed inner types") {
    ok("""{"L": [ {"S": "s"}, {"N": "1"}, {"BOOL": true} ]}""") { av =>
      val list = av.getL.asScala.toList
      assertEquals(list.size, 3)
      assertEquals(list(0).getS, "s")
      assertEquals(list(1).getN, "1")
      assertEquals(list(2).getBOOL, java.lang.Boolean.TRUE)
    }
  }

  test("jsonToAttributeValue: M (nested map)") {
    ok("""{"M": {"inner": {"S": "v"}, "flag": {"BOOL": false}}}""") { av =>
      val m = av.getM.asScala
      assertEquals(m("inner").getS, "v")
      assertEquals(m("flag").getBOOL, java.lang.Boolean.FALSE)
    }
  }

  test("jsonToAttributeValue: unknown descriptor is rejected") {
    val result = KinesisJsonDeserializer.jsonToAttributeValue(attrJson("""{"X": "huh"}"""))
    assert(result.isLeft, s"Expected Left, got $result")
  }

  test("jsonToAttributeValue: invalid base64 for B is rejected") {
    val result =
      KinesisJsonDeserializer.jsonToAttributeValue(attrJson("""{"B": "NOT!VALID@BASE64"}"""))
    assert(result.isLeft, s"Expected Left, got $result")
  }

  test("End-to-end: INSERT with binary attribute") {
    val raw = Array[Byte](10, 20, 30)
    val b64 = Base64.getEncoder.encodeToString(raw)
    val json = s"""{
        "eventName": "INSERT",
        "dynamodb": {
          "Keys": { "pk": {"S": "k"} },
          "NewImage": {
            "pk":  {"S": "k"},
            "bin": {"B": "$b64"}
          }
        }
      }"""

    val item = KinesisJsonDeserializer
      .parseRecord(makeRecord(json))
      .getOrElse(fail("parseRecord returned None"))
      .asScala

    val bb = item("bin").getB
    val got = new Array[Byte](bb.remaining())
    bb.duplicate().get(got)
    assertEquals(got.toSeq, raw.toSeq)
  }

  test("operation-type marker is stable across releases (cross-module contract)") {
    // [[DynamoStreamReplication.operationTypeColumn]], [[putOperation]], and [[deleteOperation]]
    // are package-public. They are PRODUCED by [[KinesisJsonDeserializer.parseRecord]] via
    // [[DynamoStreamReplication.createDStream]]'s messageHandler, and CONSUMED by
    // [[DynamoStreamReplication.run]]. If a future refactor silently renames either the column
    // key or flips the boolean, every put would become a delete in the target table. Pin the
    // literal values here so the break shows up as a failing test rather than as data loss.
    assertEquals(DynamoStreamReplication.operationTypeColumn, "_dynamo_op_type")
    assertEquals(DynamoStreamReplication.putOperation.getBOOL, java.lang.Boolean.TRUE)
    assertEquals(DynamoStreamReplication.deleteOperation.getBOOL, java.lang.Boolean.FALSE)
  }
}
