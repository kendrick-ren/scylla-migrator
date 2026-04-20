package com.scylladb.migrator.config

import io.circe.{ parser, DecodingFailure, Json }
import io.circe.syntax._
import io.circe.yaml

import java.time.Instant

/** Tests for [[StreamChangesSetting]] YAML/JSON parsing.
  *
  * Covers:
  *
  *   - Backward-compatible boolean form (`streamChanges: true` / `false`).
  *   - Object form with `type: kinesis` (with and without optional fields).
  *   - Unknown / invalid inputs.
  *   - Round-trip encode/decode.
  *   - Full [[TargetSettings.DynamoDB]] YAML decoding with the new streamChanges.
  */
class StreamChangesSettingParserTest extends munit.FunSuite {

  private def parseSetting(rawJson: String): StreamChangesSetting =
    parser.parse(rawJson).flatMap(_.as[StreamChangesSetting]) match {
      case Right(s)  => s
      case Left(err) => fail(s"Expected successful decode, got: $err")
    }

  test("legacy boolean 'true' is decoded as DynamoDBStreams") {
    assertEquals(parseSetting("true"), StreamChangesSetting.DynamoDBStreams)
  }

  test("legacy boolean 'false' is decoded as Disabled") {
    assertEquals(parseSetting("false"), StreamChangesSetting.Disabled)
  }

  test("null is decoded as Disabled") {
    assertEquals(parseSetting("null"), StreamChangesSetting.Disabled)
  }

  test("object form 'dynamodb-streams' is decoded as DynamoDBStreams") {
    assertEquals(
      parseSetting("""{"type": "dynamodb-streams"}"""),
      StreamChangesSetting.DynamoDBStreams
    )
  }

  test("object form 'disabled' is decoded as Disabled") {
    assertEquals(parseSetting("""{"type": "disabled"}"""), StreamChangesSetting.Disabled)
  }

  test("object form 'kinesis' with only streamArn") {
    val setting = parseSetting(
      """{"type": "kinesis", "streamArn": "arn:aws:kinesis:us-east-1:123:stream/my-stream"}"""
    )
    assertEquals(
      setting,
      StreamChangesSetting.KinesisDataStreams(
        streamArn        = "arn:aws:kinesis:us-east-1:123:stream/my-stream",
        initialTimestamp = None,
        appName          = None
      )
    )
  }

  test("object form 'kinesis' with all optional fields") {
    val setting = parseSetting(
      """{
        |  "type": "kinesis",
        |  "streamArn": "arn:aws:kinesis:us-east-1:123:stream/my-stream",
        |  "initialTimestamp": "2026-04-01T00:00:00Z",
        |  "appName": "my-app"
        |}""".stripMargin
    )
    assertEquals(
      setting,
      StreamChangesSetting.KinesisDataStreams(
        streamArn        = "arn:aws:kinesis:us-east-1:123:stream/my-stream",
        initialTimestamp = Some(Instant.parse("2026-04-01T00:00:00Z")),
        appName          = Some("my-app")
      )
    )
  }

  test("aliased 'kinesis-data-streams' is also accepted") {
    // Uses a full ARN because the decoder now rejects bare stream names up front (see
    // StreamChangesSetting.decoder). The point of this test is only the type alias, not the
    // streamArn shape, so any syntactically valid ARN is fine.
    val setting = parseSetting(
      """{"type": "kinesis-data-streams", "streamArn": "arn:aws:kinesis:us-east-1:123:stream/my-stream"}"""
    )
    assert(setting.isInstanceOf[StreamChangesSetting.KinesisDataStreams])
  }

  test("bare stream name (without 'arn:aws:kinesis:...:stream/...' prefix) is rejected") {
    // Pins the contract documented in StreamChangesSetting.KinesisDataStreams.streamArn scaladoc:
    // DynamoDB's EnableKinesisStreamingDestination API requires a full ARN, and the migrator has
    // no account-id / region context to synthesize one from a bare name, so we fail fast at
    // decode time with an actionable error rather than deep inside the AWS SDK at runtime.
    val result = parser
      .parse("""{"type": "kinesis", "streamArn": "my-stream"}""")
      .flatMap(_.as[StreamChangesSetting])
    assert(result.isLeft, s"Expected Left, got $result")
    result.left.foreach { err =>
      assert(err.isInstanceOf[DecodingFailure])
      assert(
        err.getMessage.contains("must be a full Kinesis ARN"),
        s"Expected message about full Kinesis ARN, got: ${err.getMessage}"
      )
    }
  }

  test("unknown object type produces a DecodingFailure") {
    val result = parser.parse("""{"type": "firehose"}""").flatMap(_.as[StreamChangesSetting])
    assert(result.isLeft, s"Expected Left, got $result")
    result.left.foreach { err =>
      assert(err.isInstanceOf[DecodingFailure])
      assert(err.getMessage.contains("Unknown streamChanges type"))
    }
  }

  test("kinesis type without streamArn produces a DecodingFailure") {
    val result = parser.parse("""{"type": "kinesis"}""").flatMap(_.as[StreamChangesSetting])
    assert(result.isLeft)
  }

  test("non-boolean scalar (number/string) is rejected") {
    val result1 = parser.parse("""42""").flatMap(_.as[StreamChangesSetting])
    val result2 = parser.parse(""""hello"""").flatMap(_.as[StreamChangesSetting])
    assert(result1.isLeft)
    assert(result2.isLeft)
  }

  // Round-trip tests

  test("round-trip: DynamoDBStreams encodes as boolean true") {
    val encoded = (StreamChangesSetting.DynamoDBStreams: StreamChangesSetting).asJson
    assertEquals(encoded, Json.True)
    assertEquals(encoded.as[StreamChangesSetting], Right(StreamChangesSetting.DynamoDBStreams))
  }

  test("round-trip: Disabled encodes as boolean false") {
    val encoded = (StreamChangesSetting.Disabled: StreamChangesSetting).asJson
    assertEquals(encoded, Json.False)
    assertEquals(encoded.as[StreamChangesSetting], Right(StreamChangesSetting.Disabled))
  }

  test("round-trip: KinesisDataStreams preserves all fields") {
    val original = StreamChangesSetting.KinesisDataStreams(
      streamArn        = "arn:aws:kinesis:us-east-1:123:stream/t",
      initialTimestamp = Some(Instant.parse("2026-04-13T10:30:00Z")),
      appName          = Some("app")
    )
    val encoded = (original: StreamChangesSetting).asJson
    assertEquals(encoded.as[StreamChangesSetting], Right(original))
  }

  // --- TargetSettings.DynamoDB decoding with streamChanges variants ---

  private val yamlDynamoCommon =
    """type: dynamodb
      |region: us-east-1
      |table: my-table
      |writeThroughput: null
      |throughputWritePercent: null
      |skipInitialSnapshotTransfer: null
      |""".stripMargin

  private def parseDynamoTarget(y: String): TargetSettings.DynamoDB =
    yaml.parser.parse(y).flatMap(_.as[TargetSettings]) match {
      case Right(d: TargetSettings.DynamoDB) => d
      case Right(other)                      => fail(s"Expected DynamoDB target, got $other")
      case Left(err)                         => fail(s"Decoding failed: $err")
    }

  test("TargetSettings.DynamoDB: legacy 'streamChanges: true' still works") {
    val target = parseDynamoTarget(yamlDynamoCommon + "streamChanges: true\n")
    assertEquals(target.streamChanges, StreamChangesSetting.DynamoDBStreams: StreamChangesSetting)
  }

  test("TargetSettings.DynamoDB: legacy 'streamChanges: false' still works") {
    val target = parseDynamoTarget(yamlDynamoCommon + "streamChanges: false\n")
    assertEquals(target.streamChanges, StreamChangesSetting.Disabled: StreamChangesSetting)
  }

  test("TargetSettings.DynamoDB: new Kinesis form parses") {
    val y = yamlDynamoCommon +
      """streamChanges:
        |  type: kinesis
        |  streamArn: arn:aws:kinesis:us-east-1:123:stream/t
        |  initialTimestamp: "2026-04-01T00:00:00Z"
        |""".stripMargin
    val target = parseDynamoTarget(y)
    target.streamChanges match {
      case StreamChangesSetting.KinesisDataStreams(arn, ts, appName) =>
        assertEquals(arn, "arn:aws:kinesis:us-east-1:123:stream/t")
        assertEquals(ts, Some(Instant.parse("2026-04-01T00:00:00Z")))
        assertEquals(appName, None)
      case other =>
        fail(s"Expected KinesisDataStreams, got $other")
    }
  }

  test("TargetSettings.DynamoDB: invalid streamChanges type produces a DecodingFailure") {
    val y = yamlDynamoCommon +
      """streamChanges:
        |  type: bogus
        |""".stripMargin
    val result = yaml.parser.parse(y).flatMap(_.as[TargetSettings])
    assert(result.isLeft, s"Expected Left, got $result")
  }
}
