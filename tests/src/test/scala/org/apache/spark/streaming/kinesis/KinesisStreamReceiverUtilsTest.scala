package org.apache.spark.streaming.kinesis

/** Tests for small utility helpers that live on [[KinesisStreamReceiver]]'s companion.
  *
  * This test file lives in the `org.apache.spark.streaming.kinesis` package intentionally so it can
  * reach the `private[kinesis]` companion without forcing those helpers to be public.
  */
class KinesisStreamReceiverUtilsTest extends munit.FunSuite {

  test("extractStreamName: bare name is returned unchanged") {
    assertEquals(KinesisStreamReceiver.extractStreamName("my-stream"), "my-stream")
  }

  test("extractStreamName: full ARN is reduced to the stream name") {
    assertEquals(
      KinesisStreamReceiver.extractStreamName(
        "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
      ),
      "my-stream"
    )
  }

  test("extractStreamName: ARN with dashes and slashes in the stream name") {
    assertEquals(
      KinesisStreamReceiver.extractStreamName(
        "arn:aws:kinesis:eu-west-1:000000000000:stream/ddb-to-kinesis-prod-123"
      ),
      "ddb-to-kinesis-prod-123"
    )
  }

  test("extractStreamName: malformed ARN with no slash falls back to the input") {
    val weird = "arn:aws:kinesis:us-east-1:123:stream"
    // No ':stream/<name>' segment means the regex does not match, and we return the input
    // unchanged rather than raising — the receiver then surfaces the real error from AWS on
    // first use with a more actionable message.
    assertEquals(KinesisStreamReceiver.extractStreamName(weird), weird)
  }

  test("extractStreamName: enhanced-fan-out consumer ARN returns the stream name") {
    // Consumer ARN shape: arn:aws:kinesis:<r>:<acct>:stream/<stream>/consumer/<name>:<ts>.
    // The old lastIndexOf('/') heuristic returned the consumer name here (bug) — this test pins
    // the fix so a future refactor cannot silently reintroduce it.
    val arn =
      "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream/consumer/my-consumer:1699999999"
    assertEquals(KinesisStreamReceiver.extractStreamName(arn), "my-stream")
  }

  test("extractStreamName: cross-account stream ARN with dot in stream name") {
    val arn = "arn:aws:kinesis:eu-west-1:987654321098:stream/prod.events"
    assertEquals(KinesisStreamReceiver.extractStreamName(arn), "prod.events")
  }
}
