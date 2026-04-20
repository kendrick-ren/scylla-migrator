==============
Stream Changes
==============

Instead of terminating immediately after having copied a snapshot of the source table, the migrator can also keep running and endlessly replicate the changes applied to the source table as they arrive. This feature is only supported when :doc:`reading from DynamoDB and writing to ScyllaDB Alternator </migrate-from-dynamodb>`.

Two modes are supported for capturing source changes:

* **DynamoDB Streams** (default, backward-compatible).
  The migrator enables the source table's built-in `DynamoDB Stream <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html>`_ and consumes it through the `DynamoDB Streams Kinesis Adapter <https://github.com/awslabs/dynamodb-streams-kinesis-adapter>`_.
* **Kinesis Data Streams for DynamoDB** (recommended for initial snapshots > 24 hours).
  The migrator uses a pre-existing `Kinesis Data Stream destination <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/kds.html>`_ that the source table emits change records to. Kinesis offers up to 1 year of retention and ``AT_TIMESTAMP`` replay, which removes the 24-hour limit that DynamoDB Streams imposes on the initial snapshot transfer.

DynamoDB Streams (default)
==========================

Enable this feature by setting ``streamChanges`` to ``true`` in the target database configuration:

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    # ... Full configuration not repeated here for the sake of brevity
    # ...
    # Enable the feature
    streamChanges: true

Equivalently, you can use the explicit object form:

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    streamChanges:
      type: dynamodb-streams

In this mode, the migrator has to be interrupted manually with ``Control`` + ``C`` (or by sending a ``SIGINT`` signal to the ``spark-submit`` process). Currently, the created stream is not deleted when the migrator is stopped. You have to delete it manually (e.g. via the AWS Console).

Note that for the migration to be performed without losing writes, the initial snapshot transfer must complete within 24 hours. Otherwise, some captured changes may be lost due to the retention period of the table's stream. If your snapshot is expected to take longer than that, use the Kinesis Data Streams mode instead.

.. warning::

   Starting with this version, the KCL lease table name used for checkpointing is deterministic: ``migrator_<source-table>`` (previously: ``migrator_<source-table>_<millis>``).

   * **Upgrade impact**: if you were already running on DynamoDB Streams, your existing ``migrator_<source-table>_<millis>`` lease tables become orphaned in DynamoDB. They continue to bill silently; delete them manually after confirming the new lease table has taken over.
   * **Fresh replay**: on the first run after upgrade the migrator cannot find the prior ``_<millis>``-suffixed lease table and will replay the stream from ``TrimHorizon``. Apply this upgrade during a quiet window.
   * **Concurrent migrators**: if you run two migrators against the same source table (e.g. replicating to two different targets) they now share a lease table and KCL will split the shards between them — each target sees only half of the changes. This path currently has no ``appName`` override; until one is added, avoid concurrent migrators on a single source table, or use the Kinesis Data Streams mode (below) which supports ``appName``.

Kinesis Data Streams
====================

Use this mode when:

* Your initial snapshot may take longer than 24 hours to transfer.
* You want to replay changes from a specific point in time (``AT_TIMESTAMP``).
* You already use Kinesis as the canonical change feed for the source table.

Requirements:

* A Kinesis Data Stream must already exist (the migrator does not create it). Pick a stream whose retention period covers your expected initial snapshot duration plus a safety margin.
* The IAM role that runs the migrator needs permission to call:

  * ``dynamodb:EnableKinesisStreamingDestination`` and ``dynamodb:DescribeKinesisStreamingDestination`` on the source table.
  * ``kinesis:DescribeStream``, ``kinesis:ListShards``, ``kinesis:GetShardIterator``, ``kinesis:GetRecords`` on the Kinesis stream.
  * The usual DynamoDB permissions on the KCL lease / checkpoint table (``dynamodb:CreateTable``, ``dynamodb:DescribeTable``, ``dynamodb:GetItem``, ``dynamodb:UpdateItem``, ``dynamodb:PutItem``, ``dynamodb:Scan``). The lease table is named after the ``appName`` (default: ``migrator_<sourceTable>``).
  * ``cloudwatch:PutMetricData`` (KCL publishes metrics).

Configuration:

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    streamChanges:
      type: kinesis
      streamArn: arn:aws:kinesis:us-east-1:123456789012:stream/my-stream
      # Optional: an ISO-8601 instant used as the KCL `AT_TIMESTAMP` initial position. When
      # omitted, the migrator defaults this to the moment the initial snapshot starts so no
      # events are dropped during a long snapshot.
      initialTimestamp: "2026-04-01T00:00:00Z"
      # Optional: override the KCL application (lease / checkpoint) name. Defaults to
      # `migrator_<sourceTableName>`. Set this if you run more than one migrator against the
      # same source table, or if you want lease-table isolation between runs.
      appName: my-kinesis-migrator

Snapshot / stream coordination
------------------------------

The migrator enables the Kinesis streaming destination on the source table **before** it starts reading the snapshot, then captures the current time as the default ``AT_TIMESTAMP`` initial position. When the snapshot completes, streaming begins from that captured timestamp, so any write that happened during the snapshot is still replayed exactly once against the target. If you prefer a deterministic replay window (e.g., to resume from a previous migrator run), set ``initialTimestamp`` explicitly.

Checkpoint persistence
----------------------

The KCL writes leases and checkpoints to a DynamoDB table named after the ``appName``. This name is deterministic (it no longer contains a timestamp as it did in versions prior to the Kinesis work), so a restarted migrator resumes where the previous run left off instead of re-processing the entire stream from ``TrimHorizon``.

Skipping the initial snapshot
=============================

Optionally, you can skip the initial snapshot transfer and only replicate the changed items by setting the property ``skipInitialSnapshotTransfer`` to ``true``. This works for either streaming mode:

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    streamChanges: true
    skipInitialSnapshotTransfer: true

or

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    streamChanges:
      type: kinesis
      streamArn: arn:aws:kinesis:us-east-1:123456789012:stream/my-stream
    skipInitialSnapshotTransfer: true
