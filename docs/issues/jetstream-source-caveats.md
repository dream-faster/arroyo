# JetStream Source Caveats

This note documents the current JetStream-aware NATS source behavior in Arroyo and the main caveats that make it risky to rely on as a durable ingestion mechanism.

The implementation exists today, but the recovery semantics are weaker than they first appear.

## Where the implementation lives

- Source type selection: `crates/arroyo-connectors/src/nats/mod.rs`
- Source schema: `crates/arroyo-connectors/src/nats/table.json`
- JetStream source runtime: `crates/arroyo-connectors/src/nats/source/mod.rs`

## What exists today

The NATS connector already supports two source modes:

- Core NATS via `subject`
- JetStream via `stream`

JetStream mode creates a pull consumer and resumes from Arroyo-managed state using `DeliverPolicy::ByStartSequence` when state exists.

## Caveats

### 1. Consumer state is deleted and recreated on startup

The source deletes any existing consumer before recreating it:

- `crates/arroyo-connectors/src/nats/source/mod.rs:253`

That means JetStream consumer state is not treated as the authoritative resume mechanism. Instead, the implementation rebuilds the consumer from Arroyo checkpoint state.

Implications:

- operational state is split across JetStream and Arroyo
- NATS-side consumer observability is less useful than expected
- consumer durability depends on Arroyo checkpoint correctness, not only JetStream durability

### 2. Messages are acked before Arroyo checkpoint commit

Each JetStream message is acked immediately after deserialization and buffering:

- `crates/arroyo-connectors/src/nats/source/mod.rs:409`

Checkpoint state is only persisted later when a checkpoint control message arrives:

- `crates/arroyo-connectors/src/nats/source/mod.rs:427`

This creates a correctness gap:

1. message is delivered
2. message is acked to JetStream
3. Arroyo crashes before checkpoint state is persisted

After restart, the source may resume from an older saved sequence while JetStream already considers the acked message consumed.

This is the main caveat. The current implementation does not provide strong checkpoint-aligned acknowledgement semantics.

### 3. Resume position is Arroyo-managed, not consumer-ack-floor-managed

Resume logic reads the max saved `stream_sequence_number` from Arroyo state and restarts at `sequence + 1`:

- `crates/arroyo-connectors/src/nats/source/mod.rs:302`

This can work, but it means:

- the effective source of truth is Arroyo state
- JetStream consumer progress is secondary
- deleting the consumer is part of the normal flow instead of an exceptional repair step

If the goal is durable replay directly from JetStream, this design is surprising.

### 4. Core and JetStream recovery semantics are very different

Core NATS mode just subscribes directly:

- `crates/arroyo-connectors/src/nats/source/mod.rs:467`

There is no durable recovery in that path beyond normal live-subscription behavior.

JetStream mode looks durable, but because of the ack-before-checkpoint behavior, it still does not have the kind of end-to-end replay guarantee a user might infer from "JetStream support".

The difference between these two modes should be explicit in user-facing docs.

### 5. Sink support is still core NATS only

The sink side publishes to a subject using `async_nats::Client::publish(...)`:

- `crates/arroyo-connectors/src/nats/sink/mod.rs:68`

There is no JetStream-specific sink path in the connector schema:

- `crates/arroyo-connectors/src/nats/table.json:147`

This is fine if the NATS server binds the subject to a stream, but it is worth documenting because users may assume source and sink semantics are symmetrical.

### 6. Some config defaults and shapes look inconsistent

There are a few mismatches worth cleaning up:

- `ackWait` default in schema is `300`, but code defaults to `30`
- `filterSubjects` is modeled as an array in schema, but `consumer.filter_subjects` is parsed from a comma-separated string
- `numReplicas` schema text suggests inheritance behavior for `0`, but code defaults to `1`

Relevant code:

- `crates/arroyo-connectors/src/nats/table.json:45`
- `crates/arroyo-connectors/src/nats/mod.rs:95`

These do not by themselves break the connector, but they make the interface harder to reason about.

## What should change

At minimum:

1. Delay JetStream ack until checkpoint success, or otherwise couple ack progress to durable Arroyo checkpoint progress.
2. Avoid deleting and recreating the consumer on every startup unless there is a specific recovery reason.
3. Make the intended ownership of resume state explicit: Arroyo-managed sequence state vs JetStream consumer state.
4. Align the schema defaults and option parsing with the actual implementation.
5. Document the tradeoff between core NATS and JetStream source modes clearly.

## Recommendation

Until the ack/checkpoint gap is addressed, JetStream mode should be treated as experimental for workloads that require strong replay guarantees after failure.
