# Worker Replicas in Arroyo

## Overview

Arroyo scales stream processing horizontally through **subtask-based parallelism**. Each logical operator has a configurable `parallelism` field, and the system spawns that many independent subtask replicas (indexed 0 to N-1). This document explains how replicas are managed, how their state stays isolated, and how exactly-once semantics are preserved across them.

---

## 1. Replica Definition and Scheduling

Each operator is defined with a `parallelism` value in the `ArrowNode` protobuf message (`arroyo-rpc/proto/api.proto`):

```protobuf
message ArrowNode {
  uint32 parallelism = 3;  // number of replicas
  ...
}
```

The controller's scheduler (`crates/arroyo-controller/src/states/scheduling.rs`) distributes subtasks across available worker processes using round-robin assignment. A single worker process can host multiple subtasks from different operators simultaneously.

At runtime, each subtask knows its own index and the total parallelism via `SubtaskNode { subtask_idx, parallelism }` (`crates/arroyo-worker/src/engine.rs`).

---

## 2. State Isolation — Key-Hash Partitioning

State does **not** conflict across replicas because each subtask owns a **disjoint slice of the keyspace**.

The routing function in `crates/arroyo-operator/src/lib.rs` maps every key to exactly one subtask:

```
subtask_index = hash(key) / (u64::MAX / parallelism)
```

For `parallelism=3`:
- Subtask 0 → hashes 0–33%
- Subtask 1 → hashes 33–66%
- Subtask 2 → hashes 66–100%

A given key **always routes to the same subtask**. Only that subtask ever reads or writes state for it, so there is no cross-subtask state overlap.

State is physically stored in separate Parquet files per subtask, tagged with `subtask_index` at write time (`crates/arroyo-state/src/tables/global_keyed_map.rs`).

---

## 3. Data Routing Between Operator Replicas

Two edge types connect operator replicas (`arroyo-rpc/proto/api.proto`):

| Edge Type | Behavior | Use Case |
|-----------|----------|----------|
| **FORWARD** | 1:1 direct connection (requires equal parallelism) | Chained operators with same parallelism |
| **SHUFFLE** | All-to-all queues with key-hash routing | Redistribution across different parallelism levels |

For Shuffle edges, the `repartition()` function (`crates/arroyo-operator/src/context.rs`) hashes each row's key column(s) and sends the row to the correct downstream subtask's queue. This ensures that all rows for a given key arrive at the same downstream subtask, preserving the key-partitioned state guarantee.

---

## 4. Exactly-Once Semantics

Exactly-once processing is preserved through a **checkpoint barrier + two-phase commit** protocol.

### Step 1 — Barrier Injection

Sources periodically emit a `CheckpointBarrier { epoch }` record into the stream alongside normal data records.

### Step 2 — Barrier Alignment

Each operator uses a `CheckpointCounter` (`crates/arroyo-operator/src/lib.rs`) to track barrier delivery across all input subtasks. Data from inputs that have already delivered the barrier is buffered until **all** inputs for that epoch have arrived. This prevents any subtask from processing data "past" a checkpoint boundary ahead of its peers.

### Step 3 — State Snapshot

Once all input barriers are aligned, each subtask checkpoints its local state partition to durable storage and reports `FINISHED_SYNC` to the controller.

### Step 4 — Global Commit

The controller (`crates/arroyo-worker/src/job_controller/checkpoint_state.rs`) collects `FINISHED_SYNC` from **every subtask of every operator**. Only after all subtasks have reported does it issue the commit signal, triggering `FINISHED_COMMIT` acknowledgments. Sinks that support transactions (e.g. Kafka) only commit their output transactions at this point.

### Why No Duplicate Events on Recovery

If a failure occurs mid-pipeline, the job restarts from the last fully committed checkpoint epoch. Each subtask reloads only its own state partition. Because barrier alignment enforces that no data crosses an epoch boundary unless all upstream subtasks have checkpointed it, replaying from epoch N is safe — no partial writes from epoch N+1 survive in committed storage.

### At-Least-Once Option

Individual state tables can opt out of two-phase commit (`uses_two_phase_commit = false`) for at-least-once semantics with lower overhead. The default is exactly-once.

---

## 5. Watermark Coordination

Each operator replica holds a `WatermarkHolder` (`crates/arroyo-operator/src/context.rs`) tracking the latest watermark received from each upstream subtask. The **minimum** watermark across all inputs is used as the operator's current event-time watermark. This ensures that time-based windowing operations are only triggered when all upstream replicas have advanced past that timestamp, preventing missed records due to replica skew.

---

## Key Files Reference

| Concept | File |
|---------|------|
| Operator parallelism definition | `crates/arroyo-rpc/proto/api.proto` |
| Subtask assignment | `crates/arroyo-controller/src/states/scheduling.rs` |
| Physical graph construction | `crates/arroyo-worker/src/engine.rs` |
| Key-hash routing | `crates/arroyo-operator/src/lib.rs` |
| Shuffle / repartition | `crates/arroyo-operator/src/context.rs` |
| State storage (keyed tables) | `crates/arroyo-state/src/tables/global_keyed_map.rs` |
| Checkpoint barrier alignment | `crates/arroyo-operator/src/lib.rs` (CheckpointCounter) |
| Checkpoint state tracking | `crates/arroyo-worker/src/job_controller/checkpoint_state.rs` |
| Watermark coordination | `crates/arroyo-operator/src/context.rs` (WatermarkHolder) |
