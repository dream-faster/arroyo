# PR #34 ASOF JOIN review

Review target: `copilot/implement-asof-join` against `master` for
[PR #34](https://github.com/dream-faster/arroyo/pull/34).

## Executive summary

The PR adds ASOF JOIN support by rewriting parsed ASOF syntax into an inner
join with a private marker UDF, carrying timestamp column indexes through the
planner, and enforcing "nearest right timestamp <= left timestamp" in
`JoinWithExpiration`.

The approach is directionally workable, and the existing PR CI is green.
However, the current runtime semantics are not safe for normal streaming ASOF
usage unless the join is explicitly documented as arrival-order/speculative.
Late or out-of-order right rows can cause duplicate outputs for the same left
row, and equal-timestamp right ties can emit rows that are not the deterministic
"best" candidate. There are also validation gaps that push user-facing errors
to runtime and a compatibility risk if planners with `asof` jobs talk to older
workers.

## High-priority failure modes

### 1. Late right rows can change a previously emitted ASOF match without a retraction

Evidence:

- `process_left_asof` immediately emits a pair for each left row using the best
  right row currently in state
  (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:130-170`).
- `process_right_asof` later scans buffered left rows and emits again when a new
  right row becomes the best right row for a left timestamp
  (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:176-243`).
- The join output is append-only; `check_updating` rejects updating inputs, and
  the ASOF path does not emit retract/update metadata
  (`crates/arroyo-planner/src/plan/join.rs:71-85`,
  `crates/arroyo-planner/src/plan/join.rs:474-485`).

Example failure:

1. Right `R1(k=A, ts=10)` arrives.
2. Left `L(k=A, ts=20)` arrives and emits `L-R1`.
3. Late right `R2(k=A, ts=15)` arrives.
4. `process_right_asof` now emits `L-R2` because `R2` is the closest right row.

Downstream sees two rows for one logical ASOF result and receives no indication
that `L-R1` should be withdrawn. This violates "single most recent right row"
semantics for append-only outputs.

Recommended fixes:

- Decide and document one explicit semantic model:
  - **Final event-time ASOF**: buffer left rows until the right-side watermark
    has advanced beyond each left timestamp, then emit once.
  - **Updating ASOF**: track the current selected right row per left row and
    emit retractions/upserts when a later right row supersedes it.
  - **Arrival-order ASOF**: only match against right rows already present at the
    left-row arrival time, and do not emit from `process_right_asof`; this is
    easier but should be exposed as arrival-order semantics, not final ASOF
    semantics.
- Add an integration test with right rows arriving before and after a left row
  to assert exactly the selected semantic behavior.

### 2. Equal-timestamp ties can emit the wrong row and duplicate outputs

Evidence:

- `pick_asof_right` keeps the first row with the maximum timestamp because it
  does not replace `best` when `b >= v`
  (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:449-463`).
- `process_right_asof` only compares the selected best timestamp to the new
  right row's timestamp (`best_ts != new_right_ts`) rather than checking whether
  the selected row is the new row
  (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:229-241`).
- The unit test for ties accepts either tied row instead of defining the
  contract (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:505-512`).

Example failure:

1. `R1(k=A, ts=10)` arrives.
2. `L(k=A, ts=12)` arrives and emits `L-R1`.
3. `R2(k=A, ts=10)` arrives later.
4. `pick_asof_right` still selects `R1` under current "first max" behavior, but
   `process_right_asof` sees `best_ts == new_right_ts` and emits `L-R2`.

The emitted row is not the row selected by the tie-breaker, and it duplicates
the previous output for the same left row.

Recommended fixes:

- Define a deterministic tie-breaker, for example earliest arrival, latest
  arrival, or a stable secondary ordering column.
- Store or derive a row identity/sequence number for right rows and compare the
  selected row identity to the newly inserted row, not just the timestamp value.
- Update `pick_asof_right_picks_first_when_tied_on_max` to assert the exact
  intended row, and add a `process_right_asof` regression test for equal
  timestamp late arrivals.

### 3. The marker UDF is user-callable and can turn a normal join into an ASOF join

Evidence:

- `_arroyo_asof` is registered as a normal scalar UDF in the schema provider
  (`crates/arroyo-planner/src/lib.rs:269-279`).
- The planner treats any occurrence of that UDF in a join filter as an ASOF
  marker and strips it from the filter
  (`crates/arroyo-planner/src/plan/join.rs:415-449`).
- The AST rewriter injects the same marker when it sees ASOF syntax
  (`crates/arroyo-planner/src/asof.rs:158-169`).

A user can write a plain SQL join such as:

```sql
SELECT *
FROM l JOIN r
ON l.k = r.k AND _arroyo_asof(l.ts, r.ts)
```

That query bypasses the ASOF syntax path and is still interpreted as an ASOF
join. It also strips a user-visible predicate from the physical join.

Recommended fixes:

- Avoid representing planner-only state as a public SQL UDF. Prefer a planner
  extension, side-channel annotation, or a marker expression that cannot be
  written by users.
- If a marker UDF remains necessary, reserve the function name and reject
  user-authored `_arroyo_asof` calls before AST rewriting. The rewriter could
  use a nonce-like marker name per planning invocation, but a non-SQL planner
  annotation is safer.
- Add a negative test showing that user SQL cannot call `_arroyo_asof` directly.

### 4. Runtime timestamp validation is too late and has SQL-inconsistent null behavior

Evidence:

- The planner resolves only that the ASOF marker arguments are column
  references in the left and right input schemas
  (`crates/arroyo-planner/src/plan/join.rs:487-499`).
- Runtime accepts only `Timestamp(Nanosecond)` columns
  (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:412-421`,
  `crates/arroyo-worker/src/arrow/join_with_expiration.rs:439-447`).
- A null left timestamp errors the whole operator, while null right timestamps
  are silently skipped
  (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:422-427`,
  `crates/arroyo-worker/src/arrow/join_with_expiration.rs:449-453`).

Queries can successfully plan and then fail only after data reaches the worker.
The null behavior also differs between left and right and does not match normal
SQL comparison semantics, where `NULL >= value` and `value >= NULL` are not
true and should therefore produce no inner-join match rather than crashing the
job.

Recommended fixes:

- Validate timestamp argument types and nullability during planning and return
  a clear SQL planning error.
- If nullable timestamps are allowed, define SQL-compatible null semantics:
  null left timestamp should produce no output, and null right timestamp should
  not be selected.
- Add planner tests for non-timestamp match columns and nullable timestamp
  columns, plus runtime tests for null left rows if they remain supported.

### 5. Candidate selection is O(left_rows_for_key * right_rows_for_key) on right input

Evidence:

- `process_right_asof` iterates every buffered left row for the key and calls
  `pick_asof_right`, which scans every buffered right row for that key
  (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:222-241`,
  `crates/arroyo-worker/src/arrow/join_with_expiration.rs:449-463`).
- `KeyTimeView::get_batch` coalesces per-key batches into a single unsorted
  batch and returns it wholesale
  (`crates/arroyo-state/src/tables/expiring_time_key_map.rs:959-973`).

For hot keys, one new right row can scan all left rows and all right rows for
that key. This can become quadratic or worse over time and can block the
operator on a single skewed key.

Recommended fixes:

- Store per-key right state in timestamp order, or maintain an auxiliary
  timestamp index for ASOF joins.
- For left arrivals, use binary search to find the greatest right timestamp
  <= left timestamp.
- For right arrivals under updating/final semantics, identify only the left
  timestamp interval affected by the new right row instead of rescanning all
  left rows for the key.
- Add a stress/performance test with a hot key and many right candidates.

## Medium-priority failure modes and improvements

### 6. The AST rewrite misses ASOF joins in nested table factors

Evidence:

- `rewrite_table_factor` only recurses into `TableFactor::Derived`
  (`crates/arroyo-planner/src/asof.rs:111-115`).
- `rewrite_table_with_joins` handles top-level `FROM` relations and direct join
  relations (`crates/arroyo-planner/src/asof.rs:102-109`).

Parenthesized/nested join table factors are common in SQL parsers. If
`sqlparser` represents `(a ASOF JOIN b ...)` as a nested join table factor, the
ASOF operator will survive the pre-pass and then fail later in DataFusion.

Recommended fixes:

- Recurse through all `TableFactor` variants that can contain joins or queries,
  especially nested joins.
- Add parser rewrite tests for parenthesized ASOF joins, ASOF joins nested on
  the right side of another join, and ASOF joins inside more complex subqueries.

### 7. ASOF syntax validation relies on later DataFusion join extraction for ON predicates

Evidence:

- The AST rewriter checks that `MATCH_CONDITION` is exactly a `>=` binary
  expression and that an `ON` clause exists
  (`crates/arroyo-planner/src/asof.rs:127-156`).
- It does not validate that the `ON` clause contains at least one equality key;
  the later join rewriter errors if `join.on` is empty for non-instant joins
  (`crates/arroyo-planner/src/plan/join.rs:357-359`).

The eventual behavior is probably safe, but users can get a generic "Updating
joins must include an equijoin condition" error for malformed ASOF syntax.

Recommended fixes:

- Add ASOF-specific validation and error messages for missing equality keys and
  unsupported non-equality-only ON clauses.
- Add negative tests for `ON t.symbol <> q.symbol`, `ON true`, and ON clauses
  with no extractable equality key.

### 8. Planner/runtime schema-index contract is implicit and fragile

Evidence:

- The planner stores timestamp indexes based on the left/right input schemas
  before key calculation
  (`crates/arroyo-planner/src/plan/join.rs:364-376`).
- Runtime applies those indexes after calling `ArroyoSchema::unkeyed_batch`
  (`crates/arroyo-worker/src/arrow/join_with_expiration.rs:142-146`,
  `crates/arroyo-worker/src/arrow/join_with_expiration.rs:188-191`).
- The proto documents this ordering but has no guard or version check
  (`crates/arroyo-rpc/proto/api.proto:73-79`).

This works only if `ArroyoSchema::unkeyed_batch` ordering stays exactly aligned
with the schema used by the planner. Future schema projection or key-layout
changes could silently point the runtime at the wrong column.

Recommended fixes:

- Include field names or stable field identifiers in `AsofJoinConfig`, not just
  numeric indexes, and validate at worker construction that indexes and names
  match the decoded schemas.
- Add a unit test that builds a keyed schema with keys before, between, and
  after payload columns and verifies planner-selected indexes read the intended
  runtime columns.

### 9. Protobuf rollout is wire-compatible but semantically unsafe with old workers

Evidence:

- `JoinOperator` adds optional field `asof = 7`
  (`crates/arroyo-rpc/proto/api.proto:81-89`).
- Older workers that do not know field 7 will decode and execute the join as a
  normal `JoinWithExpiration` using the embedded inner join plan.

Because the rewritten DataFusion join still contains the `left_ts >= right_ts`
filter, an old worker is likely to emit *all* right rows satisfying the
inequality rather than the single nearest right row. That is wire-compatible but
semantically wrong.

Recommended fixes:

- Gate ASOF plans on a worker capability/version check before deployment.
- Consider encoding ASOF as a distinct operator type or config version that old
  workers fail fast on instead of silently changing semantics.
- Add compatibility tests for decoding ASOF join configs with and without the
  new field.

### 10. State retention follows join table retention, not necessarily ASOF time semantics

Evidence:

- ASOF joins use the same TTL-backed keyed time tables as regular updating
  joins (`crates/arroyo-planner/src/plan/join.rs:399-404`,
  `crates/arroyo-worker/src/arrow/join_with_expiration.rs:323-345`).
- The ASOF match columns can be arbitrary columns from the input schemas, while
  state expiration is tied to the `ArroyoSchema` event-time timestamp.

If users match on a timestamp column that is not the event-time field, state may
expire too early or too late relative to the ASOF search key.

Recommended fixes:

- Either require ASOF `MATCH_CONDITION` columns to be the event-time columns, or
  make retention/watermark logic explicit for arbitrary match timestamp columns.
- Add tests where the ASOF timestamp differs from event time, or reject that
  query with a clear planner error.

## Test coverage to add

Recommended regression and integration tests:

1. Out-of-order right rows: right `10`, left `20`, right `15`; assert the chosen
   semantic model and ensure there are no unintended duplicates.
2. Equal right timestamp ties: right `10/a`, left `12`, right `10/b`; assert the
   deterministic tie-break and no accidental duplicate.
3. User-authored `_arroyo_asof` call in a plain join must be rejected or treated
   as an ordinary unavailable function.
4. Parenthesized/nested ASOF joins should be rewritten or rejected with an ASOF
   diagnostic.
5. Non-timestamp and nullable timestamp match columns should fail at planning
   time or have documented SQL-compatible runtime behavior.
6. Multi-key ASOF with skewed/hot keys should include a performance-oriented
   test or benchmark.
7. Compatibility test showing an ASOF join cannot silently run on a worker that
   lacks `AsofJoinConfig` support.

## Suggested implementation sequence

1. Choose the ASOF semantic model: final event-time, updating/correcting, or
   arrival-order. This decision drives the runtime shape.
2. Fix duplicate and tie handling in `process_right_asof` by tracking selected
   row identity, not only timestamp equality.
3. Move timestamp type/nullability and ASOF-specific ON-clause validation into
   the planner.
4. Replace or harden the marker UDF so user SQL cannot invoke it.
5. Add nested AST rewrite coverage.
6. Add worker capability/version gating for ASOF plans.
7. Rework per-key candidate state for timestamp-indexed lookup before enabling
   ASOF for high-cardinality or skewed workloads.

## Validation notes

- GitHub CI for the current PR head reported all required checks successful:
  21 successful, 2 skipped, 0 failing.
- Local ad-hoc Rust tests could not be run in this environment because the
  installed Cargo is 1.75.0 and the workspace uses edition 2024 crates. The
  attempted command was:

```text
cargo test -p arroyo-worker arrow::join_with_expiration::tests --quiet
```

It failed before compiling the crate with Cargo's edition-2024 feature error.

## Exact resolution plan

Target semantics: implement ASOF JOIN as a **final event-time join**. For each
left row, emit at most one output row after the join can know that no earlier
or equal-key right row with a timestamp closer to the left timestamp can still
arrive. Do not emit speculative rows and do not require downstream retractions.

### Phase 1: Lock the semantic contract in tests

Files to update:

- `crates/arroyo-planner/src/test/queries/asof_join.sql`
- `crates/arroyo-planner/src/test/queries/asof_join_multi_key.sql`
- new planner negative query files under
  `crates/arroyo-planner/src/test/queries/`
- worker/runtime tests in
  `crates/arroyo-worker/src/arrow/join_with_expiration.rs`, or integration
  tests under `crates/integ` if the operator harness cannot express the cases
  below directly

Steps:

1. Add a test where right `R1(k=A, ts=10)` arrives, left `L(k=A, ts=20)`
   arrives, then right `R2(k=A, ts=15)` arrives before the right-side watermark
   passes `20`. Expected result: only `L-R2` is emitted, and `L-R1` is never
   emitted.
2. Add a test where right `R1(k=A, ts=10)` arrives, left `L(k=A, ts=20)`
   arrives, the right-side watermark passes `20`, then right `R2(k=A, ts=15)`
   arrives. Expected result: only `L-R1` is emitted; `R2` is late relative to
   the finalized left row and must not create a second output.
3. Add a duplicate-timestamp tie test: right `R1(k=A, ts=10, quote_id=1)`,
   right `R2(k=A, ts=10, quote_id=2)`, left `L(k=A, ts=12)`. Expected result:
   exactly one output according to the selected tie-breaker from Phase 2.
4. Add a multi-key test with out-of-order right rows to verify that only rows
   with the same full key tuple are candidates.
5. Add planner negative tests for:
   - user-authored `_arroyo_asof(...)` in a normal join;
   - non-timestamp `MATCH_CONDITION` columns;
   - nullable timestamp match columns if nullable ASOF timestamps are rejected;
   - `ON true`, `ON t.k <> q.k`, and ASOF joins with no extractable equality
     key;
   - parenthesized/nested ASOF joins.

Acceptance criteria:

- These tests fail against the current PR implementation for the duplicate and
  late-right-row cases.
- The expected output cardinality is asserted, not only that some matching row
  appears.

### Phase 2: Define deterministic candidate ordering

Files to update:

- `crates/arroyo-rpc/proto/api.proto`
- generated proto outputs, if this repository checks them in
- `crates/arroyo-planner/src/extension/join.rs`
- `crates/arroyo-worker/src/arrow/join_with_expiration.rs`

Steps:

1. Choose the tie-breaker: use **earliest right arrival wins** for rows with the
   same ASOF timestamp unless product requirements explicitly require another
   ordering. This is stable with append-only state and avoids changing a
   finalized result when an equal-timestamp right row arrives later.
2. Extend ASOF right-state rows with an internal monotonically increasing
   arrival sequence per task. The sequence must be stored with state so
   checkpoint/restore preserves the tie-break.
3. Replace `pick_asof_right(candidates, right_ts_index, left_ts)` with a helper
   that returns the candidate with the maximum tuple
   `(right_ts <= left_ts, right_ts, -arrival_sequence)` for earliest-arrival
   ties.
4. Return a candidate identity from the helper, not only an index. The identity
   should include at least timestamp plus arrival sequence.
5. Remove timestamp-only duplicate checks such as `best_ts != new_right_ts`.
   Candidate identity comparison must be used everywhere a "new best" check is
   needed.

Acceptance criteria:

- Equal-timestamp right rows produce exactly one output per left row.
- Replaying from a checkpoint produces the same selected right row as a fresh
  run.

### Phase 3: Rework runtime emission around watermarks

Files to update:

- `crates/arroyo-worker/src/arrow/join_with_expiration.rs`
- state table definitions in `crates/arroyo-state` if an additional pending
  left table or indexed right table is required
- `crates/arroyo-rpc/proto/api.proto` if new operator config is needed

Steps:

1. Stop emitting ASOF results directly from `process_left_asof`.
2. Stop emitting ASOF results directly from `process_right_asof`.
3. Insert right rows into keyed right state with their ASOF timestamp and arrival
   sequence.
4. Insert left rows into a pending-left state keyed by the equality-key tuple and
   ordered by left ASOF timestamp.
5. Implement `ArrowOperator::handle_watermark` for `JoinWithExpiration` when
   `self.asof.is_some()`.
6. On each event-time watermark, drain pending left rows whose left ASOF
   timestamp is strictly less than the watermark chosen for finalization. This
   matches the existing operator pattern that leaves rows at the watermark
   itself open until a later watermark. If Arroyo exposes only a combined input
   watermark to the operator, document and use that combined watermark; if
   side-specific watermarks are available or can be added, use the right-side
   watermark for ASOF finalization.
7. For each drained left row, find the best right candidate with the same key
   and `right_ts <= left_ts`, using the deterministic helper from Phase 2.
8. Emit the joined pair only once, during watermark draining. If no right
   candidate exists, emit nothing for inner ASOF JOIN.
9. Ensure regular non-ASOF `JoinWithExpiration` behavior is unchanged by
   branching all new logic on `self.asof`.
10. Keep TTL cleanup aligned with finalization: a right row may be evicted only
    after no future pending or not-yet-arrived left row can need it under the
    configured lateness/retention policy.

Acceptance criteria:

- Late right rows that arrive before finalization can improve the selected
  match.
- Late right rows that arrive after finalization do not duplicate outputs.
- Each left row produces zero or one output for inner ASOF JOIN.
- Existing non-ASOF updating join tests remain unchanged.

### Phase 4: Move ASOF validation into the planner

Files to update:

- `crates/arroyo-planner/src/asof.rs`
- `crates/arroyo-planner/src/plan/join.rs`
- `crates/arroyo-planner/src/lib.rs`
- planner query tests under `crates/arroyo-planner/src/test/queries/`

Steps:

1. In `check_asof_join`, validate that the join type is inner, both inputs are
   unwindowed, and the ASOF join has at least one equality key.
2. Validate that `MATCH_CONDITION` arguments resolve to columns from opposite
   sides of the join in the expected direction: `left_ts >= right_ts`.
3. Validate that both ASOF timestamp columns have Arrow type
   `Timestamp(Nanosecond, None)` or add an explicit normalization step before
   planning the runtime operator.
4. Decide nullability policy. Recommended policy: reject nullable ASOF
   timestamp columns during planning for the first implementation. If nullable
   timestamps must be supported, implement SQL-compatible behavior where nulls
   simply do not match.
5. Replace generic errors such as "Updating joins must include an equijoin
   condition" with ASOF-specific diagnostics when the marker is present.
6. Add tests that assert the exact ASOF-specific error messages.

Acceptance criteria:

- Invalid ASOF SQL fails before worker execution.
- Error messages mention ASOF and the violated rule.

### Phase 5: Remove or harden the SQL marker UDF

Files to update:

- `crates/arroyo-planner/src/asof.rs`
- `crates/arroyo-planner/src/lib.rs`
- `crates/arroyo-planner/src/plan/join.rs`

Steps:

1. Prefer replacing `_arroyo_asof` with a planner-only annotation that cannot be
   typed by users. If DataFusion requires the marker-expression approach, keep
   the marker private by adding a pre-rewrite scan that rejects any
   user-authored `_arroyo_asof` call in the original SQL AST.
2. Make the ASOF rewriter produce a marker that includes an internal planning
   token or source-location flag so `take_asof_marker` can distinguish generated
   markers from user SQL.
3. Ensure `_arroyo_asof` is not exposed as a documented or callable user
   function. If it must remain registered with DataFusion, return a clear plan
   error for any marker not produced by the ASOF rewriter.
4. Add a negative test for a normal inner join that contains
   `_arroyo_asof(l.ts, r.ts)` in its `ON` predicate.

Acceptance criteria:

- User SQL cannot opt into ASOF behavior except through `ASOF JOIN ...
  MATCH_CONDITION`.
- The planner never silently strips user-authored predicates.

### Phase 6: Complete AST rewrite recursion

Files to update:

- `crates/arroyo-planner/src/asof.rs`

Steps:

1. Extend `rewrite_table_factor` to recurse into every `TableFactor` variant
   that can contain a subquery, nested join, table function argument, or derived
   relation.
2. Add tests for:
   - `(l ASOF JOIN r MATCH_CONDITION (...) ON ...)`;
   - `a JOIN (l ASOF JOIN r MATCH_CONDITION (...) ON ...) ON ...`;
   - ASOF inside CTEs and derived subqueries, preserving the existing coverage;
   - ASOF inside both sides of set operations, preserving the existing coverage.
3. If a `sqlparser` table-factor variant cannot be supported, reject it with a
   clear ASOF-specific parser/planner error instead of letting an unrevised
   `JoinOperator::AsOf` reach DataFusion.

Acceptance criteria:

- Every ASOF syntax form accepted by `sqlparser` is either rewritten or rejected
  with an ASOF-specific diagnostic before DataFusion planning.

### Phase 7: Strengthen planner/runtime schema contracts

Files to update:

- `crates/arroyo-rpc/proto/api.proto`
- `crates/arroyo-planner/src/extension/join.rs`
- `crates/arroyo-worker/src/arrow/join_with_expiration.rs`
- `crates/arroyo-rpc/src/df.rs` tests, if present

Steps:

1. Extend `AsofJoinConfig` to include timestamp field names or stable field
   identifiers in addition to numeric indexes.
2. At worker construction, validate that `left_ts_index` and `right_ts_index`
   are in bounds and that the indexed fields match the expected names and
   timestamp data types.
3. Add a keyed-schema test where key columns appear before, between, and after
   payload columns. Verify that `ArroyoSchema::unkeyed_batch` and the planner's
   ASOF indexes identify the same timestamp fields.
4. Keep backward-compatible proto numbering by only adding new fields with new
   field numbers; do not renumber existing fields.

Acceptance criteria:

- A schema/index mismatch fails during operator construction with a clear error.
- Future key-layout changes cannot silently point ASOF at the wrong column.

### Phase 8: Add compatibility gating

Files to update:

- planner-to-worker deployment or program validation code that already handles
  operator capabilities
- `crates/arroyo-rpc/proto/api.proto` if explicit operator capability metadata
  is needed
- CI or integration tests that can exercise mixed-version behavior

Steps:

1. Introduce an ASOF JOIN worker capability bit or minimum worker protocol
   version.
2. When a planned program contains `JoinOperator.asof`, require every target
   worker to advertise that capability before scheduling the job.
3. If capability negotiation is not available yet, encode ASOF as a distinct
   operator/config version that older workers fail to decode rather than
   silently interpreting as a regular inequality join.
4. Add a compatibility test that decodes an ASOF join config without ASOF
   support and verifies fail-fast behavior.

Acceptance criteria:

- An ASOF plan cannot run on a worker that would ignore field 7 and emit all
  inequality matches.

### Phase 9: Improve per-key state access for scale

Files to update:

- `crates/arroyo-state/src/tables/expiring_time_key_map.rs`
- `crates/arroyo-worker/src/arrow/join_with_expiration.rs`

Steps:

1. Add an ASOF-specific state access path that stores right rows per equality
   key ordered by `(right_ts, arrival_sequence)`.
2. Add a lookup API that returns the greatest right row with
   `right_ts <= left_ts` using binary search or an ordered map.
3. Add a pending-left drain API that can iterate left rows up to a watermark
   without scanning all rows for hot keys.
4. Keep the existing `KeyTimeView::get_batch` API unchanged for non-ASOF joins.
5. Add a stress test or benchmark with one hot key and many right candidates.

Acceptance criteria:

- Right-arrival and watermark-drain work is proportional to affected rows, not
  `left_rows_for_key * right_rows_for_key`.
- Non-ASOF state table behavior remains unchanged.

### Phase 10: Final verification and rollout checklist

Commands to run before merging the implementation:

```text
cargo fmt -- --check
cargo clippy --all-targets --workspace -- -D warnings
cargo nextest run -E 'kind(lib)'
cargo build
```

Additional checks:

1. Run the planner SQL regression suite that covers
   `crates/arroyo-planner/src/test/queries/*.sql`.
2. Run integration tests for both Postgres and SQLite metadata backends because
   CI exercises both variants.
3. Confirm PR checks show no failing Rust, integration, or format jobs.
4. Manually inspect generated physical plans for a representative ASOF query to
   confirm the marker predicate is removed before execution and the ASOF config
   is present.
5. Confirm the user-facing documentation states the exact semantics:
   event-time-final ASOF, inner joins only, `>=` only, unwindowed inputs only,
   deterministic equal-timestamp tie-breaker, timestamp type/nullability rules,
   and worker version requirement.
