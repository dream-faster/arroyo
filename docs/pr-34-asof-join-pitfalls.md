# PR #34 — ASOF JOIN: Pitfalls & Non-ASOF Impact Audit

Branch: `copilot/implement-asof-join`
Commits reviewed: `72e054dd…0ab672de` (full diff vs `origin/master`)
Scope: ASOF JOIN planner + runtime, plus every code path the PR adds that
runs for non-ASOF queries.

This document complements `pr-34-asof-join-review.md` and the DuckDB-compat
spec. It focuses on **failure modes that may slip through the existing test
suite** and on **regressions to non-ASOF behavior**.

## Status update after executing the follow-up fixes

Most of the correctness items from the first pass are now closed. Current
status:

| Item | Status | Notes |
| --- | --- | --- |
| P1 — pending-left rows stall on idle / end-of-stream | **Resolved** | `Watermark::Idle` now drains `left_pending` before the watermark is forwarded. |
| P2 — ASOF lookback silently bounded by TTL | **Partially resolved** | ASOF now logs a dedicated warning when it falls back to the default TTL, but there is still no planner warning or eviction metric. |
| P3 — `pick_asof_right` is O(N) per left row | **Open** | The obvious refetch overhead is gone, but selection still scans the candidate right batch linearly. |
| P4 — null-safe key extraction reads the filter before stripping the marker | **Resolved** | ASOF key extraction now uses the marker-free filter. |
| P5 — inequality recovery depends on `Expr` identity | **Resolved** | The inequality is now encoded in the internal marker name (`__arroyo_internal_asof_*`). |
| P6 — equal-timestamp ties are replay-nondeterministic | **Resolved** | Ties are now broken by encoded row payload rather than insertion order. |
| P7 — right table re-fetched once per finalized left row | **Resolved** | The right table handle is fetched once per finalized batch. |
| P8 — `Idle` is forwarded while pending-left state remains buffered | **Resolved** | The join drains pending-left state before propagating idle. |
| NON-ASOF-1 — normalizer false-trigger on ordinary SQL | **Substantially reduced** | `parse_sql()` now attempts the raw parser first and only falls back to ASOF-specific normalization when parsing actually fails. |
| NON-ASOF-2 — internal marker name reserved globally | **Resolved / reduced** | The marker moved to a much less collision-prone reserved internal namespace. |
| NON-ASOF-3 — universal pre-parse cost | **Partially resolved** | Valid non-ASOF SQL avoids the normalization passes, but ASOF fallback still pays for them. |
| NON-ASOF-4 — every join filter is cloned and walked | **Resolved** | The planner now does a cheap marker-presence check and only rewrites filters that actually contain the ASOF marker. |
| NON-ASOF-5 — ASCII-only boundary logic | **Open / unverified** | Still worth fuzzing, but no concrete regression was reproduced in this pass. |

---

## 1. Architecture recap (where the changes live)

| Layer | File | Behaviour added | Runs for non-ASOF queries? |
| --- | --- | --- | --- |
| SQL pre-parse | `crates/arroyo-planner/src/lib.rs:792-803` | reject internal marker calls, try raw parse first, fall back to ASOF-only normalization + `rewrite_asof_joins` on parse failure | **Mostly no** for valid non-ASOF queries |
| AST rewrite | `crates/arroyo-planner/src/asof.rs` | `JoinOperator::AsOf` → `Inner(... AND __arroyo_internal_asof_<op>(lhs,rhs))` | Walks every statement that parsed as ASOF |
| UDF registry | `crates/arroyo-planner/src/lib.rs:273-281` | Registers placeholder UDFs in the `__arroyo_internal_asof_*` namespace | **Yes** (reserved internal names) |
| Logical plan | `crates/arroyo-planner/src/plan/join.rs:324-426` | `take_asof_marker`, ASOF detection, null-safe key extraction, AsofConfig | **Yes** — runs on every join |
| Extension proto | `crates/arroyo-planner/src/extension/join.rs` | Serializes `AsofConfig` w/ inequality + left_outer | ASOF only |
| Runtime | `crates/arroyo-worker/src/arrow/join_with_expiration.rs` | Pending-left state, watermark drain, candidate selection | Branched on `asof.is_some()` |
| Proto | `crates/arroyo-rpc/proto/api.proto` | `AsofInequality` enum + fields on `AsofJoinConfig` | n/a |

---

## 2. Pitfalls in the ASOF implementation

### 2.1 High severity

**P1 — Pending-left rows can stall forever when no later watermark arrives.**
**Status: resolved.**
`handle_watermark` (`join_with_expiration.rs:380-408`) drains `left_pending`
only while `next_time < watermark_time`, and the earlier
`asof_finalize_watermark_time` helper returned `None` for `Watermark::Idle`.
Consequences:
- A left row whose ASOF timestamp equals the maximum observed event time is
  never finalized until the watermark advances strictly past it.
- For sources that go idle (the common bounded-source case), pending lefts
  are silently held indefinitely. The result is that the *last* batch of an
  ASOF inner join may never appear in output, and an outer join may never
  emit its trailing nulls.

**Fix**: on `Watermark::Idle`, treat the watermark as `+∞` (i.e. drain
*all* pending lefts), or wire a finalize-on-shutdown hook. At minimum
document the non-finalization of the last bucket so users add a synthetic
trailing watermark in tests.

**P2 — Right-side TTL governs ASOF lookback even when ASOF needs longer
retention.** `process_right_asof` writes into the regular `right`
key-time-table with `right_expiration = ttl` (`join_with_expiration.rs:455-462`).
For ASOF, a left row that arrives at `t` may need a right row dated as far
back as `t − lookback`. If a user's `ttl` is shorter than the realistic
ASOF lag, matches will silently disappear. There is no validation that
`ttl ≥ expected ASOF lookback` and no metric exposing eviction counts.

**Fix**: surface a planner warning when the join is ASOF and ttl is the
default (24h). Add an operator metric for evicted-right-rows-with-pending-left.

**P3 — `pick_asof_right` is O(N) per left row.** For each finalized left
row we linearly scan the matching right partition (`pick_asof_right`,
`join_with_expiration.rs:596-641`). Right partitions can hold many minutes
of events at production rates; multiplied by the buffered-left burst this
becomes quadratic per watermark tick.

**Fix**: keep the `right` table sorted by ts (or pre-sort once per drain)
and binary-search. At minimum, add a metric for max right-partition size.

### 2.2 Medium severity

**P4 — `extract_null_safe_join_keys` reads `join.filter` *before* the
marker is stripped** (`plan/join.rs:372-376`). The marker is a
`ScalarFunction`, not an `IsNotDistinctFrom`, so `split_conjunction_owned`
just yields it as a noop sibling and the function returns the right keys.
Today this is correct, but the dependency is implicit and brittle: if a
future change makes the marker contain its own `IsNotDistinctFrom` or
gets wrapped in `NOT`, key extraction will be wrong. Pass
`filter_without_marker` here once it is computed.

**P5 — `take_asof_marker` requires the inequality and the marker arguments
to match by `Expr` identity** (`plan/join.rs:553-580`). The inequality
recovery walks the filter and looks for a `BinaryExpr` whose `left == lhs`
and `right == rhs`. If the SQL author writes
`MATCH_CONDITION (CAST(left.ts AS TIMESTAMP) >= right.ts)` the marker call
will receive the cast expression but the `AsOf` AST keeps the cast on the
LHS too, so today they match — but any optimizer re-ordering or
DataFusion-side simplification (e.g. constant folding, alias rewriting)
between AST rewrite and `JoinRewriter::f_up` will desynchronize the two
expressions and produce
`"ASOF JOIN marker was present but matching inequality could not be recovered"`.

**Fix**: stash the operator inside the marker call (e.g. encode as the
function name `__arroyo_internal_asof_gte`/`_lt`/…) instead of rediscovering it
from the surrounding AND-tree.

**P6 — `pick_asof_right` tie-breaks "later in partition order".** The
inline comment claims this matches "the final qualifying row", which is
DuckDB's documented behaviour, but partition order in the right table is
*insertion order*, not event-time order. Two right rows with identical
timestamps inserted across batches may swap positions on recovery, making
the chosen tie-broken row non-deterministic across replays.

**Fix**: tie-break by a stable secondary key (insertion sequence or row
content hash) and document the chosen rule.

**P7 — `process_finalized_left_batch` queries the right table once
per left row, then re-fetches it on the next row** (`join_with_expiration.rs:260-264`).
This is correct but loses the table handle borrow each iteration; a single
fetch outside the loop would be both faster and clearer that we're using
a snapshot consistent with the watermark.

**P8 — `Watermark::Idle` is forwarded but pending lefts are left in the
state** (`join_with_expiration.rs:374-378`). Operators downstream of the
join may interpret `Idle` as "we are caught up", which is false because
the join is still buffering left rows.

**Fix**: forward `Idle` only after pending-left is drained; otherwise
suppress the `Idle` watermark.

### 2.3 Low severity / nits

- `pending_left_schema` clones the full `ArroyoSchema` per call (twice per
  left batch + once in `tables()`); **resolved** by caching the pending-left
  schema in the operator state.
- `compute_pair` builds a fresh `SessionContext` per pair
  (`join_with_expiration.rs:300-303`); **resolved** by reusing a stored task
  context.
- `decode_asof_inequality` rejects unknown variants with `anyhow!`, which
  is correct; **resolved** by including the proto field path in the error.
- The DuckDB-compat spec promises `ASOF JOIN ... USING(k1, k2, ts)` with
  multiple equality keys; the planner test suite covers the multi-key
  case. Re-review note: the earlier concern about `NULL` and `""` collapsing
  was overstated because the derived hash key already includes a separate
  `IsNull(expr)` component, so `NULL` and the empty string remain distinct.

---

## 3. Code paths that affect **non-ASOF** join behaviour

The user explicitly asked whether the PR risks breaking non-ASOF queries.
After the follow-up changes, the answer is **mostly no for valid SQL that the
raw parser already accepts**. The remaining impact is mostly the reserved
internal marker namespace and some extra fallback work on genuine ASOF syntax.

### 3.1 NON-ASOF-1 — Text-level ASOF-USING normalizer can mis-rewrite ordinary queries

`normalize_duckdb_asof_using_joins` (`asof.rs:55-123`) and
`normalize_duckdb_asof_left_joins` (`asof.rs:129-187`) run on **every**
SQL string before sqlparser ever sees it. Both use
`find_keyword_outside(sql, "ASOF", _)` which:

1. Skips text inside `'...'` and `"..."` and inside parentheses.
2. Returns any unquoted, non-parenthesized occurrence of the bare token
   `ASOF` (case-insensitive) at a word boundary.

This means a query that simply mentions `asof` as an unquoted alias or
column name and *also* contains a regular JOIN later in the statement can
be silently rewritten:

```sql
-- 100% standard SQL, contains no ASOF intent
SELECT t1.asof, t2.k
FROM t1
JOIN t2 USING (k);
```

Trace through `normalize_duckdb_asof_using_joins`:
- `find_keyword_outside` finds `asof` at `t1.asof` (word boundary OK).
- It then finds the next `JOIN` keyword after that position (the real
  `JOIN t2 USING (k)`).
- It checks the substring between `ASOF` and `JOIN` is whitespace —
  **false** in this query (`, t2.k FROM t1 ` lies between), so the
  normalizer correctly skips. ✅

So the USING normalizer is safe **when the JOIN does not directly follow
the spurious `asof` token**. But:

```sql
-- Regression candidate
SELECT *
FROM t1
LEFT JOIN t2_named_asof asof JOIN t3 USING (k) ON ...;
```

Here `asof` appears as a table alias and the next non-whitespace token is
`JOIN`. The normalizer's "is the gap between ASOF and JOIN whitespace?"
check **passes**, and it then looks for `USING` — which is present —
producing a malformed `MATCH_CONDITION (asof.k >= t3.k) ON ...` rewrite.
The query then fails to parse with a confusing error or, worse, parses
into a wrong logical plan.

The same hole affects `normalize_duckdb_asof_left_joins`: the gating
condition `ASOF` followed by `LEFT` followed by `JOIN` *with only
whitespace between* triggers when a user has:

```sql
SELECT * FROM events asof LEFT JOIN dim ON ...;
```

(`asof` is a perfectly legal alias.) The normalizer enters the LEFT-JOIN
branch and demands a `MATCH_CONDITION`, returning
`"ASOF LEFT JOIN requires a MATCH_CONDITION clause"` for what is plainly
a normal `LEFT JOIN`.

**Status update**: this risk is substantially reduced in practice because
`parse_sql()` now tries the raw parser first and only runs the text-level
normalizers on parse failure. Valid non-ASOF SQL therefore avoids the
normalizers entirely.

**Fix options**:
- Run the rewrite *after* parsing instead of on the raw text. The
  sqlparser fork already produces `JoinOperator::AsOf`; the only reason
  the normalizers exist is to translate USING/LEFT spellings. Both can
  be done as AST rewrites once the parser learns the surface forms.
- Or, gate the keyword scanner so `ASOF` must be in the **table position**
  (immediately preceded by `FROM`, `JOIN`, or `,`), which is what the
  grammar actually requires.

### 3.2 NON-ASOF-2 — the internal ASOF marker namespace is reserved globally

`reject_user_authored_asof_marker` now rejects calls to the internal
`__arroyo_internal_asof_*` marker family. Bare identifiers are still allowed,
but the placeholder UDF is registered for **every** query (`lib.rs:273-279`),
which means a user column literally named `__arroyo_internal_asof_gte` would
*resolve* to the UDF
reference rather than the column when used in an expression context that
prefers a function. In practice DataFusion gives column refs precedence
over UDFs, so this is unlikely to bite, but the name is permanently
tainted.

**Fix**: rename to a less collision-prone marker (`__arroyo_internal_asof_marker__`).

### 3.3 NON-ASOF-3 — Universal pre-parse cost

Every SQL string still runs `reject_user_authored_asof_marker`, but only
queries that fail the raw parser now pay for:
1. `normalize_duckdb_asof_using_joins`
2. `normalize_duckdb_asof_left_joins`
3. `rewrite_asof_joins`

For typical Arroyo-sized queries (≤ a few KB) this is negligible, but
the constant factor is now non-zero on the hot path of `parse_sql`. Fold
the three text scans into a single pass or, per 3.1, eliminate them in
favour of an AST rewrite.

### 3.4 NON-ASOF-4 — `take_asof_marker` runs on every join's filter

`JoinRewriter::f_up` used to clone each join's filter (`plan/join.rs:336`) and
do a `transform_up` walk before any other handling. That is now fixed: the
planner first checks whether the expression tree contains one of the internal
ASOF marker names and only rewrites matching joins.

### 3.5 NON-ASOF-5 — `is_word_boundary` accepts only ASCII identifiers

`asof.rs:423-428` treats any byte that is not `[A-Za-z0-9_]` as a
word boundary. UTF-8 identifier characters (any byte ≥ 0x80) are
considered boundaries, so a query using a Unicode column name like
`SELECT αsof FROM t JOIN ...` could allow the scanner to enter the
`ASOF` branch on the bytes `sof` after a multi-byte boundary. Low
likelihood but worth fuzzing.

---

## 4. Tests we should add before merge

| Test | Asserts |
| --- | --- |
| Planner regression: alias literally named `asof` followed by `JOIN ... USING` | normalizer leaves the SQL untouched |
| Planner regression: `events asof LEFT JOIN dim ...` | parses as a plain LEFT JOIN |
| Planner regression: column named `__arroyo_internal_asof_gte` (no parens) | parses, no error |
| Worker test: bounded source with one trailing left row at watermark = max ts | row is finalized (verifies P1 fix) |
| Worker test: `Watermark::Idle` after one left + zero rights | does not forward Idle while pending-left is non-empty (P8) |
| Worker test: ttl < typical ASOF lag, observe metric for evicted rights | (P2) |
| Worker fuzz: tie-break across recovery | deterministic chosen row (P6) |
| Planner: `MATCH_CONDITION (CAST(left.ts AS TIMESTAMP) >= right.ts)` survives DataFusion simplification | inequality recovery succeeds (P5) |

---

## 5. Suggested commit-sized fixes

1. **Idle/end-of-stream finalization**: treat `Watermark::Idle` as a
   drain-all signal for `left_pending`, and suppress idle propagation while
   pending-left is non-empty. **Done.**
2. **Inequality encoded in marker name**: replace single
   ASOF marker UDF with four distinct internal names (or with one that takes
   the operator as a string literal). Eliminates P5 and shrinks
   `take_asof_marker` to a single match. **Done with the
   `__arroyo_internal_asof_*` family.**
3. **Eliminate text-level normalizers**: extend the sqlparser fork (or
   add an AST rewrite pre-pass) to handle `ASOF JOIN ... USING (...)`
   and `ASOF LEFT JOIN`. Removes 3.1 and 3.3 entirely.
4. **Right-side ordering structure**: keep right partitions sorted by
   ts, switch `pick_asof_right` to binary search. Resolves P3.
5. **Null sentinel for IS NOT DISTINCT FROM derived keys**: prefix the
   value key with a single byte (`0x00` for NULL, `0x01` for non-NULL)
   to keep the null and empty-string cases distinct.
6. **Validation warning for ASOF + default ttl**.
7. **Metrics**: pending-left depth, evicted-right-with-pending-left,
   right-partition max size.

Items 1, 2, 3 are blocking for production. The rest are quality
improvements.

---

## 6. Summary

The implementation is now in much better shape than when this report was first
written. The two most important remaining gaps are:

- **P2 (TTL-bounded lookback)** — still needs stronger validation and
  observability.
- **P3 (linear candidate search)** — still needs a sorted/indexed right-side
  structure for hot-key efficiency.

Everything else in this document is either already fixed, significantly reduced,
or downgraded after re-checking the implementation.
