# DuckDB ASOF JOIN compatibility spec

This document defines the target behavior Arroyo should implement if the goal is
to match DuckDB's ASOF JOIN semantics as closely as possible.

It is based on DuckDB's public documentation, blog post, binder code, physical
operator code, and SQL tests:

- DuckDB docs: `guides/sql_features/asof_join.html`
- DuckDB docs: `sql/query_syntax/from.html#as-of-joins`
- DuckDB blog: `2023/09/15/asof-joins-fuzzy-temporal-lookups.html`
- DuckDB binder: `src/planner/binder/tableref/bind_joinref.cpp`
- DuckDB physical operator:
  `src/execution/operator/join/physical_asof_join.cpp`
- DuckDB tests: `test/sql/join/asof/test_asof_join.test`

## 1. Scope of compatibility

Arroyo should target **DuckDB result compatibility**, not a custom ASOF dialect.

For a finite input set, Arroyo should produce the same joined rows that DuckDB
would produce for the same SQL and data, subject to two practical limits:

1. Arroyo is streaming, while DuckDB is batch. Arroyo may need watermark-based
   finalization to reach the same final answers.
2. DuckDB's public docs do not define a user-visible secondary tie-breaker for
   multiple right rows with the same partition keys and ordering value. The
   physical operator chooses the final qualifying row in the right-side sorted
   run. Arroyo should define a deterministic streaming equivalent and document it
   as the compatibility mapping.

## 2. Supported SQL surface

### 2.1 Join forms

DuckDB publicly documents:

- `ASOF JOIN` (inner semantics)
- `ASOF LEFT JOIN`

Compatibility target for Arroyo:

- implement `ASOF JOIN`
- implement `ASOF LEFT JOIN`
- do **not** claim DuckDB compatibility for `ASOF RIGHT JOIN` or
  `ASOF FULL/OUTER JOIN` until parser/binder/runtime tests confirm the exact SQL
  surface and behavior

Note: DuckDB's physical operator source contains right/full outer-join plumbing,
but the public docs and tests retrieved here only cover inner and left-ASOF
usage. Compatibility should follow the public SQL surface first.

### 2.2 ON-clause rules

DuckDB requires:

1. **exactly one inequality** predicate for the ASOF ordering field
2. the inequality may be any of:
   - `>=`
   - `>`
   - `<=`
   - `<`
3. any other join predicates must be:
   - `=`
   - `IS NOT DISTINCT FROM`

DuckDB test evidence:

- missing inequality -> binder error
- multiple inequalities -> binder error
- `IS NOT DISTINCT FROM` is accepted for non-order predicates

Compatibility target for Arroyo:

- allow exactly one ordering inequality
- allow zero or more additional equality-style predicates
- reject multiple ordering inequalities
- reject non-equality/non-`IS NOT DISTINCT FROM` extra join predicates as ASOF
  join keys

### 2.3 Equality keys are optional

DuckDB examples usually partition by equality keys like `symbol`, but the blog
explicitly discusses ASOF joins with **no equality condition**.

Compatibility target for Arroyo:

- do **not** require at least one equality key
- support global ordered ASOF joins with only the single inequality condition

### 2.4 USING syntax

DuckDB supports `USING` with ASOF under these rules:

1. the **last** field in the `USING` list is the ordering field
2. for ASOF `USING`, that last field uses `>=`
3. preceding fields are equality fields

Example:

```sql
SELECT *
FROM trades t
ASOF JOIN prices p USING (symbol, "when");
```

Compatibility target for Arroyo:

- support `USING` with DuckDB's exact rule: last column is the `>=` ordering key
- keep the left/probe column as the merged output column for `SELECT *`

## 3. Result semantics

### 3.1 Core rule

For each left row, within the equality partition (if any), find the single
right row that is the nearest qualifying row under the inequality.

For the common case:

```sql
left.ts >= right.ts
```

the chosen row is the right row with the **largest** `right.ts` such that:

- `right.ts <= left.ts`
- equality predicates hold

DuckDB's docs phrase this as "find the most recent price before the holding's
timestamp".

### 3.2 General inequality semantics

DuckDB's blog gives the interval interpretation for all four inequalities:

| Inequality | Interval interpretation |
| --- | --- |
| `>` | `(Tn, Tn+1]` |
| `>=` | `[Tn, Tn+1)` |
| `<=` | `(Tn-1, Tn]` |
| `<` | `[Tn-1, Tn)` |

Compatibility target for Arroyo:

- carry both **direction** and **strictness** into planning and runtime
- do not hard-code only `>=`

### 3.3 At-most-one match

DuckDB ASOF returns **at most one right row per left row**.

Compatibility target for Arroyo:

- inner ASOF: output zero or one row per left row
- left ASOF: output exactly one row per left row, filling unmatched right
  columns with `NULL`

## 4. NULL semantics

### 4.1 Ordering inequality columns

DuckDB's physical operator sorts ordering keys with `NULLS LAST` and filters out
NULL matches for null-sensitive keys. In practice:

- if the left ordering expression is `NULL`, that left row does not match
- if the right ordering expression is `NULL`, that right row is not selected

Compatibility target for Arroyo:

- ordered ASOF columns with `NULL` never satisfy the ASOF inequality match

### 4.2 Equality predicates

DuckDB distinguishes:

- `=`: NULLs do **not** match
- `IS NOT DISTINCT FROM`: NULLs **do** match

Compatibility target for Arroyo:

- preserve the same distinction
- do not collapse all equality predicates into plain `=`

## 5. Tie behavior

DuckDB's physical operator:

1. sorts the right side inside each equality partition
2. binary-searches for the first non-matching value
3. chooses the **previous** value as the match

This means the chosen row is the **final qualifying right row in the right-side
sorted run** for that partition.

Practical compatibility rule for Arroyo:

- within each equality partition, sort right rows by:
  1. the ordering expression in DuckDB's direction
  2. a deterministic secondary sequence that represents right-side row order
- choose the final qualifying row in that order

Recommended Arroyo mapping:

- preserve right-side arrival order as the secondary sequence inside each
  partition, store it in state, and make replay/checkpoint preserve it

That gives Arroyo a deterministic streaming analogue of DuckDB's "last
qualifying row in sorted order" behavior.

## 6. Non-join conditions inside ON

DuckDB's ASOF tests show that non-join conditions in the `ON` clause can be
present and are ignored for ASOF key extraction.

Example from DuckDB tests:

```sql
ON 1 = 1 AND p.ts >= e.begin
```

Compatibility target for Arroyo:

- extract exactly one ASOF inequality and any equality/`IS NOT DISTINCT FROM`
  join keys
- preserve other predicates as regular post-match filters
- do not reject the query only because `ON` contains constants or other
  non-key expressions

## 7. Output-column behavior

DuckDB's `USING` behavior for ASOF differs from normal equality joins because
the ordering columns are not equal.

For:

```sql
SELECT *
FROM holdings h
ASOF JOIN prices p USING (ticker, "when");
```

DuckDB returns the merged `ticker` and `when` columns from the **left/probe**
side, not the right/build side.

Compatibility target for Arroyo:

- when supporting `USING`, merge columns exactly like DuckDB
- the merged ordering column in `SELECT *` must come from the left side
- users must explicitly project both columns if they want to see both timestamps

## 8. Planner responsibilities

To be DuckDB-compatible, Arroyo's planner should:

1. parse and retain the exact inequality operator
2. allow `=` and `IS NOT DISTINCT FROM` on non-order predicates
3. require exactly one inequality
4. not require equality predicates
5. support `USING` with DuckDB's last-column-is-inequality rule
6. preserve additional non-key predicates as filters
7. preserve left/right orientation because ASOF is not commutative

## 9. Runtime responsibilities

To be DuckDB-compatible, Arroyo's runtime should:

1. compute the same final match DuckDB would compute for the same left/right data
2. emit at most one match per left row
3. support all four inequality directions and strictness choices
4. apply equality partitioning with `=` vs `IS NOT DISTINCT FROM` semantics
5. apply left-ASOF null-padding for unmatched left rows
6. implement DuckDB-style tie resolution via "last qualifying row in ordered
   right partition"

Because Arroyo is streaming, the runtime will likely need to:

- buffer left rows until the relevant watermark proves the result is final
- sort or index right rows within each partition
- use watermark finalization to produce DuckDB-equivalent final answers

## 10. Recommended compatibility limits for the first implementation

To avoid claiming more than DuckDB-documented behavior:

### In scope for v1 DuckDB-compatible implementation

- `ASOF JOIN`
- `ASOF LEFT JOIN`
- one inequality from `{>=, >, <=, <}`
- zero or more `=` / `IS NOT DISTINCT FROM` equality-style predicates
- `USING (...)` with DuckDB's last-column rule
- DuckDB-compatible left-column output behavior for `USING`
- deterministic tie behavior based on right-side ordered run

### Out of scope until separately verified

- `ASOF RIGHT JOIN`
- `ASOF FULL JOIN`
- undocumented DuckDB tie behavior beyond the observable "last qualifying row in
  sorted order"
- any DuckDB optimizer-specific plan shape guarantees

## 11. Arroyo acceptance tests for DuckDB compatibility

Arroyo should add tests that mirror DuckDB semantics:

1. inner ASOF `>=` with equality key
2. left ASOF `>=` with unmatched left rows producing NULL right columns
3. ASOF with **no equality key**
4. one test each for `>=`, `>`, `<=`, `<`
5. `=` vs `IS NOT DISTINCT FROM` on partition keys with NULL values
6. `USING (k, ts)` output shape where `SELECT *` keeps the left/probe `ts`
7. extra non-key `ON` predicates that are preserved as filters
8. missing inequality -> planner error
9. multiple inequalities -> planner error
10. equal ordering-value ties choosing the final qualifying row in deterministic
    right-side order

## 12. Bottom line

Matching DuckDB means Arroyo should not implement a custom "ASOF-like" join.
It should implement:

- DuckDB's SQL surface
- DuckDB's single-inequality extraction rules
- DuckDB's null and equality semantics
- DuckDB's `USING` output behavior
- DuckDB's final-match semantics, adapted to streaming via watermark-based
  finalization
