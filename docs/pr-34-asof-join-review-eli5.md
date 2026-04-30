# PR #34 ASOF JOIN review and DuckDB-compatible fix plan (ELI5)

This is the plain-English version of the review.

## What changed after the DuckDB research

The earlier version of the plan was based on what seemed reasonable for Arroyo.

After researching DuckDB's docs, tests, and source, the goal is now sharper:

> **Make Arroyo behave like DuckDB for ASOF JOIN.**

That means we should stop designing our own ASOF rules and instead copy the
important DuckDB ones.

The detailed technical version lives in:

- `docs/duckdb-asof-compatibility-spec.md`
- `docs/pr-34-asof-join-review.md`

## What ASOF JOIN means in DuckDB

Plain version:

- look at one row on the left
- find right-side rows that qualify
- keep only **one** right-side row
- that one should be the **nearest qualifying row** according to the ASOF rule

Most common example:

```sql
left.time >= right.time
```

That means:

- only use right rows at or before the left time
- pick the latest such right row

Example:

- right times: `10`, `15`, `25`
- left time: `20`
- answer: `15`

## The important DuckDB rules we must match

### 1. Inner and left ASOF joins

DuckDB clearly supports:

- `ASOF JOIN`
- `ASOF LEFT JOIN`

So Arroyo should match those.

### 2. Not just `>=`

DuckDB supports all four inequality directions:

- `>=`
- `>`
- `<=`
- `<`

So Arroyo should not hard-code only `>=`.

### 3. Equality keys are optional

You do **not** always need something like `symbol = symbol`.

DuckDB allows an ASOF join with only the one inequality condition.

So Arroyo should not force users to provide an equality key.

### 4. Extra join conditions can be `=` or `IS NOT DISTINCT FROM`

DuckDB allows normal equality matching and also the SQL form where NULLs are
allowed to match:

```sql
a IS NOT DISTINCT FROM b
```

So Arroyo has to keep that difference. Plain `=` and
`IS NOT DISTINCT FROM` are **not** the same.

### 5. DuckDB's `USING` rule is special

With:

```sql
ASOF JOIN ... USING (symbol, "when")
```

DuckDB treats:

- `symbol` as equality
- `"when"` as the ASOF ordering column

And if you do `SELECT *`, DuckDB keeps the **left** `"when"` column in the
merged output.

Arroyo should do the same.

### 6. Ties should follow DuckDB's ordered result, not our old custom rule

The earlier Arroyo-only plan suggested **earliest arrival wins**.

That is no longer the target.

DuckDB's implementation sorts the right-side rows and then picks the **final
qualifying row** in that ordered run.

So Arroyo should do the same thing in a streaming-friendly way.

### 7. NULL behavior matters

DuckDB's behavior is basically:

- NULL ASOF ordering values do not match
- `=` does not match NULLs
- `IS NOT DISTINCT FROM` can match NULLs
- `ASOF LEFT JOIN` keeps the left row and fills right columns with NULL when
  there is no match

Arroyo should copy that behavior.

## What is wrong with the current PR

The current PR still does not match DuckDB because it:

1. only supports `>=`
2. only supports inner ASOF
3. assumes an equality key is required
4. does not properly handle `IS NOT DISTINCT FROM`
5. can emit answers too early in streaming mode
6. can produce duplicates
7. uses a tie rule that does not match DuckDB's ordered-run behavior
8. exposes a private planner helper as if it were normal SQL

## The new fix plan, in simple terms

### Step 1: Write DuckDB-shaped tests first

Add tests for:

- `ASOF JOIN`
- `ASOF LEFT JOIN`
- all four inequality directions
- with and without equality keys
- `=` and `IS NOT DISTINCT FROM`
- `USING (...)`
- tie cases
- bad queries that DuckDB would reject

This makes the target clear.

### Step 2: Teach the planner the real DuckDB rules

The planner should understand:

- exactly one inequality is required
- that inequality can be `>=`, `>`, `<=`, or `<`
- other ASOF join predicates may be `=` or `IS NOT DISTINCT FROM`
- equality keys are optional
- `USING (...)` follows DuckDB's special last-column rule

### Step 3: Give the runtime enough information

The runtime config must carry more than just two timestamp indexes.

It also needs:

- which inequality operator is being used
- whether the match is strict or non-strict
- how equality keys behave with NULLs
- whether the join is inner or left

### Step 4: Stop guessing too early in the stream

DuckDB is batch, so it sees all the data before deciding.

Arroyo is streaming, so the way to match DuckDB is:

- wait until the watermark says the answer is final
- then emit the answer once

That gives Arroyo the same **final** result DuckDB would give.

### Step 5: Use DuckDB-style tie behavior

If multiple right rows have the same ordering value, Arroyo should pick the
same kind of row DuckDB would pick:

- the final qualifying row in the ordered right-side partition

To do that in streaming, Arroyo should keep a stable per-partition sequence so
ordering stays deterministic across checkpoint/replay too.

### Step 6: Match DuckDB's NULL behavior

Do not invent a repo-local NULL policy.

Instead:

- NULL ordering keys should not match
- `=` stays NULL-sensitive
- `IS NOT DISTINCT FROM` allows NULL-to-NULL matches
- left ASOF join returns NULL right columns when unmatched

### Step 7: Hide internal plumbing from users

The internal `_arroyo_asof` helper should not act like a normal user SQL
function.

Best outcome:

- move the ASOF detection fully into the planner/binder

If that cannot happen yet:

- reject user-written `_arroyo_asof(...)` calls

### Step 8: Document what is and is not DuckDB-compatible

The docs should clearly say:

- which ASOF join forms are supported
- which inequalities are supported
- how `USING` works
- how NULLs behave
- how ties behave

And they should point people to the compatibility spec.

## What "done" means now

This work is done when Arroyo gives the same final answers DuckDB would give
for the supported ASOF SQL surface.

That means:

1. one inequality, not zero and not two
2. support for `>=`, `>`, `<=`, `<`
3. support for `ASOF JOIN` and `ASOF LEFT JOIN`
4. equality keys optional
5. support for `=` and `IS NOT DISTINCT FROM`
6. DuckDB-style `USING` behavior
7. DuckDB-style NULL behavior
8. deterministic DuckDB-compatible tie handling
9. no duplicate speculative answers for a single left row

## One-line summary

The new goal is not just "make ASOF JOIN work" — it is **make Arroyo's ASOF
JOIN behave like DuckDB's ASOF JOIN**, and use streaming watermarks only as the
mechanism for reaching DuckDB's final answers.
