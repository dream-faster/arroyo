# PR #34 ASOF JOIN review and fix plan (ELI5)

This is the simple version of the main review document.

## What this PR is trying to do

It adds support for **ASOF JOIN**.

In plain English, that means:

- take one row from the left side;
- find rows on the right side with the same key;
- pick the **latest** right row whose time is **not after** the left row's time.

Example:

- right side has prices at times `10`, `15`, `25`
- left side has a trade at time `20`
- ASOF JOIN should pick the price at time `15`

## The short version of the review

The PR is close, but the current version can give the **wrong answer** in some
important streaming cases.

The biggest problem is this:

- the code can output an answer too early;
- then a better right-side row can show up later;
- and the code can output **another** answer for the same left row.

So instead of "one best match", users can get **duplicates** or **changed
matches**.

## The main problems, explained simply

### 1. It can answer too early

Imagine:

1. right row arrives at time `10`
2. left row arrives at time `20`
3. code outputs match `20 -> 10`
4. later, another right row arrives at time `15`

Now the correct match should be `20 -> 15`, because `15` is closer than `10`.

But the old answer was already sent out. That means the system can produce:

- first answer: `20 -> 10`
- later answer: `20 -> 15`

That is bad if users expect exactly one final answer.

### 2. Ties are messy

If two right rows have the **same timestamp**, the current code does not choose
between them in a clean, fully deterministic way.

That can lead to:

- duplicate outputs
- different outputs depending on arrival order
- confusing behavior after replay or restore

### 3. A private internal marker is exposed like a normal SQL function

The implementation uses a hidden helper named `_arroyo_asof`.

That helper is supposed to be for the planner only, but right now it looks too
much like a normal SQL function. A user may be able to write it directly and
accidentally turn a normal join into ASOF behavior.

That is risky because internal plumbing should not be part of the public SQL
surface.

### 4. Some bad queries fail too late

Right now, some mistakes are only caught when the job is already running.

For example:

- wrong timestamp type
- nullable timestamps with unclear behavior
- malformed ASOF conditions

These should usually fail earlier, during planning, with a clear error message.

### 5. It may be slow on hot keys

If one key has lots of rows, the current code may scan too much state over and
over again.

That can become expensive and slow for busy streams.

### 6. Old workers may misunderstand the new plan

The PR adds new ASOF config to the operator message.

That is wire-compatible, but an older worker that does not understand ASOF may
still run the job and behave like a normal inequality join instead of "pick the
single nearest row".

That would silently give the wrong result.

## The key decision for the fix

The plan chooses one clear behavior:

**Use final event-time semantics.**

That means:

- do **not** emit a left row's ASOF answer immediately;
- wait until the watermark says it is safe;
- then emit **one final answer**.

This is safer than guessing early and trying to fix things later.

## ELI5 fix plan

### Step 1: Write tests that describe the real behavior we want

Before changing logic, add tests for the tricky cases:

- a better right row arrives later
- two right rows have the same timestamp
- multi-key joins
- bad SQL should fail clearly
- nested ASOF joins should still be handled

Goal: the tests should prove what "correct" means.

### Step 2: Pick a clear tie-break rule

If two right rows have the same timestamp, the system must always choose the
same one.

Recommended rule:

- **earliest right arrival wins**

That rule is simple and stable.

To support it, the runtime should store a small internal sequence number for
right rows.

### Step 3: Stop emitting ASOF matches immediately

This is the biggest runtime fix.

Instead of answering right away:

- store left rows as "waiting"
- store right rows as candidates
- when the watermark advances far enough, finalize the waiting left rows
- compute the best right match then
- emit only once

That avoids duplicate answers.

### Step 4: Make the planner catch bad ASOF queries earlier

The planner should reject bad input before the worker starts.

It should check:

- ASOF is only used on inner joins
- there is at least one equality key
- the match condition is really `left_ts >= right_ts`
- the timestamp columns are valid
- nullable timestamps are either rejected or handled explicitly

### Step 5: Hide the internal `_arroyo_asof` plumbing from users

The internal marker should not be something users can type as if it were a
public SQL function.

Best fix:

- replace it with planner-only metadata

If that is not possible yet:

- reject user-written `_arroyo_asof(...)` calls

### Step 6: Make the SQL rewrite more complete

The ASOF rewrite currently handles the obvious cases, but nested SQL shapes may
slip through.

The fix is to recurse through more table-factor shapes and add tests for
parenthesized and nested joins.

### Step 7: Strengthen the planner/runtime contract

Today the planner passes timestamp column indexes to the runtime.

That works, but it is fragile. If schema layout changes, the runtime could read
the wrong column.

Safer fix:

- include field names as well as indexes
- validate them when the worker starts

### Step 8: Prevent old workers from silently running new ASOF plans

Add capability/version checks so:

- new ASOF plans only run on workers that support them
- old workers fail fast instead of producing wrong results

### Step 9: Make state lookup faster

For scale, right-side candidates should be stored in timestamp order so the
runtime can quickly find:

- the best right row at or before a given left timestamp

That is much better than rescanning everything for hot keys.

### Step 10: Re-run the normal Rust checks before merge

Before merging, run the usual checks:

```text
cargo fmt -- --check
cargo clippy --all-targets --workspace -- -D warnings
cargo nextest run -E 'kind(lib)'
cargo build
```

And also confirm:

- planner SQL tests pass
- integration tests pass
- CI is green
- docs describe the final ASOF behavior clearly

## What "done" looks like

This work is done when all of these are true:

1. each left row produces **zero or one** ASOF output
2. a later better right row does **not** create duplicates
3. equal timestamps use one deterministic tie-break rule
4. bad ASOF SQL fails during planning with clear errors
5. old workers cannot silently mis-run ASOF plans
6. hot keys do not require wasteful rescanning

## One-line summary

The PR has the right idea, but today it can answer too early and output the
wrong match twice. The fix is to make ASOF finalize on watermarks, choose ties
deterministically, validate more in the planner, and block old workers from
silently running the new plan incorrectly.
