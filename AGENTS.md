# AGENTS

## Default workflow

Use the repo in this order:

1. Write or update tests first.
2. Implement the code.
3. Verify the relevant tests are green before considering the work done.

## Additional agent guidance

Also apply the behavioral guidance in `CLAUDE.md` when working in this repo.
Treat `AGENTS.md` as the repo workflow/source of truth for execution order and
checks, and `CLAUDE.md` as additional guidance for thinking before coding,
keeping changes simple, staying surgical, and working from clear success
criteria.

## Pre-commit checks

Run the same categories of checks that CI enforces before committing.

> Note: Do not run `pnpm`/Web UI checks before committing; they are not necessary for local pre-commit validation.

| Category | Commands |
| --- | --- |
| Formatter | `cargo fmt -- --check` |
| Linter | `cargo clippy --all-targets --workspace -- -D warnings` |
| Tests | `cargo nextest run -E 'kind(lib)'` |
| Compiler | `cargo build` |
