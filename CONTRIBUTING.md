# Contributing to neumann

Thank you for helping. neumann's value is in the guarantees it keeps across
crashes, cancels and overloads, so the bar for changes to the core is high and
the process below exists to keep it there.

## Before you start

Read [docs/overview.md](docs/overview.md). For anything that changes
behaviour, find the section of [docs/design.md](docs/design.md) (Chinese) that
specifies it; if your change alters the design, update design.md in the same
change. Open an issue first for anything large.

## Running the checks

```
make check     # gofmt -l . must be empty, go vet ./..., go test -race ./...
make test      # go test ./..., without the race detector
```

`make check` must pass before a change is reviewed. Everything runs offline;
tests use the scripted model and fake clock in package `sim`. The live
acceptance tests in `acceptance/` are skipped unless `NEUMANN_ACCEPT_LIVE=1`
(see the README) and are not required for a change to be merged.

## Tests first

Write the test that fails before you write the fix or the feature, and keep it
in the change. A bug fix comes with a test that reproduces the bug. Tests that
depend on timing use `sim.Clock` rather than sleeping; tests that wait for
something use a deadline and say what they were waiting for when it expires.

## Core changes

A change that touches `internal/core`, `internal/sched`, `internal/group`,
`mem` (including the stores), or the commit, recovery or cancellation paths in
`neumann.go` is a core change. On top of `make check` it needs:

1. **A race stress run**, green:

   ```
   go test -race -count=20 -timeout 60m ./internal/... .
   ```

   Add the packages you touched (for example `./mem/...`). It can take ten
   minutes or more, past `go test`'s default 10m timeout, hence `-timeout`.
   Green once is not enough: the interleavings that matter are rare.
2. **An independent review** by someone who did not write the change, with
   the invariants it must keep listed for the reviewer (design §4, §8, §18).
   Green tests have repeatedly missed bugs that a review found: a side-effect
   call dispatched after a committed cancel, a re-sent call recorded as never
   sent. Fixes from the review are reviewed again before merge.
3. **Crash coverage** for every new commit point: the
   `TestCrashAtEveryCommitPoint*` tests in `recovery_test.go` crash before and
   after each commit; a new kind of commit needs a case there.
4. **The conformance suite** for any change to store semantics: both stores
   run `mem/storetest`, and a new rule gets a case in it.

## Style

- Format with `gofmt`; `go vet` must be clean.
- Every exported name has a doc comment that says what it does and what it
  guarantees, not how it is implemented. Package comments say what the package
  is and how to start using it.
- Errors that callers branch on are sentinel values or types, documented on
  the function that returns them.
- Keep `internal/group` free of goroutines and I/O: it decides, the vCore
  loop acts.

## Commit messages

One line, no body. An optional lowercase prefix names the area or the kind of
change, followed by a sentence that says what is now true:

```
fix: a 429 with no retry hint counts toward the retries and backs off exponentially
sched: backoff wake timers, OnWake, WaitingFor and AfterFunc
docs: every task and machine event kind in §12
```

Common prefixes: `feat`, `fix`, `test`, `docs`, or a package name (`sched`,
`model`, `jsonl`, `bus/mcp`). Do not add trailers such as `Co-Authored-By` or
tool attributions.

## License

By contributing you agree that your contribution is licensed under the
Apache License 2.0, the license of this project (see LICENSE).
