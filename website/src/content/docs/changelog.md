---
title: Changelog
summary: Notable changes per release. 0.1.6 hardens worker lifecycle recovery; 0.1.5 adds ping_workers min_responses; 0.1.4 adds the worker & database health API; 0.1.3 adds CronSchedule; 0.1.2 is a breaking release headlined by the strict-serde redesign.
related: [./monitoring/worker-health, ./migrations/migration-to-0-1-2, ./internals/serialization]
tags: [changelog, releases, breaking-changes, 0.1.6, 0.1.5, 0.1.4, 0.1.3, 0.1.2]
---

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
horsies is pre-1.0: breaking changes may land in minor or patch releases, and
there is no migration contract between pre-1.0 versions.

## 0.1.6 — 2026-06-04

### Added

- Schema v2 task lifecycle metadata: `is_workflow_task`, `finalizing_at`, and
  `finalizing_by_worker_id`. Existing workflow-linked task rows are backfilled
  once during migration; new direct task sends and workflow enqueues write the
  flag explicitly.
- `RecoveryConfig.finalizing_stale_threshold_ms` to protect the child-to-parent
  finalization handoff without disabling stale child recovery.
- Worker-specific broker pool settings:
  `worker_pool_size`, `worker_max_overflow`,
  `worker_child_pool_min_size`, and `worker_child_pool_max_size`.

### Changed

- Worker claim batching now defaults to filling available local/global capacity
  (`max_claim_batch=0`), while a positive `max_claim_batch` remains an explicit
  per-queue fairness cap.
- Soft-prefetch local budgeting counts already-owned `CLAIMED` rows, preventing
  a worker from hoarding beyond `processes + prefetch_buffer`.
- Plain tasks skip workflow-specific child preflight/finalize checks using the
  persisted `is_workflow_task` flag.
- Worker processes now use a smaller coordinator pool by default (`3 + 2`
  overflow) while producer/web broker defaults remain unchanged (`30 + 30`).
- Child worker psycopg pools now default to `min_size=0`, `max_size=2` instead
  of `1..5`, and are configurable from `PostgresConfig`.
- Worker child processes are warmed before the parent opens long-lived database
  sockets, and replacement executors use a non-inheriting start method after
  startup, avoiding fork inheritance of psycopg connections.

### Fixed

- `BrokenProcessPool` and child-future failures now distinguish `CLAIMED` work
  from `RUNNING` work. `RUNNING` tasks respect `WORKER_CRASHED` retry policy or
  persist a terminal `WORKER_CRASHED` result instead of being blindly requeued.
- Finalization failure handling now preserves queue/workflow context across
  retries, schedules phase-2 retries after terminal state is committed, and
  returns synthetic `TaskResult` payloads for worker-failure/corrupt-result
  terminal paths so workflow advancement and capacity notifications are not
  skipped.
- The reaper skips recent `finalizing_at` handoffs, recovers stale finalizing
  rows after `finalizing_stale_threshold_ms`, and still recovers hung child
  processes even when the parent worker coordinator is alive.
- Workflow pause now cancels claimed-but-not-started internal task rows and
  resets their workflow nodes to `READY`, including retry-window nodes already
  marked `RUNNING`, so resume enqueues fresh task rows instead of leaving
  orphan claims.

## 0.1.5 — 2026-06-02

### Added

- `ping_workers(min_responses=N)` / `ping_workers_async(min_responses=N)`:
  return as soon as `N` distinct workers reply instead of waiting the full
  `timeout_seconds`. `min_responses=1` is a fast fail-open liveness gate — a
  healthy fleet answers in milliseconds; only a degraded fleet pays the
  timeout. Removes the latency floor for high-frequency `/health` probes.
  Pongs are de-duplicated by `worker_id`. See
  [Worker & Database Health](/monitoring/worker-health/).

## 0.1.4 — 2026-06-02

0.1.4 adds a typed worker & database health API: active ping-pong liveness for
workers, a database reachability probe, and typed reads over the worker-state
timeseries (including idle workers). It retires the untyped `get_worker_stats`.

### Added

- Database reachability probe: `app.ping_database_async()` / `ping_database()`
  run `SELECT 1` through the live broker pool and return
  `BrokerResult[DatabasePing]` with measured round-trip latency. Callable from a
  running event loop.
- Active worker ping-pong: `app.ping_workers_async(target_worker_id=None,
  timeout_seconds=2.0)` / `ping_workers()` broadcast a ping over LISTEN/NOTIFY
  and collect `WorkerPong` replies within the window. A reply proves the
  worker's event loop is responsive and that it can reach Postgres. Pass
  `target_worker_id` to probe one worker.
- Typed worker-state reads over the `horsies_worker_states` timeseries:
  `app.list_worker_states_async()` (latest snapshot per worker, including idle
  workers), `get_worker_state_async(worker_id)`, and
  `get_worker_state_history_async(worker_id, limit=None)` returning
  `WorkerStateSnapshot`. See [Worker & Database Health](/monitoring/worker-health/).
- New exports: `DatabasePing`, `WorkerPong`, `WorkerStateSnapshot`.
- New broker error codes: `DB_PING_FAILED`, `WORKER_PING_FAILED`.

### Removed

- `broker.get_worker_stats()` (untyped `list[dict]`, RUNNING-tasks only, missed
  idle workers). Use `app.list_worker_states_async()` — typed and inclusive of
  idle workers.

## 0.1.3 — 2026-05-31

0.1.3 adds `CronSchedule`, a typed 5-field cron-style schedule pattern. It brings
wall-clock alignment and minute-offset load staggering that `IntervalSchedule`
cannot express — without cron strings.

### Added

- `CronSchedule`: a typed 5-field cron-style schedule pattern (`minute`, `hour`,
  `month` term lists plus a `DaySelector`). Provides wall-clock alignment and
  minute-offset staggering that `IntervalSchedule` cannot express, with no cron
  strings. The day-of-month vs day-of-week ambiguity is explicit through
  `EitherDay` (OR) and `BothDays` (AND). See
  [Schedule Patterns](./scheduling/schedule-patterns). New exports: `CronSchedule`,
  `Month`, `CronEvery`, `CronStep`, `CronValues`, `CronRange`, `CronEnumValues`,
  `CronEnumRange`, `CronEnumStep`, `DaySelector`, `EveryDay`, `ByMonthDay`,
  `ByWeekday`, `EitherDay`, `BothDays`.

## 0.1.2 — 2026-05-29

0.1.2 is a **breaking** release headlined by the strict-serde redesign: the wire
stops carrying class identity, and every task parameter and return type must
classify into a concrete declared shape. It also closes a large batch of
workflow, worker, scheduler, and broker correctness bugs found in the
2026-05-24 audit.

For mechanical, copy-paste upgrade steps, see the
[Migrating to 0.1.2](./migrations/migration-to-0-1-2) guide.

### Breaking Changes

**Serialization (strict-serde)**

- The wire format no longer encodes class identity. The receiver's declared
  type drives every decode through `pydantic.TypeAdapter`; values carry only a
  single envelope marker, not per-value class tags. Banned types (`Any`,
  `object`, bare `dict`/`list`/`tuple`, `TypeVar`, bare `BaseModel`,
  `TypedDict`, `bytes`, `set`/`frozenset`, `Callable`, `pathlib.PurePath`
  subclasses) are rejected at `@app.task` registration. (#54)
- Legacy `codec/serde.py` (`to_jsonable` / `rehydrate_value` / class-tag
  envelopes) is removed. The strict `codec/json_io` (`dumps_json` /
  `loads_json`) is the only JSON boundary; `serialize_error_payload` and
  `serialize_task_options` moved to dedicated modules. (#84)
- `broker.get_result` / `broker.get_result_async` are removed. Use
  `app.get_result(_async)` for a typed decode (returns an outer `BrokerResult`
  wrapping `TaskResult[Any, TaskError]`) or `broker.get_raw_result_record_async`
  for the raw envelope. (#84)
- `dumps_json` rejects tuples and non-string dict keys instead of silently
  coercing them to lists / string keys. Tuple-annotated parameters still
  round-trip through the typed path. (#82)
- `dumps_json` rejects non-UTF-8 output (lone surrogates) up front instead of
  failing later on the Postgres `TEXT` insert. (#71)
- `SubWorkflowNode.kwargs` and `SubWorkflowSummary.output` are now typed, and a
  `SubWorkflowNode`'s result type resolves by unique `definition_key`. Child
  workflow definitions must declare a stable `definition_key`. (#63, #68)
- `SubWorkflowSummary.from_json` fails closed (raises `ValueError`) on a
  corrupt or unknown status instead of silently coercing to `FAILED`. (#70)

**Workflow API**

- Subworkflow enqueue now requires an explicit `broker` argument. (#55)
- `WorkflowHandle.get()` / `get_async(timeout_ms=0)` now returns immediately —
  the result if the workflow is already terminal, otherwise `WAIT_TIMEOUT` —
  instead of blocking forever. Only `timeout_ms=None` disables the timeout.
  (#86)

**Tasks, scheduling & CLI**

- `.schedule(delay=...)` rejects negative and non-integer delays at the wrapper
  boundary (`delay=0` still means "enqueue now"). (#73)
- Scheduled tasks validate kwargs-encodability at startup and reject positional
  arguments (kwargs-only contract), instead of failing on every tick. (#80)
- The CLI rejects a conflicting `-m/--module` flag and positional module path
  instead of silently preferring the flag. (#76)

### Added

- Opt-in `RetryPolicy.max_delay_seconds` cap so exponential backoff no longer
  grows unbounded. (#44)
- `encode_task_error` codec helper and a public `decode_task_error` that
  polymorphically preserves `SubWorkflowError`. (#59, #60)
- Err-only fast path in recovery and `get_task_info` (failed tasks surface
  without a local `ok_type`). (#61)
- `__wrapped__` on the task wrapper so `inspect.signature` / `inspect.unwrap`
  resolve the original function. (#75)

### Fixed

**Workflow engine & recovery**

- Duplicate `waits_for` edges no longer falsely trip cycle detection. (#22, #30)
- Partial child-workflow commits are prevented on a mid-loop validation
  failure, so recovery can no longer miss an orphaned `RUNNING` child. (#24)
- Terminal-state CAS guards added to the workflow-task-failed and
  parent-node-result updates. (#25, #26)
- First-failure error semantics preserved: per-failure error selection is
  serialized and recovery recomputes the first-by-index error. (#27, #29)
- `on_error=PAUSE` cascades the pause to running child workflows. (#28)
- Subworkflow cycle detection is keyed on definition identity, not the display
  name. (#33)
- `WorkflowContext` private result/summary state survives dump/restore. (#31)
- Ready subworkflows stay ready without a broker, and recovery re-evaluates
  demoted nodes in the same pass. (#62)
- Workflow tasks are locked before cancellation, closing a worker-pickup race.
  (#65)
- All `args_from` type errors are surfaced in a single pass. (#69)
- `app.workflow()` no longer mutates the caller's `TaskNode`. (#79)
- `WorkflowContext.result_for` accepts `SubWorkflowNode` in its type signature.
  (#81)

**Worker**

- Finalize retries are tracked as finalizers, so they are no longer cancelled
  at shutdown (workflow advancement is not dropped). (#35, #52)
- `RUNNING` tasks are requeued after a `BrokenProcessPool` crash. (#37)
- Concurrent executor restarts are serialized (no leaked pools). (#49)
- Nonrunnable-task cleanup is guarded by worker ownership. (#51)

**Broker, listener & scheduler**

- The racing `add_reader` on the dispatcher fd is dropped, and the dispatcher is
  paused during a health-disconnect reset. (#45, #47)
- Unverifiable enqueue conflicts fail instead of assuming idempotent success.
  (#48)
- Normal-branch schedule advancement is anchored to slot time, eliminating
  interval drift on late ticks. (#46)

**Persistence**

- ORM `datetime` defaults are evaluated per row instead of being captured at
  import, so `updated_at` advances on ORM updates. (#23)

**Models, codec & app**

- `TaskError.model_dump(mode='json')` flattens a live exception instead of
  raising. (#72)
- `SubWorkflowError` subtype and fields are preserved through error
  round-trip. (#59)
- Per-task and global `exception_mapper` reserved-code collisions are detected
  (the check matches `Mapping`, not just `dict`). (#85)
- Workflow package `__all__` drift fixed; the init log no longer asserts a
  role. (#83)

**CLI & utils**

- The CLI installs signal handlers before schema init and closes brokers on a
  startup failure. (#41, #43)
- Tasks may close over helper functions without a false
  `TASK_PREDECORATED_NOT_SUPPORTED` rejection. (#40)
- The docs tarball fetch has a download timeout and a verified prefix guard.
  (#74)
- `LoopRunner.stop()` no longer leaves the runner half-stopped, and retry
  jitter is floored before the spread, then applied upward (no lower-half
  collapse). (#78)

### Internal

- Dead-code removal, regression-test additions, fixture migrations to the
  strict envelope, and review follow-up tightenings. (#32, #34, #36, #38, #39,
  #42, #67, #77, #87)
