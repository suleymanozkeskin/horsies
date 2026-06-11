# Changelog

All notable changes to **horsies** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The project is pre-1.0: breaking changes may land in minor or patch releases,
and there is no migration contract between pre-1.0 versions.

## [Unreleased]

Workflow-completion performance redesign, supervisor-contract fixes, and the
close of the raise-contract documentation track. Schema migrates v7 → v8
automatically on first broker start.

### Changed

- Workflow completion at scale: terminal-set resolution rewritten as a
  payload-free edge read plus in-process set difference (finalizing
  completion at 1000 tasks: 168.7ms → 11.7ms under the workflow lock);
  child-workflow info collapsed to a single-pass query; new composite
  index `(workflow_id, status, task_index)` (schema v8) removes the
  per-failure first-failed scan.
- Subworkflow parent propagation is de-nested: each ancestor level now
  advances in its own transaction instead of recursing root-ward while
  holding every descendant's `FOR UPDATE` lock. A child workflow's
  `workflow_done` NOTIFY is therefore visible slightly before its parent
  node advances (waiters re-read their own workflow's status on wake).
  A crash between propagation levels is healed by workflow recovery;
  full self-healing requires `recovery_config` (the CLI wires it;
  programmatic workers should too).

### Fixed

- A worker whose executor restart failed from a background finalizer
  path now exits non-zero for supervisor restart instead of running on
  as an executorless zombie that claims nothing.
- `horsies worker` exits 1 (was 0) when startup times out after
  exhausting the resilience retry budget — a clean exit suppressed
  supervisor restarts.
- Listener `UNLISTEN` failures during unsubscribe no longer raise into
  result-waiter cleanup paths, and the channel is always untracked so a
  reconnect cannot resurrect a ghost `LISTEN`.
- `app.check()`: a workflow builder whose signature cannot be
  introspected now folds into the validation report instead of crashing
  the check phase.
- Schedule state rows self-heal: a schedule whose state-row creation
  failed at scheduler startup (or whose row was deleted externally) was
  invisible to the tick loop and stayed dormant until a restart. Each
  tick now recreates missing rows for enabled schedules.

### Removed

- `ScheduleStateManager.delete_state` — dead since schedulers stopped
  deleting foreign schedule-state rows at startup; no production caller.

### Documentation

- Raise-contract docstrings across the worker package, workflow engine,
  scheduler, app/CLI boundary, and listener: every fallible function now
  names the seam that recovers from its failure and how (the
  fallible-audit Result-conversion track is closed).

## [0.1.7] - 2026-06-10

Correctness and performance hardening from a full-project review, plus task
timeouts, child-process hooks, and uncapped queue concurrency.
Schema migrates from v2 to v7 automatically on first broker start.

### Added

- `CustomQueueConfig.max_concurrency` accepts `None` as an explicit
  uncapped sentinel (mirroring `cluster_wide_cap=None`): no per-queue
  limit is enforced and the claim pass skips that queue's in-flight
  count query. `0` remains valid (pauses claiming); negative values are
  now rejected at config validation.
- Per-child-process hook: `@app.on_child_process_start` registers sync
  zero-argument functions that run once in every worker child, after
  task imports and before horsies opens its own child pool. The
  supported seam for disposing fork-inherited app engines and setting
  worker-specific pool policy (Celery `worker_process_init` /
  Dramatiq `after_process_boot` parity). Fail-closed: a raising or
  hung hook (10 s budget) exits the child with a dedicated code and
  the worker stops with the hook named instead of restart-looping.
- Per-task execution timeout: `@app.task(..., timeout_ms=...)` (minimum
  1000 ms, measured from dispatch). On expiry the worker records a
  `TASK_TIMEOUT` attempt, fails the task — or schedules a retry when
  `"TASK_TIMEOUT"` is in `auto_retry_for` — and kills the child
  process. The kill restarts the worker's process pool; sibling tasks
  in flight recover through crash recovery. A deadline that fires
  before user code starts requeues the task instead.

### Breaking

- `catch_up_missed=False` now matches its documentation: after scheduler
  downtime, only the most recent due slot fires (skipped slots are
  logged) and the schedule resumes strictly in the future. The previous
  behavior accidentally replayed the entire backlog one run per tick;
  deployments relying on that replay must set `catch_up_missed=True`.
- `PostgresConfig.database_url` and `session_database_url` are now
  pydantic `SecretStr` — `repr()`/`model_dump()` mask credentials. Code
  reading these fields must call `.get_secret_value()`. String inputs
  validate as before.
- Producer pool defaults dropped from `30 + 30` to SQLAlchemy's `5 + 10`
  (`pool_size` / `max_overflow`); raise them explicitly for
  high-throughput producers.
- Scheduler startup no longer deletes `horsies_schedule_state` rows
  absent from its config (this broke rolling deploys and shared-database
  topologies); orphan rows are kept and logged.
- `TaskSchedule` positional `args` are rejected at scheduler startup
  (they always failed at enqueue; now they fail fast).
- Spec validation rejects `join='any'`/`'quorum'` nodes whose
  `args_from` targets a parameter without a default
  (`WORKFLOW_INVALID_JOIN`), and `@app.task` registration rejects Enums
  with non-JSON-native member values.

### Fixed

- Worker finalize/retry SQL now requires claim ownership: a stale
  finalizer (its task reaper-requeued and re-claimed by another worker)
  could overwrite the new owner's in-flight attempt, corrupt the attempt
  history, or trigger a third execution.
- Workflow terminal marks require `status='RUNNING'`: a task left
  running through `cancel()` could flip a CANCELLED workflow to
  COMPLETED/FAILED on completion and cascade the resurrection into
  parent workflows.
- `resume_workflow` / `cascade_resume_to_children` decode stored
  dependency results with the app registry again — resumed `args_from`
  consumers received `RESULT_DESERIALIZATION_ERROR` sentinels instead of
  the real upstream results.
- `stop()` shuts the executor down before draining finalizers: a task
  finishing after the drain timeout had its completed result discarded
  and was recorded (and possibly re-executed) as `WORKER_CRASHED`.
- Subworkflow completion takes the parent workflow lock before promoting
  dependents, closing a fan-in race that left nodes PENDING with all
  dependencies terminal until the next reaper sweep.
- `TaskHandle.get()` no longer caches transient errors: one broker
  hiccup or a not-found racing the enqueue poisoned the handle
  permanently.
- Schedules no longer wedge on permanent enqueue errors (e.g.
  `PAYLOAD_MISMATCH` after a deploy changed kwargs): the doomed slot is
  skipped and the schedule keeps running.
- `_schedule_retry` reads the queue name from the already-locked row
  instead of a second pooled session (pool-starvation deadlock under
  mass failure), and finalize retryability is keyed on the recovery DB
  outcome instead of the child-future exception type.
- `import_file_path` rolls back `sys.modules` and its cache when module
  execution fails (a broken module was silently returned as success on
  the next import) and no longer registers basename aliases that shadow
  other importable modules.
- A deterministic result-encode failure during workflow completion now
  degrades to a FAILED node with a serialization-error envelope instead
  of looping phase-2 finalize retries forever.

### Performance

- Claim path: partial composite indexes for both eligibility arms plus
  a split-arm `CLAIM_SQL` — measured ~430× faster claim passes at a
  50k-row pending backlog. The pending arm walks its composite in
  `ORDER BY` order and stops at the limit; the expired arm carries two
  complementary partial indexes (expiry filter for the few-expired
  steady state, ordered composite for deep expired backlogs — measured
  30.7ms → 0.11ms at 50k expired rows) with the planner choosing per
  data distribution. The cluster-wide claim advisory lock is taken only when
  cluster/queue caps require serialized accounting, and its key is a
  fixed constant (DSN-derived keys silently split the lock between
  workers using different DSN spellings of the same database).
- Workers subscribe to their queue channels only: the global `task_new`
  channel woke every worker for every insert cluster-wide (thundering
  herd). The trigger still emits `task_new` for external observers.
- Notify triggers split into INSERT/UPDATE pairs gated by
  `WHEN (OLD.status IS DISTINCT FROM NEW.status)` — lease renewals no
  longer invoke plpgsql per row.
- Result waiters get payload-keyed dispatch on `task_done` (one shared
  LISTEN, per-task delivery) and the wait loop polls a slim status
  probe instead of the full row with TOASTed payload columns.
- Reaper passes are gated by a cluster-wide try-advisory-lock (one
  executing reaper per interval instead of one per worker), stale-claim
  requeue locks only genuinely stale rows, pending expiry runs in
  bounded SKIP LOCKED batches, and two per-iteration session-churn
  sites are gone.
- Six write-amplifying single-column indexes on `horsies_tasks` dropped
  (every lifecycle UPDATE wrote entries into all of them); dependency
  lookups use `@>` so the GIN index actually applies; heartbeat and
  worker-state timeseries PKs widened to BIGINT (int4 sequences
  exhausted in months at heartbeat rates).
- Listener notification connections enable TCP keepalives so silently
  dropped connections surface within ~60s instead of hanging the
  dispatcher.

### Changed

- Global workflow-recovery passes are capped at 200 rows per candidate
  query per pass; resume-scoped passes remain uncapped. Successive
  passes converge on large backlogs without one pass holding its
  session and transaction throughout.
- In-process task calls return the lax-coerced ok value (e.g. `Ok('5')`
  for a declared `int` returns `5`), matching what wire consumers
  decode.
- `WorkerConfig.__repr__` masks its DSN fields.
- New docs: datetime round-trip caveats, scheduler DST behavior,
  exception-mapper exact-class matching, and the database trust
  boundary.

## [0.1.6] - 2026-06-04

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

## [0.1.5] - 2026-06-02

### Added

- `ping_workers(min_responses=N)` / `ping_workers_async(min_responses=N)`:
  return as soon as `N` distinct workers reply instead of waiting the full
  `timeout_seconds`. `min_responses=1` is a fast fail-open liveness gate — a
  healthy fleet answers in milliseconds; only a degraded fleet pays the
  timeout. Removes the latency floor for high-frequency `/health` probes.
  Pongs are de-duplicated by `worker_id`.

## [0.1.4] - 2026-06-02

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
  `WorkerStateSnapshot`.
- New exports: `DatabasePing`, `WorkerPong`, `WorkerStateSnapshot`.
- New broker error codes: `DB_PING_FAILED`, `WORKER_PING_FAILED`.

### Removed

- `broker.get_worker_stats()` (untyped `list[dict]`, RUNNING-tasks only, missed
  idle workers). Use `app.list_worker_states_async()` — typed and inclusive of
  idle workers.

## [0.1.3] - 2026-05-31

0.1.3 adds `CronSchedule`, a typed 5-field cron-style schedule pattern. It brings
wall-clock alignment and minute-offset load staggering that `IntervalSchedule`
cannot express — without cron strings.

### Added

- `CronSchedule`: a typed 5-field cron-style schedule pattern (`minute`, `hour`,
  `month` term lists plus a `DaySelector`). Provides wall-clock alignment and
  minute-offset staggering that `IntervalSchedule` cannot express, with no cron
  strings. The day-of-month vs day-of-week ambiguity is explicit through
  `EitherDay` (OR) and `BothDays` (AND). New exports: `CronSchedule`, `Month`,
  `CronEvery`, `CronStep`, `CronValues`, `CronRange`, `CronEnumValues`,
  `CronEnumRange`, `CronEnumStep`, `DaySelector`, `EveryDay`, `ByMonthDay`,
  `ByWeekday`, `EitherDay`, `BothDays`.

## [0.1.2] - 2026-05-29

0.1.2 is a **breaking** release headlined by the strict-serde redesign: the wire
stops carrying class identity, and every task parameter and return type must
classify into a concrete declared shape. It also closes a large batch of
workflow, worker, scheduler, and broker correctness bugs found in the
2026-05-24 audit.

For mechanical, copy-paste upgrade steps, see the
[Migrating to 0.1.2](website/src/content/docs/migrations/migration-to-0-1-2.md)
guide.

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

[0.1.2]: https://github.com/suleymanozkeskin/horsies/compare/horsies-v0.1.0...HEAD
