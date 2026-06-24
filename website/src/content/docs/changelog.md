---
title: Changelog
summary: Notable changes per release. 0.2.7 collapses the worker claim critical section into one server-side statement (the `horsies_claim` function, schema v10) so the cap-serialization advisory lock is held only across a single statement plus commit, removing the client-stall-while-holding-lock freeze, and changes the equal-queue-priority tie-break from configured queue order to a priority-then-FIFO band; 0.2.6 binds subworkflow tasks to their queue's configured priority so a parameterized child no longer claims at the default priority 100 on a CUSTOM queue; 0.2.5 adds default-on TCP keepalives for remote/pooled Postgres and gives the workflow reaper a grace window so it stops racing in-flight finalizers; 0.2.4 adds per-child memory recycling (`--max-memory-per-child-mb`, off by default), fixes a latent count-recycle hang (CPython gh-115634), and gates worker log color to TTYs; 0.2.3 recycles worker child processes (`--max-tasks-per-child`, default 100) to bound memory and adds per-child `children_memory_mb` telemetry, schema v9; 0.2.2 enforces keyword-only task parameters and validates producer values before serializing, makes schedules kwargs-only with app.check validation, and self-heals orphaned workflow tasks; 0.2.1 stops a failed outputless subworkflow from wedging its parent and isolates workflow recovery per candidate; 0.2.0 halves the worker hot-path statement budget and fixes the reaper-breaker misclassification; 0.1.10 eliminates round trips across the workflow completion, promotion, and child-start hot paths; 0.1.9 batches workflow start and scopes the claim lock per queue; 0.1.8 brings the workflow-completion performance redesign, supervisor-contract fixes, and scheduler state self-healing.
related: [./monitoring/worker-health, ./migrations/migration-to-0-1-2, ./internals/serialization]
tags: [changelog, releases, breaking-changes, 0.2.7, 0.2.6, 0.2.5, 0.2.4, 0.2.3, 0.2.2, 0.2.1, 0.2.0, 0.1.10, 0.1.9, 0.1.8, 0.1.7, 0.1.6, 0.1.5, 0.1.4, 0.1.3, 0.1.2]
---

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
horsies is pre-1.0: breaking changes may land in minor or patch releases, and
there is no migration contract between pre-1.0 versions.

## Unreleased

## 0.2.7 — 2026-06-24

Collapses the worker claim critical section into a single server-side statement.
Each claim pass previously held the cap-serialization advisory lock across many
client round trips — one `pg_advisory_xact_lock` per capped queue, the cap-counts
query, per-queue claims, then `COMMIT` — so at pooled-connection RTT a
client-side stall (GC/CPU starvation, a hung pooled connection) while holding the
lock froze every claimer cluster-wide. The new `horsies_claim(...)` SQL function
(schema v10, applied automatically by the broker's advisory-locked schema init on
next startup) acquires the locks, computes the cap/budget accounting, and runs
the windowed claim in one statement, so the lock is held only across that
statement plus the commit, never across a client round trip. The claim pass now
issues one statement under the lock instead of 7–9; at ~33 ms RTT with 16
concurrent claimers the worst single advisory-lock wait drops from ~5 s to ~0.5 s
and throughput scales with worker count instead of flat-lining at the lock's
serialization ceiling. Cap enforcement is unchanged: the function never
over-claims (under `SKIP LOCKED` contention it may claim fewer rows per pass,
deferring them to the next pass). One behavior change: when two queues share the
same priority, the tie-break was previously the configured queue order; the claim
now pools the equal-priority band and orders by task priority, then enqueue time
(FIFO), so equal-importance tasks across such queues are claimed FIFO while an
explicit task/workflow-node priority still preempts within the band. Distinct
queue priorities are unaffected.

## 0.2.6 — 2026-06-23

Subworkflow tasks now persist at their queue's configured priority. A
parameterized sub-workflow whose `build_with()` returns a direct
`WorkflowSpec(...)` bypassed `app.workflow()` binding, so its `TaskNode`s reached
the engine with `priority=None`; the engine then defaulted them to a literal
`100`. On a CUSTOM queue (e.g. `scraping` at priority 30) the child task landed
on the right queue but at the wrong intra-queue claim order
(`ORDER BY priority ASC, enqueued_at ASC`). Queue and priority resolution is now
centralized in a single bind boundary (`resolve_node_queue_and_priority`) shared
by `app.workflow()`, the engine subworkflow child branch, and `check()`,
replacing three divergent `else 100` fallbacks. An explicit `node.priority` is
preserved; `None` inherits the queue priority. DEFAULT-mode queues are unchanged
(priority 100). An invalid child queue is now contained with
`WORKFLOW_ENQUEUE_FAILED` rather than guessing a default. No schema change
(still v9). Also rolls in dev and website dependency bumps via Dependabot.

## 0.2.5 — 2026-06-17

The workflow reaper no longer races healthy finalizers. Task finalization is two
phases (Phase 1 commits the task terminal; Phase 2 advances the workflow DAG),
and the reaper's Case 1.7 recovery fired the instant a task went terminal — so
under load (amplified by frequent child recycling) it "recovered" tasks whose
Phase 2 was merely in flight, adding up to one reaper interval of latency and
noisy `crashed worker` logs. Recovery now honours a grace window
(`RecoveryConfig.crashed_worker_recovery_grace_ms`, new, default 10s, independent
of the heartbeat-coupled thresholds): a task terminal within the window is left
for its in-flight finalizer; only genuinely-stuck tasks are recovered (a genuine
crash recovers after the grace plus one reaper sweep). Correctness was never at
risk (recovery replays the stored result idempotently; the task body never
re-runs) — this is a latency and log-noise fix.

Idle pooled broker connections reaped server-side (e.g. PlanetScale's PgBouncer
pooler, which drops idle connections within ~1–2h) surfaced as a mid-query
`OperationalError` on the next claim or heartbeat — `pool_pre_ping` and
`pool_recycle` are checkout-time guards and cannot catch a connection that dies
in-flight. New TCP keepalive fields on `PostgresConfig` (`tcp_keepalives`,
default on, with `tcp_keepalives_idle`/`interval`/`count`) keep idle sockets
warm at the socket layer. libpq enables keepalives by default but leaves the
idle interval at the OS default (often 7200s); Horsies sets it to 30s, applied
to the broker engine pool and each child-process pool. No configuration is
required for remote/pooled deployments.

A direct `WorkerConfig(pgbouncer_transaction_mode=True)` built without
`child_connect_kwargs` left child pools with prepared statements enabled against
a transaction-pooled PgBouncer. `WorkerConfig.__post_init__` now ensures
`prepare_threshold=None` in `child_connect_kwargs` when the flag is set (without
overriding an explicit value), so the flag alone is sufficient; the CLI path was
unaffected.

## 0.2.4 — 2026-06-17

Per-child memory recycling complements count-based `--max-tasks-per-child`: a
task count is a poor proxy for a bytes budget, so the correct recycle point
depends on a child's RSS. New `--max-memory-per-child-mb N` (off by default,
CPython-only, forces `spawn`) recycles a child once its own resident memory
reaches N MB — the child samples RSS after each task and exits cleanly via the
stdlib `exit_pid` marker, replacing only that child. A startup baseline guard
fails the worker if the threshold is at or below the warmed child baseline.
Also fixes a latent CPython gh-115634 hang in the existing count-recycle path:
count recycling now overrides `_adjust_process_count` so a recycled child is
always replaced, falling back to the stock pool if the internals are absent.

Worker logging no longer writes raw ANSI color escapes to non-TTY sinks (log
drains, container logs, journald, files), which broke grep and log parsers.
Color is gated by stream `isatty()`, with `NO_COLOR` / `FORCE_COLOR` overrides;
non-TTY output is plain text with the same layout. Task execution log lines also
stop wrapping the id/name in one-element lists; both the start and completion
lines now read `task_name (task_id)`.

## 0.2.3 — 2026-06-16

Worker child processes can now be recycled to bound memory. Long-lived
executor children accumulate memory the OS never reclaims (allocator
high-water from heap fragmentation, C-extension caches, leaks), which crashes
memory-quota platforms (containers, PaaS dynos). `--max-tasks-per-child` (default `100`)
recycles each child after N tasks; new `children_memory_mb` telemetry exposes
the per-child footprint the parent-only `memory_usage_mb` metric hid. Schema
bumped to v9 (additive). **Behavior change:** recycling is on by default and
forces the `spawn` start method (incompatible with `fork`); set
`--max-tasks-per-child=0` to keep `fork`/no recycling.

### Added

- `--max-tasks-per-child N` worker flag (`WorkerConfig.max_tasks_per_child`,
  `N >= 2`, **default `100`**): recycle each worker child process after N tasks
  (per-child and staggered) to bound memory for workloads that retain it
  (allocator high-water, C-extension caches, leaks). `0` disables recycling.
  **Behavior change:** because the stdlib budget is incompatible with `fork`,
  any non-zero value (including the default) forces the `spawn` start method —
  on Linux this replaces `fork`, so children re-import the app instead of
  fork-cloning the parent (higher baseline RSS, slower child startup). Set
  `--max-tasks-per-child=0` to keep `fork`/no recycling.
- `children_memory_mb` column on `horsies_worker_states` and field on
  `WorkerStateSnapshot`: summed RSS of the executor child processes. The
  existing `memory_usage_mb` is the parent process only; per-child memory
  growth (the memory-quota driver) was previously invisible. Schema bumped to
  v9 (additive, idempotent `ADD COLUMN`).

## 0.2.2 — 2026-06-15

Producer-side strictness lands on both axes: task parameters must be
keyword-only, and `encode_value` validates a value against its declared type
before serializing — so a positional or mistyped task call fails at the type
checker or at `app.check` instead of returning an ignorable `Err` that silently
drops the send. Schedules become kwargs-only and `app.check()` validates them
as a preflight. Orphaned workflow tasks self-heal instead of churning the
requeue loop. No schema change (still v8).

### Fixed

- Orphaned workflow tasks (a `CLAIMED` workflow task whose `workflow_task`
  linkage is missing or terminal) are no longer requeued and re-dispatched
  forever. They are cancelled — at finalization when detected, and by a reaper
  self-heal step — which frees in-flight budget and lets retention sweep them.
- Retention no longer orphans a live task row: a terminal, expired workflow is
  retained until every backing task is terminal, instead of deleting the
  workflow/`workflow_task` rows while a backing task is still non-terminal.
- `encode_value` validates a value against its declared type before serializing.
  `dump_python` alone only serialized — a mistyped value (a `dict`/`int`/`list`
  in a `str` slot) passed through with a warning. The producer now fails closed,
  symmetric with `decode_value` on the consumer; mistyped task kwargs, results,
  and `args_from` bindings are rejected at send / at `app.check`. (#146)

### Added

- `RecoveryConfig.auto_terminate_orphaned_workflow_tasks` (default `True`):
  cancel orphaned workflow tasks at finalization and in the reaper. When
  `False` they are left `CLAIMED` for inspection (never requeued or
  retention-deleted).
- `app.check()` validates configured schedules (timezone, task registration,
  queue, and the kwargs wire contract) as a preflight phase, shared with
  scheduler boot. Skipped for the worker role, which never enqueues schedules.
  Previously a malformed schedule passed `check` and only failed at scheduler
  startup.

### Changed

- Task parameters must be keyword-only. `check_task_signature` now rejects
  `POSITIONAL_OR_KEYWORD` params, so a task is declared `def f(*, x: T)`.
  `ParamSpec` then carries the keyword-only-ness, turning a positional call
  (`f.send(42)`) into a call-site type error instead of a runtime
  `Err(VALIDATION_FAILED)` that an unchecked caller silently drops. The
  producer-side runtime guard stays as defense-in-depth. **Breaking**: add a
  bare `*,` before the first parameter of every task definition. Execution is
  unaffected (the worker already passes every argument by keyword).

### Removed

- `TaskSchedule.args` removed; schedules are kwargs-only (strict-serde has no
  positional wire form). `TaskSchedule` now sets `extra='forbid'`, so `args`
  or any unknown field raises a validation error at construction instead of
  being silently dropped. **Breaking**: pass every scheduled argument as a
  `kwargs` entry.

## 0.2.1 — 2026-06-14

A failed outputless child workflow used through a `SubWorkflowNode` leaked the
internal terminal-results envelope into the parent node, raising a strict-serde
reserved-key error that wedged the parent and, via the recovery loop, starved
recovery for every stuck workflow. The propagation path and recovery isolation
are both fixed. No schema change (still v8).

### Fixed

- Outputless child workflows no longer leak the internal terminal-results
  envelope into parent `SubWorkflowNode` results or `SubWorkflowSummary.output`.
  Completed outputless children propagate as `TaskResult[None, TaskError]`
  with `ok=None`.
- Workflow recovery now isolates candidate failures so one poison workflow row
  cannot abort the whole recovery pass.

### Changed

- `WorkflowDefinition[T]` with no `Meta.output` is now rejected for concrete
  `T`. Use `WorkflowDefinition[None]` for outputless orchestration workflows,
  or set `Meta.output` to a node producing the declared type.

## 0.2.0 — 2026-06-12

Worker hot-path statement budget halved (27.2 -> 12.8 statements per task
measured at 33ms RTT): optional per-checkout health checks, child pre-exec
collapsed to one transaction, and plain-task ok-path finalization fused into
a single statement. Remote soft-cap throughput 2.35x; local 1.15-1.36x.
Fixes a reaper-breaker misclassification that could strand CLAIMED tasks
after connection-slot exhaustion. No schema change (still v8).

### Performance

- Child pre-exec runs one transaction instead of three: the redundant
  pre-flight expire/workflow check is deleted (the RUNNING transition's
  guards and miss-path diagnosis already enforce it atomically) and the
  first runner heartbeat rides the RUNNING transaction. (#134)
- Plain-task ok-path finalization is one statement
  (`FINALIZE_TASK_COMPLETED_SQL`): lock, attempt upsert, COMPLETED CAS,
  and capacity notify in a single transaction; phase 2 is skipped for
  this path. Err results, workflow tasks, and decode failures keep the
  multi-statement flow. (#134)
- The child result payload is decoded before any SQL; `WORKFLOW_STOPPED`
  results finalize without opening a session. (#134)

### Added

- `worker_child_pool_check` (`PostgresConfig`, default `true`): disable
  the per-checkout health check on child pools; pairs with
  `pool_pre_ping=false` for high-RTT deployments (~20% of the per-task
  statement budget at 30ms+ RTT). (#134)
- Remote PostgreSQL deployment guide: connection-budget formula, pooled
  multi-worker setup, prefetch at high RTT, health-check trade-offs.
  (#136)

### Fixed

- `sqlalchemy.exc.TimeoutError` (engine pool checkout timeout) now
  classifies as retryable. Previously three consecutive reaper passes
  during connection-slot exhaustion latched the stale-CLAIMED requeue
  breaker off for the process lifetime, leaving orphaned CLAIMED tasks
  without their designed backstop after the pressure cleared. The
  mark-failed breaker had the same exposure. (#135)

### Changed

- The first runner heartbeat commits atomically with the RUNNING
  transition: a task row is never observable as RUNNING without
  heartbeat coverage. The heartbeat thread no longer sends an immediate
  beat. (#134)
- Capacity notify for plain ok-path tasks fires on the finalize commit
  instead of a separate phase-2 transaction. (#134)

### Tests

- Reaper breaker state machine pinned: counter resets on transient
  failures and successes, 3-failure latch, latched operations skipped,
  breaker independence, pool-timeout-counts-as-transient end to end.
  (#137)

## 0.1.10 — 2026-06-12

Round-trip elimination across every workflow hot path (completion,
dependent promotion, subworkflow child start) and claim-pass cap
accounting; the per-workflow completion ceiling roughly 2.4x at
remote-database RTTs. No schema change (still v8).

### Performance

- Claim-pass cap accounting is one statement. The worker-local claimed
  and in-flight counts, the optional cluster-wide count, and every capped
  queue's hard/soft count arrive in a single FILTER-aggregate read
  (`CLAIM_PASS_COUNTS_SQL`) instead of 2 + Q (+1) sequential statements
  under the claim advisory locks: an empty pass over Q=3 capped queues
  drops from 11 statements to 7. Count predicates are unchanged (verified
  column-for-column against the single-purpose statements, which remain
  for health snapshots); all counts now share one now() instant instead
  of one per statement.
- Subworkflow child start is bulk-inserted. Starting a SubWorkflowNode's
  child workflow now writes all child node rows in one executemany over
  the same bulk statement the batched workflow start uses, and child
  TaskNode roots are inserted directly as ENQUEUED with their task rows
  in a second executemany (child roots cannot carry
  `args_from`/`workflow_ctx_from` — spec validation requires both to
  reference `waits_for`). The parent workflow's name rides the enqueue
  CAS RETURNING instead of a separate read. Statements: 4 + C + 3R ->
  5 flat; measured against a remote (~33-45ms RTT) Postgres, a 50-child
  flat child start dropped from ~9.3s to ~0.3s. SubWorkflowNode roots
  still recurse; a root whose task_options fail to parse demotes to the
  per-node path so corruption keeps failing that child root, not the
  parent.
  A slow root's failure that pauses the workflow (its `on_error=PAUSE`)
  now gates the fast roots: their task rows are inserted only if the
  workflow is still RUNNING after slow roots ran and any queued
  child-to-parent propagation was drained; otherwise they revert to READY
  with task_id cleared — a paused workflow gains no runnable task rows
  (the same strengthened pause contract as batched promotion). This also
  closes the identical pre-existing corner in the batched workflow START
  (shipped in 0.1.9), where fast-root task rows landed before the
  slow-root loop and before synchronously failed child propagation.
- Dependent promotion is batched per skip-cascade level. Completing a
  task that unblocks F plain-TaskNode dependents (args_from included)
  now runs a fixed pipeline — one grouped config+dependency-status
  evaluation, batched PENDING->READY / ->SKIPPED CAS writes, one grouped
  dependency-results read, one batched READY->ENQUEUED CAS returning the
  insert payloads, one bulk task INSERT and one bulk LINK — instead of 7
  statements per dependent under the workflow lock. Statements are flat
  in F (8 vs 6+7F+2); measured against a remote (~33-45ms RTT) Postgres,
  a 1-root -> 119-dependent promotion dropped from ~34s to ~0.6s of
  lock-held time. SubWorkflowNode and `workflow_ctx_from` dependents
  keep the per-node path. Join semantics, skip cascades, pause/cancel
  guards, per-row CAS, and per-node failure isolation are unchanged; the
  payload builder is shared with the per-node path so they cannot
  diverge.
  One strengthening vs the sequential path: under `on_error=PAUSE`, a
  payload-build failure in a promotion level (a horsies bug path —
  corrupt persisted rows) pauses the workflow, stops the level, and
  reverts the level's already-CAS'd-but-uninserted siblings to READY —
  a paused workflow gains no new runnable task rows, and the siblings
  sit in the recovery-covered shape (resume re-enqueues READY nodes).
  The sequential loop's post-pause state depended on processing order
  (siblings processed before the failure were already enqueued). Pinned
  by test_pause_policy_build_failure_reverts_siblings_to_ready.
- Task completion runs in half the round trips. The completion path's
  locate -> lock -> CAS-update triple is one statement
  (`COMPLETE_WORKFLOW_TASK_SQL`: locate the node by backing task id, take
  the workflow row's FOR UPDATE lock, CAS to terminal status, return the
  progression context), the post-update status/depth reads are gone (the
  held lock freezes the workflow row, so the locked row's values are
  authoritative), the completion check no longer re-acquires the
  already-held lock, and the failure path no longer re-acquires it a
  third time or re-reads `on_error`. Per success completion with one
  pending dependent: 10 statements -> 5; failed completion
  (on_error=FAIL): 14 -> 7. Measured against a remote (~33-45ms RTT)
  Postgres: 491ms -> 328ms per completion (failure path 654ms -> 335ms).
  Completions of the same workflow serialize on the workflow row lock,
  so the shorter lock-held window raises the per-workflow completion
  ceiling from ~2.3-3/s to ~3.2-4.3/s.

### Changed

- `on_workflow_task_complete` requires a keyword-only `task_name`
  (callers read it from the task row they already hold); the result
  envelope is encoded before the first statement. The worker threads
  `task_name` through dispatch and both finalize retry stages; the
  phase2 pre-flight workflow-task existence check is removed (the merged
  statement self-detects non-workflow tasks at the same cost).
- The completion-encode failure fallback (a horsies bug path: the result
  envelope cannot be encoded) now stores
  `TaskError.data={'task_id','task_name'}` instead of
  `{'workflow_id','task_index'}` — the workflow context is not known
  before the merged statement runs.

## 0.1.9 — 2026-06-11

Workflow-start batching and per-queue claim-lock scoping: round-trip
elimination on the two hot multi-statement paths.

### Performance

- Workflow start is batched: node rows and fast-path root tasks (plain
  TaskNodes without `args_from`/`workflow_ctx_from`) are built in memory
  and inserted in a fixed handful of pipelined statements instead of one
  INSERT per node plus three statements per root. Measured against a
  remote (~33ms RTT) Postgres: starting a 119-root workflow dropped from
  ~16s to ~0.35s; statement count is flat in workflow size. Subworkflow
  roots and `args_from`/`ctx_from` roots keep the per-row path. Start
  semantics are unchanged: `Ok(handle)` still means durably persisted,
  one transaction, idempotent restart by workflow_id, whole-start
  rollback on failure.

### Changed

- The claim advisory lock is scoped per capped queue (cluster_wide_cap
  keeps the single global key): workers claiming disjoint capped queues
  no longer serialize against each other. During a rolling deploy, old
  and new workers do not contend with each other, so a per-queue cap can
  briefly overshoot by up to one pass's batch until the fleet is on one
  version.

## 0.1.8 — 2026-06-11

Workflow-completion performance redesign, supervisor-contract fixes,
scheduler state self-healing, and the close of the raise-contract
documentation track. Schema migrates v7 → v8 automatically on first
broker start.

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

### Breaking

- `TaskSchedule.timezone` is validated at scheduler startup: an invalid
  IANA name now exits 1 at boot (`CONFIG_INVALID_SCHEDULE`) instead of
  leaving the schedule silently dormant with a per-tick init failure.

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

## 0.1.7 — 2026-06-10

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
