# Changelog

All notable changes to **horsies** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The project is pre-1.0: breaking changes may land in minor or patch releases,
and there is no migration contract between pre-1.0 versions.

## [Unreleased]

### Added

- **Per-queue retention: `AppConfig.retention.queue_retention`.** Maps a
  queue name to how long a task sent on it keeps its history record, or to
  `None` to keep it forever. A queue with no entry is unchanged and takes
  the immutable 30-day class. The duration is part of the class the mapping
  derives (`emails` at seven days is `q_emails_7d`), so remapping mints a new
  class rather than redefining one and records enqueued under the previous
  mapping age out under the promise they were given. `q_` is reserved: a
  `retention_classes` key may not use the prefix. The class is resolved at
  the send and snapshotted on the row, so a retry replays the class its
  original send chose. Precedence is the explicit `retention_class_key`
  (including an explicit `standard_30d`), then the queue's mapping, then
  `standard_30d` — omitting the argument now means "the queue's mapping if
  there is one", which is the behaviour change for anyone who maps a queue.
  The direct successor to 0.4.x's
  `RecoveryConfig.queue_terminal_record_retention_hours`.

### Fixed

- **Partition detach and finalize always carry a statement timeout.** The
  heartbeat sweep's detach and the interrupted-detach finalize both took an
  unbounded wait: `statement_timeout_ms` defaulted to `None`, so a call site
  that omitted it waited on a conflicting lock with no bound while holding the
  cluster-wide maintenance gate. A blocked `DETACH PARTITION ... FINALIZE` is
  the worse of the two — PostgreSQL refuses every further detach on a parent
  while one is pending finalization, so the stall propagates to that parent's
  whole retention sweep. The finalize command gained the field, the executor
  applies and restores it, and the default is gone from both commands: `None`
  still means unbounded, but only where a call site writes it.

- **A leaf the catalog names and the database no longer has stops failing
  every finalize.** The staged history readers are generated from the leaf
  catalog, and PostgreSQL does not validate PL/pgSQL bodies at `CREATE`, so a
  reader naming a relation dropped out of band built successfully and failed at
  execution — inside the provenance probe on the terminalization path, which
  made one missing leaf fail every finalize in every queue and every retention
  class. The probe list is now filtered by relation existence when the manifest
  is built, so republication is self-correcting, and the maintenance pass
  republishes when the published readers name a relation that no longer
  resolves rather than only when a leaf was created. The vanished leaf is named
  on the coverage health surface and logged at warning level. The catalog row
  is left untouched: a leaf that disappeared behind the manager's back is an
  operator's decision, and stamping it dropped would erase the evidence. The
  history that leaf held is unreadable. A leaf inside the coverage horizon
  cannot be recreated while its catalog row survives, so that class gains no
  new partitions until an operator resolves the conflict; other classes are
  unaffected.

- **A destroyed leaf's tasks are no longer reported as too old to keep.**
  `predates_retained_floor`, which reaches the rerun surface, is derived from
  the attached leaf catalog rather than from the published probe list. The two
  differ by exactly the leaves whose relation was dropped out of band, and
  those leaves did retain history until someone removed it, so answering from
  the probe list would present an accidental drop as ordinary ageing.

- **Retention configuration errors carry their own error code.** Every refusal
  from `AppConfig.retention` — an unusable queue name or class key, a
  non-positive or fractional duration, a reserved or duplicated class key, a
  key too long for the relation names it is spliced into, and a `queue_retention`
  mapping naming a queue the deployment does not have — now reports
  `CONFIG_INVALID_RETENTION` (`HRS-216`). These answered with
  `CONFIG_INVALID_RECOVERY` (`HRS-204`), left over from when the fields lived on
  `RecoveryConfig`, and the queue cross-check answered with
  `CONFIG_INVALID_QUEUE_MODE` (`HRS-200`). Setting a moved field on
  `RecoveryConfig` still reports `CONFIG_INVALID_RECOVERY`: that refusal belongs
  to the object that no longer has the field.


- **Per-queue retention now applies on every enqueue path.** `.schedule()`,
  `.schedule_async()`, a `TaskSchedule` firing on its cron, and a workflow
  node's backing task resolve the queue mapping exactly as `.send()` does,
  with the class chosen at enqueue. Previously only the immediate sends
  resolved it, so the same task landed a different retention class
  depending on how it was enqueued, and nothing reported the divergence.
  A cron fire resolves from the scheduler process's own configuration; a
  workflow node from the queue its node runs on.

- **An explicit retention choice on a delayed send is honored.**
  `with_options(retention_class_key=None).schedule(...)` asked for the
  record to be kept forever and the record was deleted at 30 days; a named
  class was discarded the same way. The delayed methods never carried the
  option. `idempotency_key` rode the same gap and is carried with it.

- **Worker startup registers queue-derived classes.** Startup registered
  declared classes only, so a task sent on a mapped queue before any
  maintenance pass had no partition to move into at terminalization. The
  window is not one tick: the periodic pass sits behind the cluster-wide
  maintenance gate, which can deny.

- **A failing retention class no longer stops heartbeat coverage or the
  classes after it.** Containment previously covered raised exceptions in
  history-class leaf coverage and nothing else. A *returned* refusal still
  ended the pass, and every queue-derived key sorts before `standard_30d`,
  so the feature made the adverse ordering systematic rather than
  incidental. Heartbeat coverage ran after the failure report and so was
  skipped entirely — and heartbeat leaves gate worker startup, so one
  poisoned class row could leave a restarted fleet unable to come back.
  Each class now runs inside a savepoint, so a database-level error rolls
  back to that savepoint instead of poisoning the caller's transaction and
  taking the pass down with it.

- **A queue named in `queue_retention` must be a queue the deployment
  has.** A misspelled queue was accepted, minted a class, and grew leaves
  and indexes on every pass forever while nothing routed into it — and the
  queue the adopter meant kept the 30-day default. Checked in both queue
  modes, refused at construction naming the configured queues.

- `RetentionChoice` is exported from `horsies`. It appears in the
  `with_options` signature, so it was a type a reader could see and not
  import.

- **A retention class key longer than 31 characters could stop partition
  coverage across a deployment.** Configuration accepted any key up to
  PostgreSQL's 63-byte identifier limit, but the key is interpolated into
  the relations the class owns and the longest of those adds 45 bytes, so
  the class registered and every later maintenance pass then raised on its
  leaf name. The pass reads registered classes from the database rather than
  from configuration, so removing the declaration did not recover it. Keys
  are now bounded at 18 characters — the point past which a derived name
  exceeds the limit — and refused at configuration with the arithmetic. Two
  shorter bands also break and are inside the bound: from 19 the index name
  is silently truncated, and at 30 and 31 both of a leaf's index names
  truncate to identical bytes and the second `CREATE INDEX` fails on a
  duplicate.

- **A failing retention class no longer denies coverage to the others.**
  Classes are served in key order and a failure inside one escaped the loop,
  so every class sorting after it stopped receiving partitions. Each class is
  now bounded on its own: the failure is recorded against its key, the
  remaining classes are served, and the pass reports a refusal naming every
  class that failed. This also restores the health signal — the refusal is a
  value the caller matches on, where a raised failure skipped that match and
  left coverage health reporting whatever the previous pass had put there.

### Changed

- **Retention configuration moved to `AppConfig.retention`.** Nine fields
  leave `RecoveryConfig` for a new `RetentionConfig`: `retention_classes`,
  `terminal_record_retention_hours`, `worker_state_retention_hours`,
  `retention_sweep_interval_s`, `retention_delete_batch_size`,
  `history_leaf_horizon_days`, `heartbeat_leaf_horizon_hours`,
  `partition_maintenance_interval_s`, and
  `paused_workflow_auto_cancel_after`.

  The two answer different questions. Recovery is about work that went
  wrong — stale claims, dead runners, the thresholds for noticing.
  Retention is about work that went right and is now history: how long
  its record survives, how partitions are kept ahead of writes, and how
  they are reclaimed. `paused_workflow_auto_cancel_after` moves with them
  because expiring a workflow that has sat paused past a declared age is
  a policy about how long a record stays actionable, not a response to a
  crash.

  Setting a moved field on `RecoveryConfig` fails at construction and
  names its new home; every misplaced field in a config is reported at
  once. There is no alias and no deprecation shim, per the pre-1.0
  posture.

  `RetentionConfig` and `RetentionClassConfig` export from `horsies`
  directly. No schema change, and the worker-state monitoring payload
  keeps its existing keys.

## [0.5.1] - 2026-08-10

### Added

- **Declared retention classes.** A deployment may declare additional
  finite retention classes in `RecoveryConfig.retention_classes`, each a
  key and a duration. The maintenance owner registers every declared
  class at startup and on each maintenance pass, exactly as it registers
  the classes the library ships, so partition DDL stays out of adopter
  hands and registration keeps a single owner. Tasks are then sent into a
  declared class with `with_options(retention_class_key=...)`.

  Declarations are validated where they are written: a key must be a
  usable identifier, must not be one the library owns (`standard_30d`,
  `forever`, `heartbeats`), must not repeat, and its duration must be
  positive. Every problem in a config is reported together rather than
  one per run. Re-declaring an existing key with a different duration is
  refused by the registration machinery and named at startup — classes
  are immutable.

  `duration` is a minimum, not an exact age: history leaves span one day
  and a leaf is dropped only once its whole day is past the duration, so
  a row survives between `duration` and `duration + 1 day`. Sub-day
  durations are accepted and never under-retain, but cannot expire faster
  than daily partition granularity allows.

### Changed

- Send-time retention validation widens from the two shipped classes to
  the set this deployment declares. The check stays process-static and
  costs no database round trip, which makes the contract per-process:
  a process whose config omits a class refuses it even if another
  deployment registered that class in the same database. The rerun
  path's registry lookup is unchanged.

## [0.5.0] - 2026-08-10

Task-history live/history split. Schema v34. **This release requires an
offline cutover for existing deployments**: stop all units, upgrade the
package, run migrations, run the cutover program, start units. Fresh
installs are born at the cutover's end state; the history subsystem is
dormant until invoked and adds no cost to deployments before cutover.

### Changed

- **Workflow retention removes unconsumed phase-2 evidence with the
  workflow.** A terminal task hands its workflow node's progression to
  an outbox, and deleting the workflow deletes any evidence still
  waiting there — the same disposition the consumer would have reached
  for a workflow that is already terminal. Retention is not held up by
  a stalled consumer.
- **Enqueue-time facts are required columns on a fresh install.** The
  retention class, command fingerprint, rerun-input flag and prepared
  disposition are added nullable so that an upgraded deployment's
  running fleet cannot violate them before the cutover backfills; the
  cutover's tighten stage makes them required. A database created by
  this release has no such fleet and never runs that stage, so it
  applies the same tightening at creation: the columns are required and
  their declared checks present from the first row. Upgraded
  deployments are unchanged — the columns tighten at the cutover, where
  they always did. The cutover now refuses to tighten while any live row
  carries no retention class, naming the count.
- **Terminal task records move to a partitioned history archive at
  terminalization.** The live table holds only live rows (`PENDING`,
  `CLAIMED`, `RUNNING`); completed, failed, cancelled, and expired
  tasks live in `horsies_task_history`, partitioned by retention class
  and day. Task and workflow identity columns are `uuid`; task ids are
  UUIDv7. Retention works by dropping whole partitions instead of
  deleting rows. The default retention class keeps terminal records
  30 days; passing `retention_class_key=None` at enqueue keeps a
  record forever.
- **Crashed-worker workflow recovery flows through the terminalization
  outbox.** A worker that dies between recording a task's terminal state
  and advancing its workflow node used to be found by scanning the live
  table for terminal tasks; terminalization now records the owed
  progression as it moves the task, and the reaper consumes those
  records. Same outcome, different mechanism, and the same grace window
  and per-pass bound as before.
- **Manual retry of a terminal task is removed.** A terminal record is
  immutable; re-execution is a new request through the rerun API, with
  a new task id and recorded lineage. The dashboard's retry action is
  replaced by rerun.
- **Tasks cancelled by a workflow-level cancellation carry no error
  summary of their own.** The terminal record's `error_code` and
  `failed_reason` describe the terminalization itself; requeue residue
  from earlier attempts stays in the attempt history instead of being
  frozen beside a status it does not describe.
- **Attempt rows purge with their task's history record.** At
  terminalization the attempts are archived into the history record's
  snapshot and the live attempt rows are deleted; the snapshot is the
  attempt history's only home from then on, and it ages with the
  record's retention class.
- **The dashboard's task totals are live rows plus a bounded terminal
  window.** Monitoring reads default to the last 24 hours of terminal
  history (`since`/`until` accepted, 30-day maximum); a request over
  the maximum is refused with the bound named, never silently clamped.
- **Monitoring history reads ride a per-leaf enqueue-order index.**
  Every task-history leaf carries a btree on `enqueued_at`
  (schema v34); the dashboard list's default sort merge-appends leaves
  in index order and stops at its LIMIT instead of sorting every
  matched row per call. The migration builds the index on each
  existing leaf at upgrade. The unfiltered total's history side
  becomes the planner estimate the surface already documents; totals
  under any active filter stay exact.
- **Pausing a workflow relocates its claimed tasks' terminal
  records.** Pause abandons a claimed internal task row, and the
  abandoned row terminalizes to history as `CANCELLED` with a message
  naming the pause; resume enqueues fresh rows. A node still pending
  moves nothing, and the paused workflow itself is unchanged — only
  its abandoned claimed tasks gain history records.

### Added

- **`WorkflowStatus.EXPIRED` and `paused_workflow_auto_cancel_after`.**
  A workflow paused longer than the configured age is expired by
  policy: `CANCELLED` continues to mean someone decided, `EXPIRED`
  means time ran out. The policy is opt-in (default `None`); the
  workflow's `error` column records the policy name and configured
  age. An expired child propagates to its parent exactly as a
  cancelled one. Exhaustive matches over `WorkflowStatus` gain a
  member.
- Rerun and idempotency API: re-execute a terminal task by reference
  with recorded lineage; scoped idempotency keys with a reservation
  window.
- Workers own partition coverage and pruning: heartbeat and history
  partitions are created ahead of writes at worker startup and by a
  periodic maintenance pass, and the same pass detaches and drops
  leaves whose retention class duration has elapsed — a refusal (a
  partition pinned by recovery evidence, a reader outliving the 5 s
  detach timeout) skips that leaf, surfaces with its reason in the
  `partition_pruning` health payload, and retries next pass. The
  worker role needs `CREATE` on the partition parents; a deployment
  that withholds it must run an external coverage cron.
  `run_schema_migrations` continues to govern versioned migrations
  only.
- **`retention_class_key` at enqueue.** `with_options` and both send
  paths accept it: omitted lands the immutable 30-day default class,
  explicit `None` is the only route to a forever record, and an
  unknown class name is refused at the send statement with nothing
  written.
- **Unresolvable crashed-worker recovery evidence quarantines after a
  bounded attempt count.** A phase-2 pending row whose disposition
  keeps refusing to resolve is retried for
  `phase2_quarantine_after_attempts` recovery passes (default 25,
  bounds 3–1,000), then moved to a quarantine table with its evidence
  preserved; discovery stops retrying it. Quarantined counts, rows
  over the attempt bound, and the quarantine function's refusals join
  the worker health surface beside the phase-2 pass summary.
- **`rerun_task` is importable from `horsies` directly**, with its
  command, policy, exhaustive outcome union, and `NotEligibleReason`.

### Removed

- `queue_terminal_record_retention_hours` and
  `heartbeat_retention_hours`: terminal task records age by retention
  class and heartbeats drop with their partitions. Setting either now
  fails validation naming the successor.
  `terminal_record_retention_hours` narrows to workflow records only.

### Known incompatibilities

- `syce` is not compatible with this release; compatibility returns in
  a post-0.5.0 release.


## [0.4.7] - 2026-08-05

Terminal failure-summary cleanup. Schema v26.

> Every worker calls the shared terminalization functions, so the corrected
> summaries take effect fleet-wide as soon as the first upgraded process
> migrates the schema to v26. They do not roll out worker by worker.

### Fixed

- Terminal writers own the complete final-attempt summary. Automatic retry
  clears `error_code` when it requeues a task but never `failed_reason`, so a
  task that failed, was retried, and then reached a different terminal outcome
  carried the earlier attempt's reason beside that outcome: a completed task
  could show a stale failure reason, and an expired task kept one beside
  `TASK_EXPIRED`. Locked failure now assigns its reason unconditionally — a
  NULL reason clears the column instead of preserving the old value — and the
  completion and expiry operations clear `failed_reason` at the transition.
  Per-attempt reasons remain in `horsies_task_attempts`.

### Documentation

- Corrected the retention description for workflow-backing task rows: they use
  the global window and are protected while their workflow is non-terminal;
  once the workflow is terminal, each row ages from its own terminal
  timestamp. The previous wording claimed a workflow and its task rows are
  retained as a unit.

## [0.4.6] - 2026-08-05

Task terminalization consolidation. Schema v25.

> **Rolling-upgrade prerequisite:** complete the 0.4.5 rollout on every worker
> in a deployment before installing 0.4.6. This release adds a constraint that
> requires terminal tasks to carry a terminal timestamp. The first upgraded
> process enforces it on new writes as soon as it migrates the shared schema;
> workers older than 0.4.5 do not set that timestamp, so their terminal
> transitions would fail. This ordering is per deployment, not a calendar
> delay. A full-stop deployment may upgrade directly from an earlier version
> because no older worker remains active when the migration begins.

### Added

- `horsies_tasks.terminalization_kind` records which operation ended each
  task. Replays are recognized only when the committed operation is equivalent
  to the requested transition; a terminal row produced by another operation is
  reported as a conflict rather than inferred from its status alone.

### Changed

- Terminal task transitions are owned by fifteen database functions behind one
  typed boundary. Callers receive explicit applied, replay, lost-claim,
  source-conflict, or absent outcomes instead of inferring them from row counts.
  Refusal diagnostics carry the locked values used to evaluate claim,
  staleness, deadline, and workflow-link guards.
- The schema program advances from v17 to v25 in one startup migration,
  including a one-time backfill of `terminal_at` for existing terminal rows
  before the terminal-status/timestamp constraint is validated. Installations
  may upgrade directly from v17 through every intermediate schema step.
- The measured completion, failure, workflow-success, and expiry paths remained
  within their declared statement-count, transaction, latency, and WAL budgets;
  the evidence and conditions are committed under `tests/perf/results`. The
  orphan, workflow-scoped, child-stop, and administrative-cancellation paths
  ship under correctness-only verification; their measurements remain
  available through the manual performance workflow.

### Fixed

- Sub-second task-staleness thresholds are honored at millisecond precision
  instead of being truncated to whole seconds.

## [0.4.5] - 2026-08-03

Task lifecycle release. Schema v17 (additive).

**0.4.6 adds a CHECK constraint tying `terminal_at` to terminal status.
Complete your 0.4.5 rollout on every worker before deploying 0.4.6.** The
constraint rejects terminal writes that omit `terminal_at`, which is what a
pre-0.4.5 worker produces; a deployment still running mixed versions would
fail those workers' finalize statements. The requirement is per deployment,
not a release gate — 0.4.6 ships when ready, and each deployment applies it
once its own 0.4.5 rollout is complete. A full-stop upgrade (all workers
stopped) may move to 0.4.6 from any version directly; the rollout requirement
applies to rolling restarts, where pre-0.4.5 workers would keep writing
terminal rows during the migration.

### Changed

- Workflow pause and cancellation no longer terminalize a task whose claim
  has been replaced. A task whose lease lapsed, was requeued by the reaper,
  and was re-claimed carries a new claim generation; the abandon or cancel
  now applies only to the generation it was issued against. Batch statements
  fence per task, so one batch spanning several claim transactions rejects
  only the entries whose generation moved. The child pre-start branches fence
  to their own claim. A requeued `PENDING` row under a cancelled workflow
  carries no claim to fence and is still cancelled.

### Added

- `horsies_tasks.terminal_at TIMESTAMPTZ` (schema v17): the canonical instant
  a task became terminal, assigned from database time in the same transaction
  as the terminal transition and cleared by manual in-place retry. Every
  terminal transition from this release forward records it. Eight of the
  terminal paths previously recorded no end timestamp at all, leaving terminal
  age to fall through to `updated_at`. Nothing reads the column yet;
  `completed_at` and `failed_at` are unchanged.

## [0.4.4] - 2026-08-02

Workflow retention performance release. No schema change. At 40k workflows
(35k eligible, 1-2 nodes each) / 460k tasks, a full backlog drain drops
from 12.0s across 177 statements to 0.5s across 107 (per 500-node batch:
~49k buffer reads → ~4.2k, 9ms).

### Changed

- Workflow retention batches by workflow, not by node row, in a single
  statement (`DELETE_EXPIRED_WORKFLOW_TASKS_SQL` removed). Candidates come
  from the workflows retention index; each is guarded once per statement
  against a materialized set of workflows with non-terminal backing tasks,
  computed from the non-terminal side (`status IN` the terminal set's
  enum-derived complement, served by the tasks status index) instead of a
  per-candidate `NOT terminal` heap walk. Node rows purge set-wise in the
  same statement; a workflow with more nodes than the whole batch budget
  drains alone instead of starving. Deletion semantics unchanged: same
  cutoff, same guard meaning, a workflow and its node rows leave together.
- Finalize logs scope their claim to the finalize-retry loop:
  `Finalize stage will not be re-attempted` replaces
  `Finalize error non-retryable`, and the broken-pool message states that
  the task was already recovered per its retry policy and the pool
  replaced. The old wording read as task loss when no task is lost.

### Fixed

- SKILL.md's task-info example steered readers to the broker-level
  `get_task_info_async`, whose `decoded_result` is always empty; it now
  shows `app.get_task_info_async` (typed decode) as the primary path with
  the broker variants labeled raw-envelope.

## [0.4.3] - 2026-08-02

Monitoring read-path performance release. Schema v16
(`idx_horsies_tasks_enqueued_at`, `idx_horsies_tasks_task_name`). At 1M
rows / 1.3GB heap, the task list drops from ~167k buffer reads (full seq
scan + top-N sort per page view) to 53, and the unfiltered task-name facet
from ~167k to ~1.2k (index-only scan).

### Added

- Schema v16: indexes for the monitoring read path — `enqueued_at`
  (task list default sort) and `task_name` (facet index-only scan).
  Neither column appears in any worker-path predicate or leading sort
  key; a plan test pins the claim statement to
  `idx_horsies_tasks_claim_pending`.
- Plan tests for the monitoring list, task-name facet, and claim
  statement (EXPLAIN-based, same mechanism as the retention plan tests).

### Changed

- `list_tasks` loads only the columns the list view renders; `args`,
  `kwargs`, `result`, and `task_options` are no longer fetched, so a
  list page stops shipping task payloads (previously up to several MB
  of egress per 50-row page) and stops detoasting large results.
- `TaskListPage.total` is a planner estimate (`pg_class.reltuples`) on
  the unfiltered view; any active filter keeps the exact count. A
  never-sampled table falls back to the exact count.
- Sort ordering wraps only nullable sort keys in `NULLS LAST`. On the
  NOT NULL `enqueued_at` the wrapper was semantically inert but blocked
  index-order scans (`DESC NULLS LAST` does not match a plain btree's
  `DESC`); results are unchanged.
- Web UI: the task aggregates (stats, facets, breakdown) no longer
  refetch on every task event (the server debounce emits up to 4
  events/s under load). They refresh on their existing cadences
  (10s/12s/30s) in both live and fallback modes, on stream reconnect,
  and on explicit user action. SSE keeps driving the task list, task
  detail, and workflow run/node views.

### Fixed

- Docs changelog frontmatter failed YAML parsing (a `: ` sequence in the
  0.4.2 summary); the docs site had not deployed since the v0.4.2 merge.

## [0.4.2] - 2026-08-01

Retention and payload-path performance release. Schema v15
(`idx_horsies_tasks_queue_retention`). Retention sweep defaults changed:
5-minute cadence and 500-row batches replace hourly 5,000-row batches.

### Added

- Per-queue terminal retention windows:
  `RecoveryConfig.queue_terminal_record_retention_hours` maps queue names
  to override windows (hours) for plain (non-workflow) tasks. Queues not
  listed use the global window; overrides apply even when the global
  window is `None`; workflow-backing task rows always age under the
  global window and are protected while their workflow is non-terminal.
  After workflow termination, each task row ages from its own terminal
  timestamp.
  Override keys must name declared queues — an unknown key fails config
  construction (and therefore `horsies check`) with HRS-200. Override
  deletes are served by the new queue-leading composite partial index
  (schema v15); the global delete keeps the v11 index.
- Payload-size guardrail: `AppConfig.payload` (`PayloadPolicy`, exported
  from `horsies`). `warn_bytes` (default 1 MiB, `None` disables) logs a
  warning rate-limited to once per task name and payload kind per
  process; `reject_bytes` (default `None` — off) fails an enqueue closed
  before any row is written — `.send()` returns
  `Err(TaskSendError(PAYLOAD_TOO_LARGE))`, a scheduled fire fails its
  slot. Results are never rejected. The check is one integer comparison
  against the already-serialized string. Coverage by boundary (task
  sends, scheduled fires, workflow-node enqueues, results) is documented
  on the app-config page.
- `RecoveryConfig.retention_sweep_interval_s` (default 300, 30s–24h) and
  `RecoveryConfig.retention_delete_batch_size` (default 500, 50–10,000)
  replace the previously hardcoded hourly cadence and 5,000-row batch
  size.

### Changed

- Retention deletes purge `horsies_task_attempts` set-wise in the same
  statement as the parent task batch instead of relying on the per-row
  FK `ON DELETE CASCADE` (which remains as a correctness net). The
  row-level cascade cost more in aggregate than the parent delete itself
  (at 37,500 deletes/day: 20.6 s/day cascade versus 17.6 s/day parent
  batch).
- Retention batch statements are bounded to tens of milliseconds by the
  new defaults; the previous hourly 5,000-row batches ran 0.5–1.6 s each
  while holding row locks.
- The result-wait terminal fetch selects only `task_name`, `status`, and
  `result` instead of the full task row, so args/kwargs no longer ship
  with result polls — for kwargs-heavy tasks this removes nearly all
  poll egress. Same `RawResultRecord` contract.
- The scheduler's missing-row existence check runs on a ~60 s wall-clock
  cadence (every tick while a row is missing or a re-init failed)
  instead of every tick. At 108 schedules on a 1 s tick the per-tick
  check read 9.1M rows/day from a 108-row table for an answer that
  changes only on rare events; worst-case detection of an externally
  deleted state row moves from one tick to ~60 s.

## [0.4.1] - 2026-07-27

Worker lifecycle fix: a timeout kill landing while the replacement process
pool warmed no longer stops the worker. No schema change (v14).

### Fixed

- Executor replacement published the new pool before its children finished
  warming, so dispatch ran real tasks on the warming pool; a per-task
  timeout SIGKILL landing on one of them — or warm rounds finding too few
  distinct children because real tasks occupied them — failed the warmup
  itself, and the worker exited fail-closed
  (`worker child warmup started X/N process(es)`). A backlog of ~12 queued
  timeout-prone tasks crash-looped the worker at any pool size. The pool is
  now published only after warmup completes: dispatch waits on the restart
  lock instead of feeding a warming pool, and no timeout kill can target
  one. Present since 0.1.8.
- A dispatch-path "executor missing" request that lost the race with a
  concurrent restart could destroy the healthy pool that restart had just
  built. Ensure and replace are now separate operations: the dispatch path
  creates a pool only when none is published and never tears one down;
  force-replace requires the failed pool and is identity-guarded.
- Warmup interruptions are classified by type: `BrokenProcessPool` and the
  new `WarmupIncompleteError` retry the create+warm cycle (3 attempts,
  stop-aware delay) before failing closed; child-hook failures still stop
  the worker without retrying; OS-level and memory-baseline failures stay
  immediately fatal.
- Cancellation during warmup (the 30s start-attempt timeout) tears the
  unpublished pool down instead of leaking its children; a stop requested
  during warmup discards the pool unpublished.

## [0.4.0] - 2026-07-27

The web monitoring dashboard: a typed monitoring API in core, registry-free
task cancel/retry primitives, a mountable FastAPI app plus a `horsies web`
CLI behind the new `web` extra, and the Acme Clothing showcase application.
No schema change (v14).

### Added

- `horsies.monitoring` (core, no extra): typed read-query API over tasks,
  workflows, workers, and schedules (`task_stats`, `task_facets`,
  `task_breakdown`, `list_tasks`, `get_task_detail`, workflow run/node
  queries, `list_schedules`), with `error_category` filtering expanded
  server-side from the built-in code registry. An absent row is `Ok(None)`;
  `Err` is reserved for database failures.
- `cancel_task` / `retry_task`: registry-free, single-transaction
  compare-and-set task actions. A committed cancel cannot be overwritten by
  claim, finalize, auto-retry, or reaper paths — each carries its own status
  guard. Retry reuses the task row, preserves attempt history
  (`retry_count` becomes the highest recorded attempt), never modifies
  `max_retries` or `good_until` (a task past its deadline is refused, not
  revived), and emits the queue NOTIFY the insert trigger does not fire for
  updates. Cancelling a RUNNING task requires `include_running=True`: the row
  flips durably but the process keeps executing and no attempt row is
  recorded for that run. Workflow-bound rows are refused — workflow runs are
  managed only by the workflow primitives.
- `horsies[web]` extra: `create_monitoring_app(app, *, auth_policy, config)`
  — a mountable FastAPI monitoring application — and the `horsies web` CLI
  (app-path form with full features, or registry-less `--database-url` form;
  the latter supports all reads, task actions, and workflow pause/cancel, but
  not resuming runs whose next nodes carry `args_from`). The dashboard SPA is
  bundled into the wheel as static assets; no Node at runtime. The library
  never owns identity: mounted mode requires an explicit
  `MonitoringAuthPolicy`, and the CLI is fail-closed — loopback default,
  trusted-header auth required off-loopback, actions off unless
  `--enable-actions`, `X-Horsies-Intent: action` required on mutations.
- No-DDL guarantee for monitoring: brokers constructed by the web layer skip
  schema migrations (new keyword-only `run_schema_migrations` on
  `Horsies`/`PostgresBroker`, default unchanged for every existing caller).
  Schema state is probed and reported as MATCH / MISMATCH / ABSENT / UNKNOWN:
  mismatch serves reads and refuses actions with 409 `SCHEMA_INCOMPATIBLE`; a
  database with no horsies schema is reported, never initialized; an
  unreachable database is UNKNOWN, never conflated with absence.
- Live updates: one `PostgresListener` per web process on the existing
  `horsies_task_status` / `horsies_workflow_status` / `horsies_worker_state`
  channels, coalesced into SSE invalidation events (`GET /api/events`);
  client polling is a fallback that activates only while the stream is down.
- `showcase/`: Acme Clothing, a runnable demonstration application — 35
  tasks, 12 workflow definitions, 31 schedules across all six pattern types,
  8 scenarios — with deterministic stable-hash failure draws so every run
  reproduces. Own README, Procfile, docker-compose, and a Quick Start docs
  page.
- Agent skills: new `monitoring.md`; `horsies web` added to the CLI
  reference.
- CI: `webui` and `showcase` jobs; the release workflow builds the SPA before
  the wheel and blocks unless the wheel carries the built assets.

### Fixed

- `args_from` envelopes now decode for optional targets. The codec's
  passthrough matched only a bare `TaskResult[...]` annotation, so the
  `TaskResult[T, TaskError] | None` form — the documented requirement for
  `join='any'`/`'quorum'` targets — sent the envelope through the value
  decoder and failed `WORKER_SERIALIZATION_ERROR` ("reserved key
  '__h_taskresult_envelope__' in user-originated data") whenever the source
  resolved in time. Unions are unwrapped before the passthrough test.
- Documentation: `PostgresConfig` examples wrap `database_url` in
  `pydantic.SecretStr` (the field's type — plain strings validate at runtime
  but fail strict type checking), and PEP 695 `type` aliases in `TaskResult`
  ok slots are documented as unsupported (rejected by strict-serde at startup,
  HRS-105).

## [0.3.1] - 2026-07-24

The workflow retention DELETEs plan onto their v13 index. Schema v14 (two
expression-statistics objects plus an ANALYZE, applied automatically by the
broker's advisory-locked schema init on next startup). Documented PostgreSQL
floor raised from 12+ to 14+.

### Fixed

- The workflow retention DELETEs ignored the v13 partial index: the planner
  never uses statistics gathered on a partial index for whole-table
  selectivity, so the retention COALESCE cutoff was costed at the default
  1/3 selectivity and index-vs-walk became a function of table size alone.
  At 1M retained tasks the heap walk is expensive enough that the index
  wins regardless; at 36k retained workflows the planner kept a full-table
  walk — estimate 12,245 vs 13 actual, 4–5 s per statement, two statements
  per hourly reaper pass, independent of eligible-row count — and the same
  misestimate degraded the NOT EXISTS guard into a seq scan of the 1M-row
  tasks table whenever the outer side did use the index. Schema v14 creates
  whole-table expression statistics (`CREATE STATISTICS ... ON (<retention
  COALESCE>)`) for `horsies_tasks` and `horsies_workflows` and runs ANALYZE
  in the same migration transaction — extended statistics are empty until
  the table is analyzed after their creation; ANALYZE is sampled, takes
  SHARE UPDATE EXCLUSIVE, and never blocks reads or writes. Measured at 1M
  tasks / 36k workflows / 144k workflow_tasks with the shipped statements:
  workflows delete 98 ms → 7.7 ms, workflow_tasks delete 413 ms → 34 ms,
  guard planned as per-row index probes instead of the 1M-row seq scan. No
  statement changes.

### Changed

- Documented PostgreSQL floor raised from 12+ to 14+:
  `CREATE STATISTICS ON (expression)` requires PostgreSQL 14 (12 and 13
  are past end-of-life).

## [0.3.0] - 2026-07-23

Sync send fails closed on a running event loop (breaking for callers that
silently blocked their loop), service-loop death is process-fatal, and three
child-process expiry/recycle defects are fixed. Schema v13 (one partial
index, applied automatically by the broker's advisory-locked schema init on
next startup).

### Changed

- **Breaking**: sync `.send()`, `.schedule()`, `.retry_send()`, and
  `.retry_schedule()` called inside a running event loop return
  `Err(TaskSendError(ASYNC_CONTEXT))` before any broker work. The sync
  executors are blocking database round trips; inline on a loop they stall
  every coroutine on it for the duration of the enqueue, and a burst of
  inline sends serializes the whole loop. The error carries the prepared
  `task_id` and payload: complete the dispatch with `retry_send_async(err)`
  / `retry_schedule_async(err)`, or call the matching `*_async` entry point
  with the original arguments. Off-loop callers are unaffected.
- A worker-lifetime service loop (claimer heartbeat, worker-state snapshot,
  ping responder, reaper) that ends with no shutdown requested — escaped
  exception or unexpected return — is now process-fatal
  (`ServiceLoopDiedError`): the loops contain their own per-iteration
  errors, and a worker that keeps claiming with a dead heartbeat loop stops
  renewing claim leases, disappears from monitoring, and contributes no
  reaper passes while looking healthy. Previously this ended the task with
  one log line; now the error is captured, the worker stops, and
  `run_forever` re-raises for a non-zero exit and supervisor restart.

### Added

- `schedule_async()` and `retry_schedule_async()` on task functions and
  `with_options(...)` builders, with the same delay validation as their
  sync counterparts. `ASYNC_CONTEXT` added to `TaskSendErrorCode`; retry
  methods accept it alongside `ENQUEUE_FAILED`.

### Fixed

- A claimed workflow task whose `good_until` passed before child start was
  marked EXPIRED by the child itself, but the parent's phase-1 skip arm
  never ran phase 2 — the workflow node stayed ENQUEUED against a terminal
  task row until reaper recovery case 1.7 repaired it 10–40 s later at
  default thresholds. Phase 1 now loads the child-persisted terminal result
  for workflow tasks and proceeds to phase 2, so the workflow resolves per
  `on_error` without waiting for the reaper. Plain-task TASK_EXPIRED keeps
  the skip.
- A failed replacement spawn during child recycle (fork ENOMEM/EMFILE) ran
  on the executor manager thread with no containment: the manager died via
  `threading.excepthook`, `_broken` was never set, later `submit()` was
  accepted silently, and pending futures hung PENDING forever — only
  3–5-minute reaper thresholds bounded the damage. The spawn is now
  contained: the pool is marked broken, pending futures fail with
  `BrokenProcessPool`, and the failure surfaces through the existing
  recovery path (requeue + executor restart).

### Performance

- Schema v13 adds `idx_horsies_workflows_retention`, a partial expression
  index on `COALESCE(completed_at, updated_at, created_at)` over terminal
  workflow statuses. Both workflow retention deletes filter
  `horsies_workflows` on that predicate; with no index and no statistics on
  the expression, the planner overestimated eligibility and chose a
  stop-early pkey walk whose `LIMIT` never filled — a full-table walk per
  statement, twice per hourly pass, serial under `FOR UPDATE`, regardless
  of how few rows were eligible (36k retained workflows ≈ 4–5 s per
  statement with zero eligible rows). Completes the v11/v12
  retention-index set. A row enters the index once, at its terminal
  transition; updates during a workflow's running life never maintain it.
- A recycling child exits with `os._exit(0)` after flushing logging and
  stdio instead of running full interpreter teardown. The result is
  already on the wire before exit, and the stdlib manager inlines
  `p.join()` on the recycled child before reading further results, so
  teardown of the very heap that triggered the recycle head-of-line
  blocked sibling results — measured ~35 ms per million heap objects
  (728 ms at a 2.2 GB heap).

### Documentation

- New `internals/operational-indexes` page: opt-in DDL for adopter-side
  history queries that horsies deliberately does not index in the shipped
  schema — the verified partial terminal-only
  `(task_name, COALESCE(completed_at, failed_at, updated_at) DESC)` index,
  its exactness requirements, the all-rows-index and non-`CONCURRENTLY`
  anti-patterns, `CONCURRENTLY`'s own constraints (no transaction block;
  a failed build leaves an `INVALID` index that `IF NOT EXISTS` treats as
  present), and the compatibility contract.

### Internal

- E2E suites route sends through `send_async` where tests run on a loop;
  worker ready-checks execute off the event-loop thread, and the
  ready-poll drains a dead worker's output with a bounded `communicate()`
  instead of a blocking read-to-EOF that hung when the process group held
  the pipe. Manual `propagate` flips around caplog removed — obsolete and
  double-capturing under pytest 9.1, and their hardcoded restore corrupted
  the propagation baseline. Docs site on astro 7 + starlight 0.41.

## [0.2.9] - 2026-07-16

Two correctness fixes on the workflow-cancel and task-finalize paths, plus a
retention index. Schema v12 (`horsies_claim` return type + one index, applied
automatically by the broker's advisory-locked schema init on next startup;
rolling deploys are safe — pre-v12 workers select named columns and never
bind the new fence parameter).

### Fixed

- Cancel/completion deadlock: the cancel transaction locked `horsies_tasks` →
  `horsies_workflow_tasks` → `horsies_workflows`, while task completion locks
  `horsies_workflows` → `horsies_workflow_tasks`. A task completing while its
  workflow is cancelled deadlocked; Postgres aborted one side with SQLSTATE
  40P01 — a spurious `DB_OPERATION_FAILED` on `cancel()` or an aborted
  completion transaction. Cancel now takes the workflow row lock first, for
  the parent and for each descendant in the cascade. Lock-order invariant
  (workflows before workflow_tasks) documented at both statements.
- Stale finalize could clobber a live attempt: finalize/recovery statements
  fenced on `(status, worker_id)` alone cannot reject a stale actor when the
  SAME worker re-claims its own reaper-requeued task — a stalled attempt that
  later finished marked the task COMPLETED with its own result while the
  re-claimed attempt was still executing, and the attempt-history row was
  attributed to the wrong attempt. Every statement acting on a row the worker
  believes it owns — the fused ok-path finalize, the err-path context lock,
  the timeout-handler and future-failure row locks, the standalone unclaim,
  orphan termination, and the child's CLAIMED→RUNNING ownership confirm — is
  now fenced to the claim generation: `claimed_at`, returned by the claim
  statements, cleared by every requeue. A NULL fence preserves the previous
  matching for callers without a dispatch context. The confirm fence also
  closes soft-cap same-worker double dispatch after lease expiry.

### Performance

- Schema v12 adds `idx_horsies_heartbeats_sent_at`: heartbeat retention
  deletes filter `sent_at < cutoff`, but the composite
  `(task_id, role, sent_at DESC)` index cannot serve a leading-column
  `sent_at` range, so every hourly retention pass scanned the heartbeats
  heap — the highest-insert-rate table in the schema. The v11 retention
  indexes covered tasks and worker_states and omitted heartbeats.

### Documentation

- Soft-cap concurrency docs state the real overshoot bound: with
  `prefetch_buffer > 0`, buffered CLAIMED tasks are invisible to the
  per-queue cap and dispatch without a re-check, so RUNNING can exceed the
  cap for the duration of the excess tasks, and N workers can reach
  N × cap (previously described as short-lived bursts during claiming).
- The workflow engine's transaction-ownership invariant (the engine never
  commits — node-FAILED, `on_error='pause'`, and the child-pause cascade
  share the caller's single commit) is documented and pinned by a
  source-level tripwire test.

## [0.2.8] - 2026-07-09

Removes the two remaining full-table scans on the monitoring/retention path,
bounds retention deletes, and fixes two claim/finalize defects. Schema v11
(two indexes, applied automatically by the broker's advisory-locked schema
init on next startup).

### Performance

- `list_worker_states` reads the latest snapshot per worker via a recursive
  skip-scan — one `(worker_id, snapshot_at DESC)` index probe per worker —
  instead of a `DISTINCT ON` pass over the whole snapshot timeseries. At 118k
  retained rows / 6 workers: 10.3 s → 9.2 ms.
- Schema v11 adds retention eligibility indexes: `idx_horsies_tasks_retention`
  (partial expression index on `COALESCE(completed_at, failed_at, updated_at,
  created_at)` over terminal statuses) and
  `idx_horsies_worker_states_snapshot_at`. The hourly retention pass
  previously seq-scanned both heaps (593 MB + 157 MB at 554k tasks / 118k
  snapshots) even with zero eligible rows. A task row enters the tasks index
  once, at its finalize transition; claim and lease-renewal updates never
  maintain it.

### Added

- `RecoveryConfig.worker_state_snapshot_interval_ms` (1 s–5 min, default
  30 s): how often each worker inserts a monitoring snapshot row into
  `horsies_worker_states`. Previously hardcoded to 5 s.

### Changed

- Worker-state snapshot cadence default is 30 s (was the hardcoded 5 s):
  ~20k snapshot rows per worker per week at the 7-day default retention
  instead of ~120k. Set `worker_state_snapshot_interval_ms=5_000` to keep the
  old resolution.
- Retention deletes run in 5,000-row batches, one transaction per batch,
  under a 60 s per-pass budget (previously five unbounded DELETEs in one
  transaction). Enabling retention on a long-running database no longer
  produces a single DELETE of the entire backlog — it drains across
  consecutive hourly passes — and a mid-pass failure loses one batch instead
  of rolling back the whole pass. Concurrent passes drain disjoint batches
  via `FOR UPDATE SKIP LOCKED`.

### Fixed

- Per-queue `max_concurrency` is enforced when `queue_priorities` is not
  configured. A cap-only queue previously either ran uncapped (empty priority
  map: concurrent claimers over-claimed past the cap) or was never serviced
  (partial priority map omitting the capped queue).
- Workflow finalization phase-2 replay accepts EXPIRED rows. An
  expired-before-start task writes terminal EXPIRED plus a `TASK_EXPIRED` err
  result, but the replay reload only accepted COMPLETED/FAILED and discarded
  it as "terminal task result unavailable", wedging in-process replay for the
  expired case.

## [0.2.7] - 2026-06-24

Collapses the worker claim critical section into a single server-side statement.
Each claim pass previously held the cap-serialization advisory lock across many
client round trips (one `pg_advisory_xact_lock` per capped queue, the cap-counts
query, per-queue claims, then `COMMIT`); at pooled-connection RTT a client-side
stall while holding the lock froze every claimer cluster-wide. A new
`horsies_claim(...)` SQL function acquires the locks, computes the cap/budget
accounting, and runs the windowed claim in one statement, so the lock is held
only across that statement plus the commit — never across a client round trip.
Cap enforcement is unchanged: the function never over-claims.

### Performance

- The claim pass issues one statement under the lock instead of 7–9. At ~33 ms
  RTT with 16 concurrent claimers the worst single advisory-lock wait drops from
  ~5 s to ~0.5 s, and claim throughput scales with worker count instead of
  flat-lining at the lock's serialization ceiling.

### Changed

- Equal-queue-priority tie-break. When two queues share the same priority, the
  prior per-queue loop broke ties by configured queue order; the claim now pools
  the equal-priority band and orders by task priority, then enqueue time (FIFO).
  Equal-importance tasks across such queues are claimed FIFO, while an explicit
  task/workflow-node priority still preempts within the band. Distinct queue
  priorities are unaffected. Under `FOR UPDATE SKIP LOCKED` contention the single
  windowed pass may claim fewer rows than the prior greedy loop; the deferred
  rows are picked up on the next pass.

### Database

- Schema v10: adds the `horsies_claim` function, applied automatically by the
  broker's advisory-locked schema init on next startup. No manual migration.

## [0.2.6] - 2026-06-23

A workflow scheduling correctness fix. A parameterized sub-workflow whose
`build_with()` returns a direct `WorkflowSpec(...)` bypassed `app.workflow()`
binding, so its `TaskNode`s reached the engine with `priority=None` and the
engine defaulted them to a literal `100`. On a CUSTOM queue (e.g. `scraping` at
priority 30) the child task persisted at priority 100 — correct queue, wrong
intra-queue claim order (`ORDER BY priority ASC, enqueued_at ASC`). No schema
change (still v9).

### Fixed

- Subworkflow tasks now persist at their queue's configured priority. Queue and
  priority resolution is centralized in a single bind boundary
  (`resolve_node_queue_and_priority`) that `app.workflow()`, the engine
  subworkflow child branch, and `check()` all route through, replacing three
  divergent `else 100` fallbacks. An explicit `node.priority` is preserved;
  `None` inherits the queue priority via `effective_priority`. DEFAULT-mode
  queues are unchanged (`effective_priority` returns 100). An invalid child
  queue is now contained with `WORKFLOW_ENQUEUE_FAILED` rather than guessing a
  default. (#158)

### Dependencies

- Dev and website dependency bumps via Dependabot. (#157, #156)

## [0.2.5] - 2026-06-17

Connection durability for remote/pooled Postgres, plus two recovery-path
fixes. Default-on TCP keepalives keep idle broker and child-pool sockets warm
so a server-side reap (PlanetScale's PgBouncer drops idle connections within
~1–2h) no longer surfaces as a mid-query error. The reaper gains a grace
window so it stops racing in-flight two-phase finalizers, and a direct
`WorkerConfig(pgbouncer_transaction_mode=True)` now stays consistent with its
child connect kwargs. No schema change (still v9).

### Added

- TCP keepalive configuration on `PostgresConfig`: `tcp_keepalives` (bool,
  **default on**), `tcp_keepalives_idle` (30), `tcp_keepalives_interval` (10),
  `tcp_keepalives_count` (3). These libpq params apply to the broker engine
  pool and each child-process psycopg pool. libpq enables keepalives by default
  but leaves the idle interval at the OS default (often 7200s); Horsies sets it
  to 30s so a dropped socket is detected and recycled before the next query.
  The LISTEN/NOTIFY listener keeps its own keepalives. Non-positive
  idle/interval/count values are rejected when keepalives are enabled
  (`HRS-215`). (#100)

### Fixed

- Idle pooled broker connections reaped server-side (e.g. PlanetScale's
  PgBouncer pooler, which drops idle connections within ~1–2h) surfaced as a
  mid-query `OperationalError` on the next claim or heartbeat. `pool_pre_ping`
  and `pool_recycle` are checkout-time guards and cannot catch a connection
  that dies in-flight; the default-on TCP keepalives now keep idle sockets warm
  at the socket layer. No configuration is required for remote/pooled
  deployments. (#100)
- The reaper recovered a workflow task the instant its underlying task went
  terminal (Case 1.7: task terminal but workflow progression not yet applied),
  with no grace window. Under load — and especially with frequent child
  recycling — this raced healthy two-phase finalizers (Phase 1 commits the task
  terminal, Phase 2 advances the workflow), "recovering" tasks whose Phase 2 was
  merely in flight and adding up to one reaper interval of latency per affected
  task. Case 1.7 now honours a grace window
  (`RecoveryConfig.crashed_worker_recovery_grace_ms`, new, default 10s, not
  coupled to heartbeat thresholds): a task that went terminal within the window
  is left for its in-flight finalizer; only genuinely-stuck tasks are recovered.
  A genuine crash in that gap recovers after the grace plus one reaper sweep.
  Correctness was never at risk — recovery
  replays the stored result idempotently (the `FOR UPDATE` + already-terminal
  CAS in `on_workflow_task_complete`), and the task body never re-runs — this is
  a latency and log-noise fix. The recovery log line is reworded from "crashed
  worker" (a COMPLETED task is not a crash) to reflect the actual condition.
- A direct `WorkerConfig(pgbouncer_transaction_mode=True)` built without
  `child_connect_kwargs` left child pools with prepared statements enabled
  against a transaction-pooled PgBouncer. `WorkerConfig.__post_init__` now
  ensures `prepare_threshold=None` in `child_connect_kwargs` when the flag is
  set (without overriding an explicit value), so the flag alone is sufficient.
  The CLI path was unaffected. (#152)

## [0.2.4] - 2026-06-17

Per-child memory recycling, complementing count-based `--max-tasks-per-child`.
Heterogeneous workloads make a task count a poor proxy for a bytes budget: the
correct recycle point depends on a child's RSS, not how many tasks it ran. This
release also fixes a latent CPython gh-115634 hang in the existing count-recycle
path.

### Added

- `--max-memory-per-child-mb N` worker flag (`WorkerConfig.max_memory_per_child_mb`,
  positive int, **default off**): recycle a child once its own resident memory
  reaches N MB. The child samples its RSS after each real task and, at or above
  the threshold, finishes the task, sends its result, and exits cleanly via the
  stdlib `exit_pid` marker — the pool replaces only that child. It is a
  retention guardrail, not a hard sandbox: a task that exceeds the threshold
  while still running is not interrupted. **CPython-only** (built on private
  `ProcessPoolExecutor` internals, asserted at startup) and forces the `spawn`
  start method. A startup baseline guard fails the worker when the threshold is
  at or below the warmed child baseline (the app does not fit the per-child
  budget) and warns within 80% of it.

### Fixed

- Count-based child recycling (`--max-tasks-per-child`, including the default
  `100`) routed through the stock `ProcessPoolExecutor`, which can hang under
  queued load when a cleanly-recycled child is not replaced (CPython
  gh-115634). Count recycling now uses an executor that overrides
  `_adjust_process_count` to always replace a recycled child, falling back to
  the stock pool only if the required internals are absent (no version gate;
  the override is correct wherever the surface exists).
- Worker logs emitted raw ANSI color escapes to non-TTY sinks (log drains,
  container logs, journald, files), breaking grep and log parsers. Color is now
  gated by `_should_use_color` (stream `isatty()`, with `NO_COLOR` / `FORCE_COLOR`
  overrides); non-TTY output is plain text with the same layout.
- Task execution log lines no longer wrap the id/name in one-element lists. Both
  the start and completion lines now read `task_name (task_id)`.

## [0.2.3] - 2026-06-16

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

## [0.2.2] - 2026-06-15

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
  workflow / `workflow_task` rows while a backing task is still non-terminal.
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

## [0.2.1] - 2026-06-14

A failed outputless child workflow used through a `SubWorkflowNode` leaked the
internal terminal-results envelope into the parent node, raising a strict-serde
reserved-key error that wedged the parent and, via the recovery loop, starved
recovery for every stuck workflow. The propagation path and recovery isolation
are both fixed. No schema change (still v8).

### Fixed

- Outputless child workflows no longer leak the internal terminal-results
  envelope into parent `SubWorkflowNode` results or `SubWorkflowSummary.output`.
  Completed outputless children propagate as `TaskResult[None, TaskError]`
  with `ok=None`. (#141)
- Workflow recovery now isolates candidate failures so one poison workflow row
  cannot abort the whole recovery pass. (#141)

### Changed

- `WorkflowDefinition[T]` with no `Meta.output` is now rejected for concrete
  `T`. Use `WorkflowDefinition[None]` for outputless orchestration workflows,
  or set `Meta.output` to a node producing the declared type. (#141)

## [0.2.0] - 2026-06-12

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

## [0.1.10] - 2026-06-12

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

## [0.1.9] - 2026-06-11

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

## [0.1.8] - 2026-06-11

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
