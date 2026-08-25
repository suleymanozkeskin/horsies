# Changelog

All notable changes to horsies-rust are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The project is pre-1.0. Breaking changes may ship in alpha releases.

## [0.1.0-alpha.30] - 2026-08-25

### Fixed

- Delayed-retry and stale-task retry wake-ups now send the task UUID to
  PostgreSQL `pg_notify` as text. PostgreSQL rejected the previous UUID bind.
  The retry state stayed durable and worker polling found it later, but the
  direct wake-up did not occur.

### Changed

- The monitoring `/api/tasks/stats` route now caches each successful request
  scope for 10 seconds. Concurrent requests for the same scope share one
  aggregate query. Errors are not cached. The cache holds at most 256 scopes.

### Removed

- Migration 0044 drops `idx_horsies_tasks_retention` and
  `idx_horsies_tasks_queue_retention`. Terminal tasks move to partitioned
  history, so live-task retention does not use these indexes.

### Upgrade

- Apply migration 0044 before processes use this release.

## [0.1.0-alpha.29] - 2026-08-19

### Fixed

- `serde_json/arbitrary_precision` now sits behind the off-by-default
  `arbitrary-precision` Cargo feature. The flag changes `serde_json` number
  handling for the whole build graph, and Cargo has no negative features, so
  every crate that depended on horsies inherited it. Under the flag, any
  `#[serde(flatten)]`, `#[serde(tag = "...")]` or `#[serde(untagged)]`
  container rejects a typed float with `invalid type: map, expected f64`,
  including containers horsies never sees. Enable the feature only for
  byte-exact rerun-input fingerprints over integers outside the i64/u64
  domain.

### Changed

- The default build rejects an integer literal outside the i64/u64 domain in
  `args`, `kwargs`, `task_options`, and in a stored rerun-input envelope.
  Without the retained source lexeme, such a literal parses to binary64 and
  loses digits with no error, which would anchor the input digest to a value
  the caller did not send. Build with `arbitrary-precision` to accept these
  literals.
- Without `arbitrary-precision`, the canonical form of the `-0` literal is
  `-0.0` rather than `0`. The parse cannot separate `-0` from `-0.0` without
  the source lexeme. Turning the feature on therefore changes the input
  digest for inputs that carry `-0`.

## [0.1.0-alpha.28] - 2026-08-14

### Fixed

- Worker panic conversion on the blocking and join paths now reports
  `UNHANDLED_ERROR`. These paths reported `TASK_ERROR` before. The
  documented vocabulary maps a worker-captured panic to
  `UNHANDLED_ERROR` and a returned `Err(TaskError)` to `TASK_ERROR`.
  The async panic path already followed the documented mapping.

## [0.1.0-alpha.27] - 2026-08-13

This release adds the browser monitoring dashboard and its transport-free read
and action API. The migration chain advances from 0042 to 0043.

### Added

- Added the optional `web` Cargo feature. It provides an axum router and an
  embedded React dashboard for tasks, workflows, workers, and schedules.
- Added the transport-free `horsies::monitoring` module. It provides typed
  live-plus-history reads and task or workflow action decisions.
- Added `horsies web` with TOML and database-URL modes. The command supports
  view-only service, trusted proxy headers, actions, custom CSS, and PgBouncer
  split URLs.
- Added migration 0043 with the task `enqueued_at` and `task_name` monitoring
  indexes.
- Added server-sent invalidation events for task, workflow, and worker changes.

### Changed

- Dashboard task lists and aggregates now merge live tasks with a bounded
  retained-history window. The default window is 24 hours. The maximum window
  is 30 days.
- Dashboard actions require authorization, `X-Horsies-Intent: action`, an exact
  schema version, and the task-history cutover attestation.
- The standalone dashboard uses observe-only startup. It never applies
  migrations or writes cutover state.
- Monitoring task actions now cancel eligible live tasks. Workflow actions now
  pause, resume, or cancel eligible workflows.

### Removed

- Removed the stale manual-retry control from the vendored dashboard. Use the
  programmatic `rerun_task` API to create a new lineage-bearing task from an
  eligible retained source.

### Security

- `--auth none` now requires a loopback bind. Network deployments must use a
  trusted proxy or mount a custom `MonitoringAuthPolicy`.
- Trusted-header mode requires the proxy to strip or overwrite the configured
  identity header. The CLI prints this requirement at startup.
- Schema mismatch, absent schema, incomplete cutover, and unknown schema states
  disable actions without running DDL.

### Upgrade

- Apply migration 0043 before enabling actions. Reads can remain available
  while the schema probe reports a version mismatch.
- Rebuild with `--features web` to include the monitoring router and dashboard.
- See the [web UI deployment guide](https://suleymanozkeskin.github.io/horsies-rust/monitoring/web-ui-deployment/).

## [0.1.0-alpha.26] - 2026-08-13

This release adds the task-history live/archive split. The Rust migration chain
advances from 0032 to 0042. Migration 0032 is the schema-v26 compatibility
boundary. The final table shape matches task-history schema v35.

### Added

- Added `RetentionConfig` and `RetentionClassConfig` under
  `AppConfig.retention`.
- Added immutable finite retention classes. Daily partitions enforce each
  class window.
- Added per-queue retention through `queue_retention`. An explicit send choice
  overrides the queue mapping. Unmapped queues use `standard_30d`.
- Added `TaskSendOptions::retention_class(...)` and
  `TaskSendOptions::retain_forever()`.
- Added scoped idempotency keys through
  `TaskSendOptions::idempotency_key(...)`.
- Added retained-task rerun through `rerun_task`, `RerunTask`, and
  `RerunEnqueuePolicy`. A rerun creates a new UUID and records lineage.
- Added the phase-2 workflow outbox and quarantine. The worker reports pending,
  failed, over-bound, and quarantined recovery evidence.
- Added `WorkflowStatus::Expired`. Optional paused-workflow expiry stores a
  structured `WORKFLOW_EXPIRED` error.
- Added the `horsies cutover` and `horsies transcode` operator commands.

### Changed

- Terminal tasks now move from `horsies_tasks` to partitioned
  `horsies_task_history` in the terminalization transaction.
- `horsies_tasks` now accepts only `PENDING`, `CLAIMED`, and `RUNNING`.
- Task, workflow, workflow-node, and heartbeat identities now use PostgreSQL
  `uuid`. Public task and workflow handles expose `uuid::Uuid`. New task IDs
  use UUIDv7. Scheduler state keeps its compatibility link in `VARCHAR(36)` and
  converts it at the runtime boundary.
- Terminal attempt rows now become a verified snapshot in the history row.
  Live attempt rows are deleted in the same transaction.
- Task retention now drops whole partitions. It no longer deletes terminal task
  rows in batches.
- Heartbeats now use hourly partitions. The worker creates and drops heartbeat
  leaves during partition maintenance.
- History lookups now use staged reader functions. UUIDv7 time is a lookup hint.
  Every skipped leaf is probed before absence is returned.
- Each history leaf now has a task-ID index and an `enqueued_at` index.
- Worker startup requires migration 0042 and the
  `task_history_v1_validated_v1` cutover attestation.
- Pausing a workflow now relocates claimed backing tasks to history. Resume
  creates fresh backing task rows.
- A regular workflow node now records `started_at` when it first reaches
  `RUNNING`, not when its backing task is enqueued. A replay preserves the
  first timestamp. A requeue or pause reset clears it.
- Retention fields moved from `RecoveryConfig` to `AppConfig.retention`:
  `terminal_record_retention_hours`, `worker_state_retention_hours`,
  `retention_sweep_interval_s`, and `retention_delete_batch_size`.

### Removed

- Removed `RecoveryConfig.queue_terminal_record_retention_hours`. Use
  `AppConfig.retention.queue_retention`.
- Removed `RecoveryConfig.heartbeat_retention_hours`. Use partitioned heartbeat
  storage and `AppConfig.retention.heartbeat_leaf_horizon_hours`.
- Removed terminal task row cleanup from
  `terminal_record_retention_hours`. Use task retention classes.

### Upgrade

- Existing databases at migration 0032 require an offline cutover. Stop all
  processes. Take a named backup. Apply migrations 0033–0042. Run the
  documented stage order. Restart only after validation writes the cutover
  attestation.
- Fresh databases start at migration 0042 and need no cutover.
- See the [task-history cutover runbook](https://suleymanozkeskin.github.io/horsies-rust/operations/cutover-runbook/).

### Known incompatibilities

- The current Syce release does not support the task-history schema. A
  compatible Syce release is required for complete terminal task and result
  views.
