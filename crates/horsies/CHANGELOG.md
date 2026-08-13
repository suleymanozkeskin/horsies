# Changelog

All notable changes to horsies-rust are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The project is pre-1.0. Breaking changes may ship in alpha releases.

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
