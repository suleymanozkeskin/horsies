---
title: Recovery Config
section: shared
summary: Automatic detection and recovery of stale tasks.
related: [retention-config, ../../workers/heartbeats-recovery, app-config]
tags: [configuration, recovery, heartbeats]
---

## Overview

Tasks can become stale when:

- Worker process crashes mid-execution
- Network partition prevents heartbeats
- Worker machine goes down

Horsies automatically detects and recovers these tasks.

The settings are the `recovery` section of the app config. The API is on the client pages: [Recovery Configuration](/horsies/configuration/app-config/#recovery-configuration) (Python) and [AppConfig](/horsies/rust/configuration/app-config/#appconfig) (Rust).

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_requeue_stale_claimed` | `bool` | `True` | Requeue tasks stuck in CLAIMED |
| `claimed_stale_threshold_ms` | `int` | 120,000 | Ms before CLAIMED task is stale |
| `auto_fail_stale_running` | `bool` | `True` | Fail tasks stuck in RUNNING |
| `running_stale_threshold_ms` | `int` | 300,000 | Ms before RUNNING task is stale |
| `finalizing_stale_threshold_ms` | `int` | 300,000 | Ms a completed child may remain in finalization handoff before recovery |
| `crashed_worker_recovery_grace_ms` | `int` | 10,000 | Grace before recovering a workflow task whose underlying task is terminal but whose workflow progression was not applied; `0` disables |
| `check_interval_ms` | `int` | 30,000 | How often to check for stale tasks |
| `runner_heartbeat_interval_ms` | `int` | 30,000 | How often the worker process writes the heartbeats of its RUNNING tasks |
| `claimer_heartbeat_interval_ms` | `int` | 30,000 | CLAIMED task heartbeat frequency |
| `worker_state_snapshot_interval_ms` | `int` | 30,000 | How often each worker persists a monitoring snapshot row to `horsies_worker_states` (1s–1min). The cap is half the 120 s [live window](/horsies/monitoring/worker-health/), so a running worker stays live when a snapshot write is late by up to half the window. A larger value fails with `HRS-204` |
| `auto_terminate_orphaned_workflow_tasks` | `bool` | `True` | Terminate claimed tasks whose workflow node linkage no longer exists |
| `phase2_quarantine_after_attempts` | `int` | 25 | Recovery passes an unresolvable crashed-worker progression row may retain before its evidence moves to the quarantine table and discovery stops retrying it (3–1,000) |
| `workflow_recovery_failure_limit` | `int` | 25 | Reaper passes in which the recovery of a workflow may raise a persistent error before the workflow fails closed (3–1,000) |

All time values for thresholds and intervals are in milliseconds. Retention windows are in hours; the sweep interval is in seconds.

**Removed in 0.5.0 (setting either fails validation naming the successor):**
`heartbeat_retention_hours` — heartbeats live in hourly partitions and old
partitions drop whole; a row-delete window no longer exists.
`queue_terminal_record_retention_hours` — terminal task records age by the
retention class assigned at enqueue; per-queue row-delete windows no longer
exist.

Retention and partition-coverage fields moved to
[`AppConfig.retention`](/horsies/configuration/retention-config/); this table lists only what
`RecoveryConfig` still owns.

## Recovery Behaviors

What the reaper does with a stale CLAIMED or RUNNING task, and what a worker does when it cannot write
its runner heartbeats, is on [Heartbeats & Recovery](/horsies/workers/heartbeats-recovery/#recovery-actions).
The thresholds that decide it are in [Fields](#fields).

### Workflow Progression After a Crash

For **workflow** tasks, terminalization records the owed workflow progression in a transactional outbox as it moves the task to history, and the reaper consumes those records. Task finalization is two transactions — the task is terminalized first, then the workflow DAG is advanced — so a progression can be owed for a brief moment while its finalizer is still in flight. The reaper waits `crashed_worker_recovery_grace_ms` (default 10s) before consuming such a record, so it does not race a healthy finalizer; only a genuine crash in that gap is recovered (after the grace plus one reaper sweep). This grace is independent of the heartbeat-coupled thresholds. See [Heartbeats & Recovery](../../workers/heartbeats-recovery) for details.

### Unresolvable progression evidence quarantines

A progression record whose disposition keeps refusing to resolve is retried for
`phase2_quarantine_after_attempts` recovery passes (default 25, bounds 3–1,000),
then moved to a quarantine table with its evidence preserved; discovery stops
retrying it. Retaining dispositions are either transient races, which resolve
within a pass or two, or structural conflicts, which never do — the bound gives
a transient an order of magnitude more passes than it needs while capping a
structural row's error-log noise. Quarantined counts, rows over the attempt
bound, and the quarantine function's refusals are published on the worker
health surface beside the phase-2 pass summary.

A progression that raises a persistent error (see the next section) counts
toward the same bound. At the bound the record moves to the quarantine table,
and the workflow fails closed with `WORKFLOW_RECOVERY_LIMIT_REACHED`, because
discovery stops and no other path ends it. A transient raise does not count.

### Workflow recovery limit

The reaper repairs a stuck workflow by running the repair again. A repair that
raises rolls back, so the next pass selects it again. Without a bound, a raise
that repeats would repeat on every pass, and the workflow would stay `RUNNING`.

A workflow fails closed after `workflow_recovery_failure_limit` reaper passes in
which its recovery raised a persistent error (default 25, bounds 3–1,000). At
the default `check_interval_ms` and the default limit, the limit is reached 24
intervals after the first pass, which is about 720 seconds.

- **A transient raise does not count.** A connection, lock, deadlock or timeout
  error clears by itself, so a database outage cannot fail a workflow. The rule
  is the type of the error: `OperationalError`, `InterfaceError`, a pool
  timeout, a disconnect, and the error that follows a lost connection in the
  same session. A transient raise that repeats stays in the log and in the
  health `errors` count.
- **A persistent raise counts once per pass.** This covers a defect, stored data
  that the engine cannot use, and a SQL error outside `OperationalError`. Many
  poison nodes in one workflow add one count per pass.
- **The count is stored.** It is in `horsies_workflow_recovery_failures`, so a
  worker restart keeps it, and all workers share it. A pass that is lost also
  loses its count, so the bound moves later, never sooner.
- **The count has no reset, so a later unrelated raise after a repair reaches
  the limit sooner.** Delete the row of a workflow to start its count again. A
  row is deleted with its workflow.
- **The resume pass does not count.** It logs the raise, and the reaper counts
  the same candidate.

At the limit, the write runs SQL only, because engine code is what raised:

- The node that raised goes `FAILED` with `WORKFLOW_RECOVERY_LIMIT_REACHED`.
- Other `PENDING` and `READY` nodes go `SKIPPED`.
- The workflow keeps a stored first error. If it has none, the limit error is
  stored.
- With no `ENQUEUED` or `RUNNING` node, the workflow goes `FAILED` at once.
- With a live node, the workflow stays `RUNNING` until the last live node ends,
  as for `on_error=OnError.FAIL`. If a `success_policy` is then met by nodes
  that completed, the workflow can end `COMPLETED`.
- The write at once does not evaluate `success_policy`, because that evaluation
  is one of the paths that raise.
- `on_error=OnError.PAUSE` does not apply. A limit must end the workflow, and a
  `PAUSED` workflow does not end by default.

If the limit write fails, the count stays, the workflow stays `RUNNING`, and a
`CRITICAL` line names the workflow and the cause. The next raise tries the write
again. `over_failure_limit` on the worker health surface counts such workflows
and the ones with live nodes.

## Threshold Guidelines

| Threshold | Constraint |
|-----------|------------|
| Running stale, `auto_fail_stale_running` on | Must be >= 2 × (runner heartbeat interval + 2 s) |
| Running stale, `auto_fail_stale_running` off | Must be >= 2x runner heartbeat interval |
| Finalizing stale | Must be >= 2x runner heartbeat interval |
| Claimed stale | Must be >= 2x claimer heartbeat interval |
| Claimed stale | 1 second to 1 hour |
| Running stale | 1 second to 2 hours |
| Check interval | 1 second to 10 minutes |
| Heartbeat intervals | 1 second to 2 minutes |

For the heartbeat timing rules, CPU-heavy tasks and quick tasks, see
[Timing Guidelines](/horsies/workers/heartbeats-recovery/#timing-guidelines).

## Validation

A config that breaks a rule of [Threshold Guidelines](#threshold-guidelines) is refused with `HRS-204`. The refusal names the fields and the smallest valid value. See the example in [Rule: Thresholds Hold the Heartbeats](/horsies/workers/heartbeats-recovery/#rule-thresholds-hold-the-heartbeats).

## Retention

Retention moved to its own configuration section:
[`AppConfig.retention`](/horsies/configuration/retention-config/). It decides how long records
live and how their storage is prepared and reclaimed, which is a
different question from the one this page answers — recovery is about
work that went wrong, retention about work that went right and is now
history.

Setting a retention field on `RecoveryConfig` fails at construction and
names its new home; there is no alias.

## Automatic and Manual Passes

The reaper of each worker runs the recovery passes below. Each pass has a method that runs it once, on demand. Three of the passes also have a setting that turns the automatic pass on or off. The method runs the pass also when that setting is off.

| Pass | Setting (default `True`) | Automatic interval | Method of `PostgresBroker` (Python) |
|------|--------------------------|--------------------|-------------------------------------|
| Stale CLAIMED tasks go back to PENDING | `auto_requeue_stale_claimed` | `check_interval_ms` | `requeue_stale_claimed()` |
| Stale RUNNING tasks are retried or failed | `auto_fail_stale_running` | `check_interval_ms` | `mark_stale_tasks_as_failed()` |
| One page of orphan workflow tasks is cancelled | `auto_terminate_orphaned_workflow_tasks` | `orphan_task_audit_interval_ms` (default 60,000) | `terminate_orphaned_workflow_tasks()` |
| PENDING tasks past their `good_until` expire | None; the reaper always runs this pass | `check_interval_ms` | `expire_pending_tasks()` |

[Broker Methods](/horsies/monitoring/broker-methods/#methods) documents `requeue_stale_claimed`, `mark_stale_tasks_as_failed` and `terminate_orphaned_workflow_tasks`. The Rust client has no method for these passes.

## Disabling Recovery

To disable automatic recovery (not recommended), set `auto_requeue_stale_claimed` and `auto_fail_stale_running` to false. See [Disable Recovery](/horsies/configuration/app-config/#disable-recovery) (Python).

Tasks will remain stuck until manually resolved, for example with the methods of [Automatic and Manual Passes](#automatic-and-manual-passes).

## Manual Recovery

See [Manual Recovery](/horsies/workers/heartbeats-recovery/#manual-recovery).
