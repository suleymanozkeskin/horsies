---
title: Database Schema
summary: PostgreSQL tables for tasks, heartbeats, worker states, and schedules.
related: [postgres-broker, operational-indexes, ../../configuration/broker-config]
tags: [internals, database, schema, PostgreSQL]
---

## horsies_tasks

Primary task storage table.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | VARCHAR(36) PK | UUID task identifier |
| `task_name` | VARCHAR(255) | Registered task name |
| `queue_name` | VARCHAR(100) | Queue assignment |
| `priority` | INT | 1-100, lower = higher priority |
| `args` | TEXT | JSON-serialized positional args |
| `kwargs` | TEXT | JSON-serialized keyword args |
| `status` | VARCHAR | PENDING/CLAIMED/RUNNING/COMPLETED/FAILED/CANCELLED/EXPIRED (stored as VARCHAR via `native_enum=False`) |

These values correspond to `TaskStatus` in the API (see Task Lifecycle for terminal states).
| `sent_at` | TIMESTAMP | Immutable call-site timestamp (when `.send()`/`.schedule()` was called) |
| `enqueued_at` | TIMESTAMP | Mutable dispatch timestamp (when task becomes claimable; updated on retry) |
| `is_workflow_task` | BOOLEAN | Whether the task row belongs to a workflow node; used to skip workflow-only checks on plain tasks |
| `claimed_at` | TIMESTAMP | When worker claimed task |
| `started_at` | TIMESTAMP | When execution started |
| `completed_at` | TIMESTAMP | When task completed |
| `failed_at` | TIMESTAMP | When task failed |
| `result` | TEXT | JSON-serialized TaskResult |
| `failed_reason` | TEXT | Human-readable failure message |
| `error_code` | TEXT | Final `TaskError.error_code` for failed tasks (NULL for successful and non-terminal tasks) |
| `claimed` | BOOLEAN | Claiming flag |
| `claimed_by_worker_id` | VARCHAR(255) | Worker identifier |
| `good_until` | TIMESTAMP | Task expiry deadline |
| `retry_count` | INT | Current retry attempt |
| `max_retries` | INT | Maximum retries allowed |
| `next_retry_at` | TIMESTAMP | Next retry time |
| `task_options` | TEXT | Serialized TaskOptions |
| `worker_pid` | INT | Executing process ID |
| `worker_hostname` | VARCHAR(255) | Executing machine |
| `worker_process_name` | VARCHAR(255) | Process identifier |
| `claim_expires_at` | TIMESTAMP | Claim lease expiry deadline |
| `finalizing_at` | TIMESTAMPTZ | Child has finished user code and parent finalization is in progress |
| `finalizing_by_worker_id` | TEXT | Worker responsible for parent finalization |
| `enqueue_sha` | VARCHAR(64) | SHA-256 digest for idempotent retry |
| `created_at` | TIMESTAMP | Row creation time |
| `updated_at` | TIMESTAMP | Last update time |

Indexes: `queue_name`, `status`, `claimed`, `good_until`, `next_retry_at`, `error_code` (partial, WHERE NOT NULL)

## horsies_task_attempts

Immutable per-attempt execution history. One row per finished execution attempt (success, failure, or worker crash). Written atomically in the same transaction as the task state transition.

Both `error_code` and attempt history are only guaranteed from deployment of the schema changes in version `0.1.0a26` onward. No historical backfill is performed.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | BIGSERIAL PK | Auto-increment |
| `task_id` | VARCHAR(36) FK | References `horsies_tasks(id)` with `ON DELETE CASCADE` |
| `attempt` | INT | 1-based attempt number (`retry_count + 1` at finalization time) |
| `outcome` | VARCHAR(32) | `COMPLETED`, `FAILED`, or `WORKER_FAILURE` (CHECK constraint enforced) |
| `will_retry` | BOOLEAN | Whether a retry was scheduled after this attempt |
| `started_at` | TIMESTAMPTZ | When the attempt started running |
| `finished_at` | TIMESTAMPTZ | When the attempt finished |
| `error_code` | TEXT | `TaskError.error_code` for this attempt (NULL on success) |
| `error_message` | TEXT | `TaskError.message` for this attempt (NULL on success) |
| `failed_reason` | TEXT | Worker-level failure reason (NULL for domain errors and successes) |
| `worker_id` | VARCHAR(255) | Worker that executed this attempt |
| `worker_hostname` | VARCHAR(255) | Machine hostname |
| `worker_pid` | INT | Process ID |
| `worker_process_name` | VARCHAR(255) | Process identifier |
| `created_at` | TIMESTAMPTZ | Row creation time |

Constraints: `UNIQUE (task_id, attempt)`, `CHECK (outcome IN ('COMPLETED', 'FAILED', 'WORKER_FAILURE'))`

Indexes: `error_code` (partial, WHERE NOT NULL), `finished_at DESC`

Pre-execution aborts (`CLAIM_LOST`, `OWNERSHIP_UNCONFIRMED`, `WORKFLOW_CHECK_FAILED`, `WORKFLOW_STOPPED`) do not create attempt rows.

## horsies_heartbeats

Task liveness tracking.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | INT PK | Auto-increment |
| `task_id` | VARCHAR(36) | Associated task |
| `sender_id` | VARCHAR(255) | Worker/process ID |
| `role` | VARCHAR(20) | 'claimer' or 'runner' |
| `sent_at` | TIMESTAMP | Heartbeat time |
| `hostname` | VARCHAR(255) | Machine hostname |
| `pid` | INT | Process ID |

Indexes: `(task_id, role, sent_at DESC)`

## horsies_worker_states

Worker monitoring snapshots (timeseries). Each worker inserts one row per
`worker_state_snapshot_interval_ms` (default 30s).

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | INT PK | Auto-increment |
| `worker_id` | VARCHAR(255) | Worker identifier |
| `snapshot_at` | TIMESTAMP | Snapshot time |
| `hostname` | VARCHAR(255) | Machine hostname |
| `pid` | INT | Main process ID |
| `processes` | INT | Worker process count |
| `queues` | VARCHAR[] | Subscribed queues |
| `tasks_running` | INT | Current running count |
| `tasks_claimed` | INT | Current claimed count |
| `memory_usage_mb` | FLOAT | Parent worker process resident memory |
| `children_memory_mb` | FLOAT | Summed resident memory of executor child processes |
| `cpu_percent` | FLOAT | CPU usage |
| `memory_percent` | FLOAT | Memory usage percentage |
| `max_claim_batch` | INT | Max tasks claimed per batch |
| `max_claim_per_worker` | INT | Max tasks claimable per worker |
| `cluster_wide_cap` | INT | Cluster-wide in-flight cap |
| `queue_priorities` | JSONB | Queue priority configuration |
| `queue_max_concurrency` | JSONB | Per-queue concurrency limits |
| `recovery_config` | JSONB | Recovery configuration snapshot |
| `worker_started_at` | TIMESTAMP | Worker start time |

## horsies_schedule_state

Scheduler execution tracking.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `schedule_name` | VARCHAR(255) PK | Schedule identifier |
| `last_run_at` | TIMESTAMP | Last execution time |
| `next_run_at` | TIMESTAMP | Next scheduled time |
| `last_task_id` | VARCHAR(36) | Most recent task ID |
| `run_count` | INT | Total executions |
| `config_hash` | VARCHAR(64) | Configuration hash |
| `updated_at` | TIMESTAMP | Last state update |

## Trigger

```sql
CREATE FUNCTION horsies_notify_task_changes()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.status = 'PENDING' THEN
        PERFORM pg_notify('task_new', NEW.id);
        PERFORM pg_notify('task_queue_' || NEW.queue_name, NEW.id);
    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
            PERFORM pg_notify('task_done', NEW.id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER horsies_task_notify_trigger
    AFTER INSERT OR UPDATE ON horsies_tasks
    FOR EACH ROW
    EXECUTE FUNCTION horsies_notify_task_changes();
```

## Trust Boundary

Treat **database write access as task-execution privilege**: anyone who
can INSERT into `horsies_tasks` can run any registered task with any
kwargs that pass its typed signature, and can read stored results and
tracebacks. There is no separate authentication layer between the
tables and the workers (the same model as Celery with the JSON
serializer). Task names resolve only against the in-process registry —
rows cannot trigger arbitrary imports or code loading — but scope DB
credentials accordingly.

## Schema Creation

Tables created automatically via SQLAlchemy's `Base.metadata.create_all()`:

```python
await conn.run_sync(Base.metadata.create_all)
```

Protected by advisory lock to prevent race conditions.

## Automatic Retention Cleanup

The worker's reaper loop prunes old rows automatically every `retention_sweep_interval_s` seconds (default 5 minutes) based on [RecoveryConfig](../../configuration/recovery-config) retention settings:

| Table | Config field | Default | Condition |
|-------|-------------|---------|-----------|
| `horsies_heartbeats` | `heartbeat_retention_hours` | 24h | `sent_at` older than threshold |
| `horsies_worker_states` | `worker_state_retention_hours` | 7 days | `snapshot_at` older than threshold |
| `horsies_tasks` | `terminal_record_retention_hours` | 30 days | Terminal status + oldest timestamp older than threshold; plain tasks on queues listed in `queue_terminal_record_retention_hours` use their queue's window instead |
| `horsies_task_attempts` | — | — | Purged set-wise in the same statement that deletes the parent task rows (the FK cascade remains as the correctness net for non-retention deletes) |
| `horsies_workflows` | `terminal_record_retention_hours` | 30 days | Terminal status + oldest timestamp older than threshold |
| `horsies_workflow_tasks` | `terminal_record_retention_hours` | 30 days | Parent workflow is terminal and older than threshold |

Terminal statuses: COMPLETED, FAILED, CANCELLED, EXPIRED. Set any retention field to `None` to disable cleanup for that category. See [Recovery Config](../../configuration/recovery-config#retention-cleanup) for configuration details.

Deletes run in batches of `retention_delete_batch_size` rows (default 500), one transaction per batch, under a 60-second budget per pass. A backlog larger than the budget allows — for example the first pass after enabling retention on a long-running database — drains across consecutive passes instead of running as one unbounded DELETE.

## File Location

`horsies/core/models/task_pg.py`
