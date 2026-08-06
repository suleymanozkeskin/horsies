---
title: Database Schema
summary: PostgreSQL tables for tasks, heartbeats, worker states, and schedules.
related: [../../configuration/broker-config]
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
| `status` | VARCHAR | PENDING/CLAIMED/RUNNING/COMPLETED/FAILED/CANCELLED/EXPIRED (stored as VARCHAR) |

These values correspond to `TaskStatus` in the API (see Task Lifecycle for terminal states).

| Column | Type | Description |
| ------ | ---- | ----------- |
| `sent_at` | TIMESTAMP | Immutable call-site timestamp (when `.send()` / `.schedule()` was called) |
| `enqueued_at` | TIMESTAMP | Mutable dispatch timestamp (when task becomes claimable; updated on retry) |
| `claimed_at` | TIMESTAMP | When worker claimed task |
| `started_at` | TIMESTAMP | When execution started |
| `completed_at` | TIMESTAMP | When task completed |
| `failed_at` | TIMESTAMP | When task failed |
| `result` | TEXT | JSON-serialized TaskResult |
| `failed_reason` | TEXT | Human-readable failure message |
| `error_code` | TEXT | Final `TaskError.error_code` for failed tasks (NULL for successful, non-terminal, or worker-level failures without a `TaskResult`) |
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
| `enqueue_sha` | VARCHAR(64) | SHA-256 digest for idempotent retry |
| `created_at` | TIMESTAMP | Row creation time |
| `updated_at` | TIMESTAMP | Last update time |

Indexes: `queue_name`, `status`, `claimed`, `good_until`, `next_retry_at`, `error_code` (partial, WHERE NOT NULL)

## horsies_task_attempts

Immutable per-attempt execution history. One row per finished execution attempt (success, failure, or worker crash). Written atomically in the same transaction as the task state transition.

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

Worker monitoring snapshots (timeseries).

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | INT PK | Auto-increment |
| `worker_id` | VARCHAR(255) | Worker identifier |
| `snapshot_at` | TIMESTAMP | Snapshot time |
| `hostname` | VARCHAR(255) | Machine hostname |
| `pid` | INT | Main process ID |
| `processes` | INT | Worker concurrency level |
| `queues` | VARCHAR[] | Subscribed queues |
| `tasks_running` | INT | Current running count |
| `tasks_claimed` | INT | Current claimed count |
| `memory_usage_mb` | FLOAT | Memory consumption |
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

## horsies_workflows

Workflow instance rows.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | VARCHAR(36) PK | Workflow UUID |
| `name` | VARCHAR(255) | Workflow name |
| `status` | VARCHAR | `RUNNING`, `COMPLETED`, `FAILED`, `PAUSED`, `CANCELLED` |
| `on_error` | VARCHAR | `FAIL` or `PAUSE` |
| `output_task_index` | INT | Output node index |
| `success_policy` | TEXT | Serialized success policy JSON |
| `definition_key` | VARCHAR(255) | Stable workflow definition key |
| `parent_workflow_id` | VARCHAR(36) | Parent workflow if this is a child |
| `parent_task_index` | INT | Parent workflow task index for child workflows |
| `depth` | INT | Workflow nesting depth |
| `root_workflow_id` | VARCHAR(36) | Root workflow ID |
| `sent_at` | TIMESTAMP | Workflow creation timestamp |
| `started_at` | TIMESTAMP | When workflow entered running state |
| `completed_at` | TIMESTAMP | Completion timestamp |
| `updated_at` | TIMESTAMP | Last update time |
| `created_at` | TIMESTAMP | Row creation time |

## horsies_workflow_tasks

Per-node state for workflow execution.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `id` | VARCHAR(36) PK | Row identifier |
| `workflow_id` | VARCHAR(36) FK | Parent workflow |
| `task_index` | INT | Node index within the workflow |
| `node_id` | VARCHAR(255) | Stable node identifier |
| `task_name` | VARCHAR(255) | Registered task name or `__sub_workflow:*` marker |
| `task_args` | TEXT | Serialized positional input JSON |
| `task_kwargs` | TEXT | Serialized keyword input JSON |
| `queue_name` | VARCHAR(100) | Resolved queue |
| `priority` | INT | Resolved priority |
| `dependencies` | INT[] | Upstream dependency indices |
| `args_from` | JSONB | Field-name to dependency-index mapping |
| `workflow_ctx_from` | TEXT[] | Node IDs used for workflow context |
| `allow_failed_deps` | BOOLEAN | Whether downstream runs after upstream failure |
| `join_type` | VARCHAR | `ALL`, `ANY`, `QUORUM` |
| `min_success` | INT | Quorum threshold |
| `task_options` | TEXT | Serialized task options JSON |
| `status` | VARCHAR | `PENDING`, `READY`, `ENQUEUED`, `RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED` |
| `is_subworkflow` | BOOLEAN | Whether this node is a child workflow |
| `sub_workflow_name` | VARCHAR(255) | Child workflow name if subworkflow |
| `sub_definition_key` | VARCHAR(255) | Child workflow definition key if subworkflow |
| `task_id` | VARCHAR(36) | Linked row in `horsies_tasks` for task nodes |
| `sub_workflow_id` | VARCHAR(36) | Linked row in `horsies_workflows` for subworkflow nodes |
| `started_at` | TIMESTAMP | When the node was enqueued/launched |
| `completed_at` | TIMESTAMP | Completion timestamp |
| `created_at` | TIMESTAMP | Row creation time |
| `updated_at` | TIMESTAMP | Last update time |

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

## Schema Creation

Schema is managed through embedded SQL migrations. High-level `Horsies` entry points ensure the schema automatically on first use.

```rust
let broker = PostgresBroker::connect_with(config).await?;
broker.ensure_schema_initialized().await?;
```

Applications already using `Horsies` typically do not need a separate migration step:

```rust
let broker = app.get_broker().await?;
// schema already ensured by the high-level app path
broker.health_check().await?;
```

## Automatic Retention Cleanup

The worker's reaper loop prunes old rows automatically every `retention_sweep_interval_s` seconds (default 5 minutes) based on [RecoveryConfig](../../configuration/recovery-config) retention settings:

| Table | Config field | Default | Condition |
|-------|-------------|---------|-----------|
| `horsies_heartbeats` | `heartbeat_retention_hours` | 24h | `sent_at` older than threshold |
| `horsies_worker_states` | `worker_state_retention_hours` | 7 days | `snapshot_at` older than threshold |
| `horsies_tasks` | `terminal_record_retention_hours` | 30 days | Terminal status + oldest timestamp older than threshold; plain tasks on queues listed in `queue_terminal_record_retention_hours` use their per-queue window instead |
| `horsies_task_attempts` | -- | -- | Purged set-wise in the same statement that deletes the parent task rows (the FK cascade remains as the net for non-retention deletes) |
| `horsies_workflows` | `terminal_record_retention_hours` | 30 days | Terminal status + oldest timestamp older than threshold, and no live backing task |
| `horsies_workflow_tasks` | `terminal_record_retention_hours` | 30 days | Purged set-wise in the same statement that deletes the parent workflow rows (the FK cascade remains as the net for non-retention deletes) |

Terminal statuses: COMPLETED, FAILED, CANCELLED, EXPIRED. Set any retention field to `None` to disable cleanup for that category. See [Recovery Config](../../configuration/recovery-config#retention-cleanup) for configuration details.

## File Location

`horsies/src/broker/postgres.rs` (schema creation and queries)
