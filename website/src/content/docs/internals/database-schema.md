---
title: Database Schema
summary: PostgreSQL tables for tasks, heartbeats, worker states, and schedules.
related: [postgres-broker, ../../configuration/broker-config]
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
| `status` | ENUM | PENDING/CLAIMED/RUNNING/COMPLETED/FAILED/CANCELLED/REQUEUED |

These values correspond to `TaskStatus` in the API (see Task Lifecycle for terminal states).
| `sent_at` | TIMESTAMP | When task was enqueued |
| `claimed_at` | TIMESTAMP | When worker claimed task |
| `started_at` | TIMESTAMP | When execution started |
| `completed_at` | TIMESTAMP | When task completed |
| `failed_at` | TIMESTAMP | When task failed |
| `result` | TEXT | JSON-serialized TaskResult |
| `failed_reason` | TEXT | Human-readable failure message |
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
| `created_at` | TIMESTAMP | Row creation time |
| `updated_at` | TIMESTAMP | Last update time |

Indexes: `queue_name`, `status`, `claimed`, `good_until`, `next_retry_at`

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

## horsies_worker_states

Worker monitoring snapshots (timeseries).

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
| `memory_usage_mb` | FLOAT | Memory consumption |
| `cpu_percent` | FLOAT | CPU usage |
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
        IF NEW.status IN ('COMPLETED', 'FAILED') THEN
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

Tables created automatically via SQLAlchemy's `Base.metadata.create_all()`:

```python
await conn.run_sync(Base.metadata.create_all)
```

Protected by advisory lock to prevent race conditions.

## Maintenance

### Heartbeat Cleanup

```sql
DELETE FROM horsies_heartbeats WHERE sent_at < NOW() - INTERVAL '24 hours';
```

### Worker State Cleanup

```sql
DELETE FROM horsies_worker_states WHERE snapshot_at < NOW() - INTERVAL '7 days';
```

### Completed Task Archival

```sql
-- Move old completed tasks to archive
INSERT INTO horsies_tasks_archive SELECT * FROM horsies_tasks
WHERE status IN ('COMPLETED', 'FAILED')
  AND completed_at < NOW() - INTERVAL '30 days';

DELETE FROM horsies_tasks
WHERE status IN ('COMPLETED', 'FAILED')
  AND completed_at < NOW() - INTERVAL '30 days';
```

## File Location

`horsies/core/models/task_pg.py`
