//! The statements of the fleet read, except the class coverage, which is
//! the fleet check statement of `crate::broker::class_coverage_reads`.
//!
//! Each bounded part reads one row past its limit, so a part at its limit
//! and a part past it are told apart.

use crate::broker::class_coverage_reads::{
    LIVE_SNAPSHOT_READ_LIMIT, LIVE_WINDOW_SECONDS, PENDING_HEAD_READ_LIMIT,
    PENDING_STREAM_READ_LIMIT,
};
use crate::db::{BoundStatement, ParameterType, TypedStatement};

use super::{HELD_GROUP_LIMIT, PENDING_COUNT_LIMIT, SCHEDULE_LIMIT};

const PENDING_COUNT_READ_LIMIT: i64 = PENDING_COUNT_LIMIT as i64 + 1;
const HELD_GROUP_READ_LIMIT: i64 = HELD_GROUP_LIMIT as i64 + 1;
const SCHEDULE_READ_LIMIT: i64 = SCHEDULE_LIMIT as i64 + 1;

/// The live rows per queue, class and status. `$1` the stream limit + 1,
/// `$2` the count limit + 1, `$3` the held group limit + 1.
///
/// - `streams`: each distinct (queue, class) of the `PENDING` rows, a skip
///   scan with one probe of `horsies_tasks_claim_pending` per stream, as the
///   class coverage read. The `LIMIT` of `stream_page` ends the recursion.
/// - `pending`: per stream, the count of its `PENDING` rows, read on
///   `horsies_tasks_claim_pending` with the stream as the index prefix and
///   stopped at `$2` rows.
/// - `held`: the `CLAIMED` and `RUNNING` rows per (status, queue, class),
///   read through `horsies_tasks_status`. Workers hold these rows, so their
///   count is bounded by the fleet's seats, prefetch buffers and expired
///   leases.
pub(super) const QUEUE_TASKS_SQL: &str = "\
WITH RECURSIVE streams AS (
    (
        SELECT t.queue_name, t.execution
        FROM horsies_tasks t
        WHERE t.status = 'PENDING'
        ORDER BY t.queue_name, t.execution
        LIMIT 1
    )
    UNION ALL
    SELECT nxt.queue_name, nxt.execution
    FROM streams s
    CROSS JOIN LATERAL (
        SELECT t.queue_name, t.execution
        FROM horsies_tasks t
        WHERE t.status = 'PENDING'
          AND (t.queue_name, t.execution) > (s.queue_name, s.execution)
        ORDER BY t.queue_name, t.execution
        LIMIT 1
    ) nxt
),
stream_page AS (
    SELECT p.queue_name, p.execution FROM streams p LIMIT $1
),
pending AS (
    SELECT p.queue_name, p.execution,
           (SELECT count(*)
            FROM (SELECT 1 FROM horsies_tasks t
                  WHERE t.status = 'PENDING'
                    AND t.queue_name = p.queue_name
                    AND t.execution = p.execution
                  LIMIT $2) AS counted) AS task_count
    FROM stream_page p
),
held AS (
    SELECT t.status, t.queue_name, t.execution, count(*) AS task_count
    FROM horsies_tasks t
    WHERE t.status IN ('CLAIMED', 'RUNNING')
    GROUP BY t.status, t.queue_name, t.execution
    LIMIT $3
)
SELECT
    (SELECT coalesce(jsonb_agg(jsonb_build_array(p.queue_name, p.execution, p.task_count)), '[]'::jsonb)
     FROM pending p) AS pending,
    (SELECT coalesce(jsonb_agg(jsonb_build_array(h.status, h.queue_name, h.execution, h.task_count)),
                     '[]'::jsonb)
     FROM held h) AS held";

/// The wait of the oldest due row of each pending (queue, class). `$1` the
/// head limit + 1.
///
/// `heads`: the head of each (queue, class, priority) of the `PENDING` rows,
/// as the class report (`CLASS_REPORT_SQL`): the first index entry of the
/// group is its earliest `enqueued_at`. One probe of
/// `horsies_tasks_claim_pending` per group. The `LIMIT` of `head_page` ends
/// the recursion.
pub(super) const OLDEST_PENDING_SQL: &str = "\
WITH RECURSIVE heads AS (
    (
        SELECT t.queue_name, t.execution, t.priority, t.enqueued_at
        FROM horsies_tasks t
        WHERE t.status = 'PENDING'
        ORDER BY t.queue_name, t.execution, t.priority, t.enqueued_at, t.id
        LIMIT 1
    )
    UNION ALL
    SELECT nxt.queue_name, nxt.execution, nxt.priority, nxt.enqueued_at
    FROM heads h
    CROSS JOIN LATERAL (
        SELECT t.queue_name, t.execution, t.priority, t.enqueued_at
        FROM horsies_tasks t
        WHERE t.status = 'PENDING'
          AND (t.queue_name, t.execution, t.priority) > (h.queue_name, h.execution, h.priority)
        ORDER BY t.queue_name, t.execution, t.priority, t.enqueued_at, t.id
        LIMIT 1
    ) nxt
),
head_page AS (
    SELECT p.queue_name, p.execution, p.enqueued_at FROM heads p LIMIT $1
)
SELECT
    now() AS checked_at,
    (SELECT coalesce(jsonb_agg(jsonb_build_array(p.queue_name, p.execution, p.enqueued_at)),
                     '[]'::jsonb)
     FROM head_page p) AS heads";

/// The live workers, their slot health and the rows they hold. `$1` the
/// live window in seconds, `$2` the snapshot row limit + 1.
///
/// - `recent`, `live`: the latest snapshot of each worker inside the live
///   window, as the fleet check (`CLASS_COVERAGE_SQL`), a range of
///   `horsies_worker_states_snapshot_at`; each live entry also holds the
///   snapshot's `processes` and `slots_down`.
/// - `in_flight`: the rows each live worker holds, per class, as the claim
///   counts them (`RUNNING`, and `CLAIMED` with a live lease), as the class
///   report; read through `horsies_tasks_status`.
pub(super) const WORKERS_SQL: &str = "\
WITH recent AS (
    SELECT w.worker_id, w.snapshot_at, w.queues, w.class_capacity, w.processes, w.slots_down
    FROM horsies_worker_states w
    WHERE w.snapshot_at >= now() - make_interval(secs => $1::double precision)
    ORDER BY w.snapshot_at DESC
    LIMIT $2
),
live AS (
    SELECT DISTINCT ON (r.worker_id) r.worker_id, r.snapshot_at, r.queues, r.class_capacity,
           r.processes, r.slots_down
    FROM recent r
    ORDER BY r.worker_id, r.snapshot_at DESC
),
in_flight AS (
    SELECT t.claimed_by_worker_id AS worker_id, t.execution, count(*) AS held
    FROM horsies_tasks t
    WHERE t.status IN ('CLAIMED', 'RUNNING')
      AND t.claimed_by_worker_id IN (SELECT l.worker_id FROM live l)
      AND (t.status = 'RUNNING' OR t.claim_expires_at > now())
    GROUP BY t.claimed_by_worker_id, t.execution
)
SELECT
    now() AS checked_at,
    (SELECT count(*) FROM recent) AS recent_rows,
    (SELECT coalesce(jsonb_agg(jsonb_build_object(
        'worker_id', l.worker_id,
        'snapshot_at', l.snapshot_at,
        'queues', to_jsonb(l.queues),
        'class_capacity', l.class_capacity,
        'processes', l.processes,
        'slots_down', l.slots_down)), '[]'::jsonb)
     FROM live l) AS live,
    (SELECT coalesce(jsonb_agg(jsonb_build_array(f.worker_id, f.execution, f.held)), '[]'::jsonb)
     FROM in_flight f) AS in_flight";

/// The `RUNNING` and `PAUSED` workflows, each counted on its partial index
/// (`idx_horsies_workflows_running_recovery_scan`,
/// `idx_horsies_workflows_paused_age`).
pub(super) const WORKFLOWS_SQL: &str = "\
SELECT (SELECT count(*) FROM horsies_workflows w WHERE w.status = 'RUNNING') AS running,
       (SELECT count(*) FROM horsies_workflows w WHERE w.status = 'PAUSED') AS paused";

/// The last run of each schedule in name order, on the primary key of
/// `horsies_schedule_state`. `$1` the schedule limit + 1.
pub(super) const SCHEDULES_SQL: &str = "\
SELECT now() AS checked_at,
       (SELECT coalesce(jsonb_agg(jsonb_build_array(s.schedule_name, s.last_run_at)), '[]'::jsonb)
        FROM (SELECT st.schedule_name, st.last_run_at
              FROM horsies_schedule_state st
              ORDER BY st.schedule_name
              LIMIT $1) AS s) AS schedules";

/// The newest successful migration, on the primary key of
/// `horsies_migrations`. No row when none is recorded.
pub(super) const SCHEMA_VERSION_SQL: &str = "\
SELECT m.version
FROM horsies_migrations m
WHERE m.success
ORDER BY m.version DESC
LIMIT 1";

pub(super) static QUEUE_TASKS: TypedStatement = TypedStatement::new(
    "fleet_queue_tasks",
    QUEUE_TASKS_SQL,
    &[
        ParameterType::Int8,
        ParameterType::Int8,
        ParameterType::Int8,
    ],
);

pub(super) static OLDEST_PENDING: TypedStatement = TypedStatement::new(
    "fleet_oldest_pending",
    OLDEST_PENDING_SQL,
    &[ParameterType::Int8],
);

pub(super) static WORKERS: TypedStatement = TypedStatement::new(
    "fleet_workers",
    WORKERS_SQL,
    &[ParameterType::Float8, ParameterType::Int8],
);

pub(super) static WORKFLOWS: TypedStatement =
    TypedStatement::new("fleet_workflows", WORKFLOWS_SQL, &[]);

pub(super) static SCHEDULES: TypedStatement =
    TypedStatement::new("fleet_schedules", SCHEDULES_SQL, &[ParameterType::Int8]);

pub(super) static SCHEMA_VERSION: TypedStatement =
    TypedStatement::new("fleet_schema_version", SCHEMA_VERSION_SQL, &[]);

/// [`QUEUE_TASKS_SQL`] with its three limits. Pure.
pub(super) fn queue_tasks_statement() -> BoundStatement<'static> {
    QUEUE_TASKS
        .bind()
        .value(&PENDING_STREAM_READ_LIMIT)
        .value(&PENDING_COUNT_READ_LIMIT)
        .value(&HELD_GROUP_READ_LIMIT)
        .finish()
}

/// [`OLDEST_PENDING_SQL`] with its limit. Pure.
pub(super) fn oldest_pending_statement() -> BoundStatement<'static> {
    OLDEST_PENDING
        .bind()
        .value(&PENDING_HEAD_READ_LIMIT)
        .finish()
}

/// [`WORKERS_SQL`] with the live window and its limit. Pure.
pub(super) fn workers_statement() -> BoundStatement<'static> {
    WORKERS
        .bind()
        .value(&LIVE_WINDOW_SECONDS)
        .value(&LIVE_SNAPSHOT_READ_LIMIT)
        .finish()
}

/// [`WORKFLOWS_SQL`]; it takes no parameter. Pure.
pub(super) fn workflows_statement() -> BoundStatement<'static> {
    WORKFLOWS.bind().finish()
}

/// [`SCHEDULES_SQL`] with its limit. Pure.
pub(super) fn schedules_statement() -> BoundStatement<'static> {
    SCHEDULES.bind().value(&SCHEDULE_READ_LIMIT).finish()
}

/// [`SCHEMA_VERSION_SQL`]; it takes no parameter. Pure.
pub(super) fn schema_version_statement() -> BoundStatement<'static> {
    SCHEMA_VERSION.bind().finish()
}
