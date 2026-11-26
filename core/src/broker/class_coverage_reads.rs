//! Database reads for the execution classes of the fleet: the fleet check
//! ([`PostgresBroker::get_class_coverage`]) and the class report
//! ([`PostgresBroker::get_class_report`]).
//!
//! Each read is one autocommit statement, so its parts read one snapshot and
//! one `now()`.
//! Both reads run on the main core pool; the reaper's fleet check
//! ([`read_class_coverage`]) runs the same statement there as reserved work.
//! The fleet state read (`crate::monitoring::fleet`) runs the fleet check
//! statement ([`class_coverage_statement`]) with its own time bound, and
//! decodes its reads of the live snapshots, the rows in flight and the
//! pending heads with the part decoders of this module.
//! The parts come back as JSON arrays in one row. Every part is bounded by a
//! named limit; a part that returns more than its limit refuses the read
//! instead of answering in part.
//!
//! Index use: the pending streams and the pending heads are skip scans of
//! `horsies_tasks_claim_pending`, one probe per (queue, class) or per
//! (queue, class, priority); the live snapshots are a range of
//! `horsies_worker_states_snapshot_at`; the in-flight rows are read through
//! `horsies_tasks_status`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;

use crate::broker::class_coverage::{
    class_coverage, class_report, live_worker, refused, ClassCoverage, ClassReadRefused,
    ClassReport, LiveWorker, StreamHead,
};
use crate::broker::core_rows::{column, one_row, RowDecodeError};
use crate::broker::error::BrokerError;
use crate::broker::postgres::PostgresBroker;
use crate::broker::result_types::{DatabaseOperationError, BrokerOperationSource, DatabaseResult};
use crate::core::defaults::WORKER_STATE_LIVE_WINDOW_S;
use crate::core::task::Execution;
use crate::db::{
    BoundStatement, CheckoutClass, ParameterType, Row, StatementBound, TypedStatement,
};

/// The most snapshot rows inside the live window that a read takes. A worker
/// writes `WORKER_STATE_LIVE_WINDOW_S` / snapshot interval rows into the
/// window: 4 at the default 30 s, 120 at the 1 s floor.
pub const LIVE_SNAPSHOT_ROW_LIMIT: usize = 20_000;

/// The most (queue, class) streams with `PENDING` rows that a read takes:
/// 1,000 queues with all three classes.
pub const PENDING_STREAM_LIMIT: usize = 3_000;

/// The most (queue, class, priority) groups of `PENDING` rows that the report
/// reads. A queue priority is 1 to 100, so one stream has at most 100.
pub const PENDING_HEAD_LIMIT: usize = 10_000;

/// The live window in seconds, as the statements take it (`$1`).
pub(crate) const LIVE_WINDOW_SECONDS: f64 = WORKER_STATE_LIVE_WINDOW_S as f64;

/// Each bounded part reads one row past its limit, so a part at its limit
/// and a part past it are told apart.
pub(crate) const LIVE_SNAPSHOT_READ_LIMIT: i64 = LIVE_SNAPSHOT_ROW_LIMIT as i64 + 1;
pub(crate) const PENDING_STREAM_READ_LIMIT: i64 = PENDING_STREAM_LIMIT as i64 + 1;
pub(crate) const PENDING_HEAD_READ_LIMIT: i64 = PENDING_HEAD_LIMIT as i64 + 1;

/// The fleet check in one statement. `$1` the live window in seconds, `$2`
/// the snapshot row limit + 1, `$3` the stream limit + 1.
///
/// - `recent`: the snapshot rows inside the live window, newest first, on
///   `horsies_worker_states_snapshot_at` (Python `_LIVE_WORKERS_SQL`).
/// - `live`: the latest of those rows per worker.
/// - `streams`: each distinct (queue, class) of the `PENDING` rows, a skip
///   scan with one probe of `horsies_tasks_claim_pending` per stream (Python
///   `_PENDING_STREAMS_SQL`). The `LIMIT` of `stream_page` ends the
///   recursion.
pub(crate) const CLASS_COVERAGE_SQL: &str = "\
WITH RECURSIVE recent AS (
    SELECT w.worker_id, w.snapshot_at, w.queues, w.class_capacity
    FROM horsies_worker_states w
    WHERE w.snapshot_at >= now() - make_interval(secs => $1::double precision)
    ORDER BY w.snapshot_at DESC
    LIMIT $2
),
live AS (
    SELECT DISTINCT ON (r.worker_id) r.worker_id, r.snapshot_at, r.queues, r.class_capacity
    FROM recent r
    ORDER BY r.worker_id, r.snapshot_at DESC
),
streams AS (
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
    SELECT p.queue_name, p.execution FROM streams p LIMIT $3
)
SELECT
    now() AS checked_at,
    (SELECT count(*) FROM recent) AS recent_rows,
    (SELECT coalesce(jsonb_agg(jsonb_build_object(
        'worker_id', l.worker_id,
        'snapshot_at', l.snapshot_at,
        'queues', to_jsonb(l.queues),
        'class_capacity', l.class_capacity)), '[]'::jsonb)
     FROM live l) AS live,
    (SELECT coalesce(jsonb_agg(jsonb_build_array(p.queue_name, p.execution)), '[]'::jsonb)
     FROM stream_page p) AS streams";

/// The class report in one statement. `$1` the live window in seconds, `$2`
/// the snapshot row limit + 1, `$3` the head limit + 1.
///
/// - `recent`, `live`: as [`CLASS_COVERAGE_SQL`].
/// - `heads`: the head of each (queue, class, priority) of the `PENDING`
///   rows: the first index entry of the group is its earliest `enqueued_at`.
///   One probe of `horsies_tasks_claim_pending` per group (Python
///   `_PENDING_HEADS_SQL`). The `LIMIT` of `head_page` ends the recursion.
/// - `in_flight`: the rows each live worker holds, per class, as the claim
///   counts them: `RUNNING`, and `CLAIMED` with a live lease (Python
///   `_IN_FLIGHT_SQL`). The `IN` form reads `horsies_tasks_status`. A
///   `CLAIMED` row always has a lease (`horsies_tasks_status_live`), so the
///   test needs no `claim_expires_at IS NULL` arm.
pub(crate) const CLASS_REPORT_SQL: &str = "\
WITH RECURSIVE recent AS (
    SELECT w.worker_id, w.snapshot_at, w.queues, w.class_capacity
    FROM horsies_worker_states w
    WHERE w.snapshot_at >= now() - make_interval(secs => $1::double precision)
    ORDER BY w.snapshot_at DESC
    LIMIT $2
),
live AS (
    SELECT DISTINCT ON (r.worker_id) r.worker_id, r.snapshot_at, r.queues, r.class_capacity
    FROM recent r
    ORDER BY r.worker_id, r.snapshot_at DESC
),
heads AS (
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
    SELECT p.queue_name, p.execution, p.enqueued_at FROM heads p LIMIT $3
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
        'class_capacity', l.class_capacity)), '[]'::jsonb)
     FROM live l) AS live,
    (SELECT coalesce(jsonb_agg(jsonb_build_array(p.queue_name, p.execution, p.enqueued_at)),
                     '[]'::jsonb)
     FROM head_page p) AS heads,
    (SELECT coalesce(jsonb_agg(jsonb_build_array(f.worker_id, f.execution, f.held)), '[]'::jsonb)
     FROM in_flight f) AS in_flight";

/// The parameters of both class reads: the live window in seconds, the
/// snapshot row limit + 1, the stream or head limit + 1.
const CLASS_READ_PARAMETERS: &[ParameterType] = &[
    ParameterType::Float8,
    ParameterType::Int8,
    ParameterType::Int8,
];

/// [`CLASS_COVERAGE_SQL`] on the statement layer.
static CLASS_COVERAGE: TypedStatement =
    TypedStatement::new("class_coverage", CLASS_COVERAGE_SQL, CLASS_READ_PARAMETERS);

/// [`CLASS_REPORT_SQL`] on the statement layer.
static CLASS_REPORT: TypedStatement =
    TypedStatement::new("class_report", CLASS_REPORT_SQL, CLASS_READ_PARAMETERS);

/// The row of [`CLASS_COVERAGE_SQL`].
#[derive(Debug)]
pub(crate) struct CoverageRow {
    pub(crate) checked_at: DateTime<Utc>,
    pub(crate) recent_rows: i64,
    pub(crate) live: Value,
    pub(crate) streams: Value,
}

/// The row of [`CLASS_REPORT_SQL`].
#[derive(Debug)]
struct ReportRow {
    checked_at: DateTime<Utc>,
    recent_rows: i64,
    live: Value,
    heads: Value,
    in_flight: Value,
}

/// One entry of the `live` part.
#[derive(Debug, Deserialize)]
struct LiveEntry {
    worker_id: String,
    snapshot_at: DateTime<Utc>,
    queues: Vec<String>,
    class_capacity: Value,
}

impl PostgresBroker {
    /// The fleet check: for each (queue, class) with `PENDING` rows, whether
    /// a live worker serves it (Python `get_class_coverage_async`,
    /// `clients/python/horsies/core/brokers/postgres.py:3102-3133`).
    ///
    /// A worker is live while its latest snapshot is at most
    /// `WORKER_STATE_LIVE_WINDOW_S` old. It serves a (queue, class) when the
    /// queue is one of its queues and one of its budgets serves the class.
    /// One autocommit statement on the main core pool; reads only.
    ///
    /// Errors, all `MONITORING_QUERY_FAILED`:
    /// - a database or network failure; `retryable` is the statement layer's
    ///   classification ([`BrokerError::is_retryable`]); a closed handle that
    ///   cannot open its pools again, and a row that does not read, are not
    ///   retryable;
    /// - a live snapshot whose `class_capacity` is unreadable, or a part
    ///   that reached its bound ([`LIVE_SNAPSHOT_ROW_LIMIT`],
    ///   [`PENDING_STREAM_LIMIT`]); not retryable, since the same rows give
    ///   the same answer.
    pub async fn get_class_coverage(&self) -> DatabaseResult<ClassCoverage> {
        let row = self
            .coverage_row(CheckoutClass::Batch)
            .await
            .map_err(|error| class_statement_failed("get_class_coverage", error))?;
        coverage_from_row(row).map_err(|cause| class_read_refused("get_class_coverage", &cause))
    }

    /// The class report: pending streams with the wait of their oldest due
    /// row and their coverage, and the budget load of each live worker
    /// (Python `get_class_report_async`,
    /// `clients/python/horsies/core/brokers/postgres.py:3135-3162`).
    ///
    /// One autocommit statement on the main core pool; reads only.
    ///
    /// Errors, all `MONITORING_QUERY_FAILED`: as
    /// [`get_class_coverage`](Self::get_class_coverage) with
    /// [`PENDING_HEAD_LIMIT`] in place of the stream bound, or a live worker
    /// that holds rows of a class that its recorded budgets do not serve
    /// (not retryable).
    pub async fn get_class_report(&self) -> DatabaseResult<ClassReport> {
        let row = self
            .report_row()
            .await
            .map_err(|error| class_statement_failed("get_class_report", error))?;
        report_from_row(row).map_err(|cause| class_read_refused("get_class_report", &cause))
    }

    /// The row of [`CLASS_COVERAGE_SQL`], read on the main core pool with a
    /// checkout of `class`: [`CheckoutClass::Batch`] for a caller of the
    /// handle, [`CheckoutClass::Reserved`] for the reaper. Reads only.
    ///
    /// Errors: [`BrokerError::PoolSettings`], [`BrokerError::Statement`],
    /// [`BrokerError::RowDecode`].
    async fn coverage_row(&self, class: CheckoutClass) -> Result<CoverageRow, BrokerError> {
        let bound = class_coverage_statement();
        let open = self.core_pools_for_read()?;
        self.statement_log().log(&CLASS_COVERAGE);
        let rows = open
            .main()
            .query(class, &bound, StatementBound::ConnectionTimeouts)
            .await?;
        Ok(coverage_row_from(rows)?)
    }

    /// The row of [`CLASS_REPORT_SQL`], read on the main core pool. Reads
    /// only. Errors as [`PostgresBroker::coverage_row`].
    async fn report_row(&self) -> Result<ReportRow, BrokerError> {
        let statement = &CLASS_REPORT;
        let rows = self
            .query_main(
                &statement
                    .bind()
                    .value(&LIVE_WINDOW_SECONDS)
                    .value(&LIVE_SNAPSHOT_READ_LIMIT)
                    .value(&PENDING_HEAD_READ_LIMIT)
                    .finish(),
            )
            .await?;
        let row = one_row(rows, statement)?;
        Ok(ReportRow {
            checked_at: column(&row, statement, "checked_at")?,
            recent_rows: column(&row, statement, "recent_rows")?,
            live: column(&row, statement, "live")?,
            heads: column(&row, statement, "heads")?,
            in_flight: column(&row, statement, "in_flight")?,
        })
    }
}

/// [`CLASS_COVERAGE_SQL`] bound with the live window, the snapshot row limit
/// + 1 and the stream limit + 1. Pure.
pub(crate) fn class_coverage_statement() -> BoundStatement<'static> {
    CLASS_COVERAGE
        .bind()
        .value(&LIVE_WINDOW_SECONDS)
        .value(&LIVE_SNAPSHOT_READ_LIMIT)
        .value(&PENDING_STREAM_READ_LIMIT)
        .finish()
}

/// The row of [`CLASS_COVERAGE_SQL`] from the rows of its statement. Pure.
///
/// Errors: [`RowDecodeError::NoRow`] when the statement returned no row;
/// [`RowDecodeError::Column`] for a column that does not decode.
pub(crate) fn coverage_row_from(rows: Vec<Row>) -> Result<CoverageRow, RowDecodeError> {
    let statement = &CLASS_COVERAGE;
    let row = one_row(rows, statement)?;
    Ok(CoverageRow {
        checked_at: column(&row, statement, "checked_at")?,
        recent_rows: column(&row, statement, "recent_rows")?,
        live: column(&row, statement, "live")?,
        streams: column(&row, statement, "streams")?,
    })
}

/// The fleet check of [`PostgresBroker::get_class_coverage`] for the reaper,
/// after each pass: the same statement on the main core pool, checked out as
/// reserved work ([`CheckoutClass::Reserved`]) as the reaper's other steps.
/// Same answers and errors; reads only.
pub(crate) async fn read_class_coverage(broker: &PostgresBroker) -> DatabaseResult<ClassCoverage> {
    let row = broker
        .coverage_row(CheckoutClass::Reserved)
        .await
        .map_err(|error| class_statement_failed("get_class_coverage", error))?;
    coverage_from_row(row).map_err(|cause| class_read_refused("get_class_coverage", &cause))
}

/// A class read on the core pool that the database did not answer, or whose
/// row did not read. `retryable` is the statement layer's classification
/// ([`BrokerError::is_retryable`]); a row that does not read is not
/// retryable.
fn class_statement_failed(operation: &str, error: BrokerError) -> DatabaseOperationError {
    DatabaseOperationError::monitoring_query_failed(operation, BrokerOperationSource::Broker(error))
}

/// Stored data that a class read cannot decide on, or a bound it reached.
/// Not retryable: the same rows give the same answer. Nothing changed
/// (Python `_class_read_refused`, `postgres.py:408-421`).
fn class_read_refused(operation: &str, cause: &ClassReadRefused) -> DatabaseOperationError {
    DatabaseOperationError::monitoring_query_refused(operation, cause.clone())
}

/// The fleet check from its statement row, checked in Python's order: the
/// snapshot bound, each live snapshot's budgets, the stream bound.
pub(crate) fn coverage_from_row(row: CoverageRow) -> Result<ClassCoverage, ClassReadRefused> {
    let workers = live_workers(row.recent_rows, row.live)?;
    let streams = pending_streams(row.streams)?;
    class_coverage(row.checked_at, &workers, streams)
}

/// The class report from its statement row, checked in Python's order: the
/// snapshot bound, each live snapshot's budgets, the head bound, the rows
/// that each worker holds.
fn report_from_row(row: ReportRow) -> Result<ClassReport, ClassReadRefused> {
    let workers = live_workers(row.recent_rows, row.live)?;
    let heads = pending_heads(row.heads)?;
    let in_flight = in_flight_rows(row.in_flight)?;
    class_report(row.checked_at, &workers, heads, &in_flight)
}

pub(crate) fn live_workers(
    recent_rows: i64,
    live: Value,
) -> Result<Vec<LiveWorker>, ClassReadRefused> {
    if recent_rows > LIVE_SNAPSHOT_ROW_LIMIT as i64 {
        return Err(refused(format!(
            "more than {LIVE_SNAPSHOT_ROW_LIMIT} worker snapshots are inside the live window \
             of {WORKER_STATE_LIVE_WINDOW_S} s"
        )));
    }
    let entries: Vec<LiveEntry> = decoded_part("live", live)?;
    entries
        .into_iter()
        .map(|entry| {
            live_worker(
                entry.worker_id,
                entry.snapshot_at,
                entry.queues,
                &entry.class_capacity,
            )
        })
        .collect()
}

fn pending_streams(streams: Value) -> Result<Vec<(String, Execution)>, ClassReadRefused> {
    let entries: Vec<(String, String)> = decoded_part("streams", streams)?;
    if entries.len() > PENDING_STREAM_LIMIT {
        return Err(refused(format!(
            "more than {PENDING_STREAM_LIMIT} (queue, class) streams have PENDING rows"
        )));
    }
    Ok(entries
        .into_iter()
        .map(|(queue_name, execution)| (queue_name, stored_execution(&execution)))
        .collect())
}

pub(crate) fn pending_heads(heads: Value) -> Result<Vec<StreamHead>, ClassReadRefused> {
    let entries: Vec<(String, String, DateTime<Utc>)> = decoded_part("heads", heads)?;
    if entries.len() > PENDING_HEAD_LIMIT {
        return Err(refused(format!(
            "more than {PENDING_HEAD_LIMIT} (queue, class, priority) groups have PENDING rows"
        )));
    }
    Ok(entries
        .into_iter()
        .map(|(queue_name, execution, enqueued_at)| StreamHead {
            queue_name,
            execution: stored_execution(&execution),
            enqueued_at,
        })
        .collect())
}

/// The rows each live worker holds, per class. The statement groups by
/// (worker, class), so each key comes once.
pub(crate) fn in_flight_rows(
    in_flight: Value,
) -> Result<BTreeMap<(String, Execution), u64>, ClassReadRefused> {
    let entries: Vec<(String, String, u64)> = decoded_part("in_flight", in_flight)?;
    Ok(entries
        .into_iter()
        .map(|(worker_id, execution, held)| ((worker_id, stored_execution(&execution)), held))
        .collect())
}

/// One JSON part of a class read row. A part of another shape refuses the
/// read and names the part: a stored value that does not decode (for example
/// a `NULL` queue name in a snapshot's `queues`).
pub(crate) fn decoded_part<T: DeserializeOwned>(
    part: &str,
    value: Value,
) -> Result<T, ClassReadRefused> {
    serde_json::from_value(value).map_err(|error| {
        refused(format!(
            "the {part} part of the read did not decode: {error}"
        ))
    })
}

/// The [`Execution`] of a `horsies_tasks.execution` value. The column has a
/// CHECK on the three class names (`horsies_tasks_execution_known`), so
/// another value is a defect of the schema, and the read panics with the
/// value.
pub(crate) fn stored_execution(value: &str) -> Execution {
    Execution::from_stored(value).unwrap_or_else(|| {
        panic!(
            "horsies_tasks.execution holds {value:?}; its CHECK admits only the three class names"
        )
    })
}

#[cfg(test)]
mod tests {
    //! The named bounds and the part decoding, without a database: a part at
    //! its bound answers; one row past it refuses the read.
    use super::*;
    use serde_json::json;

    fn streams(count: usize) -> Value {
        Value::Array(
            (0..count)
                .map(|index| json!([format!("q{index}"), "ISOLATED"]))
                .collect(),
        )
    }

    fn heads(count: usize) -> Value {
        Value::Array(
            (0..count)
                .map(|index| {
                    json!([
                        format!("q{index}"),
                        "SHARED_ASYNC",
                        "2026-11-10T12:00:00+00:00"
                    ])
                })
                .collect(),
        )
    }

    #[test]
    fn the_snapshot_bound_refuses_one_row_past_it() {
        let at_bound = live_workers(LIVE_SNAPSHOT_ROW_LIMIT as i64, json!([]));
        assert_eq!(at_bound.map(|workers| workers.len()), Ok(0));

        let past = live_workers(LIVE_SNAPSHOT_ROW_LIMIT as i64 + 1, json!([]))
            .expect_err("past the bound");
        assert_eq!(
            past.reason,
            "more than 20000 worker snapshots are inside the live window of 120 s"
        );
    }

    #[test]
    fn the_stream_bound_refuses_one_stream_past_it() {
        let at_bound = pending_streams(streams(PENDING_STREAM_LIMIT)).expect("at the bound");
        assert_eq!(at_bound.len(), PENDING_STREAM_LIMIT);
        assert_eq!(at_bound[0], ("q0".to_owned(), Execution::Isolated));

        let past = pending_streams(streams(PENDING_STREAM_LIMIT + 1)).expect_err("past the bound");
        assert_eq!(
            past.reason,
            "more than 3000 (queue, class) streams have PENDING rows"
        );
    }

    #[test]
    fn the_head_bound_refuses_one_group_past_it() {
        let at_bound = pending_heads(heads(PENDING_HEAD_LIMIT)).expect("at the bound");
        assert_eq!(at_bound.len(), PENDING_HEAD_LIMIT);
        assert_eq!(at_bound[0].execution, Execution::SharedAsync);

        let past = pending_heads(heads(PENDING_HEAD_LIMIT + 1)).expect_err("past the bound");
        assert_eq!(
            past.reason,
            "more than 10000 (queue, class, priority) groups have PENDING rows"
        );
    }

    #[test]
    fn a_live_entry_that_does_not_decode_refuses_the_read() {
        let live = json!([{
            "worker_id": "w1",
            "snapshot_at": "2026-11-10T12:00:00+01:00",
            "queues": ["a", null],
            "class_capacity": {},
        }]);

        let refusal = live_workers(1, live).expect_err("a NULL queue name");

        assert!(
            refusal
                .reason
                .starts_with("the live part of the read did not decode"),
            "{}",
            refusal.reason
        );
    }

    #[test]
    fn in_flight_rows_key_by_worker_and_class() {
        let rows = in_flight_rows(json!([["w1", "SHARED_THREAD", 2], ["w2", "ISOLATED", 1]]))
            .expect("decodes");
        assert_eq!(
            rows,
            BTreeMap::from([
                (("w1".to_owned(), Execution::SharedThread), 2),
                (("w2".to_owned(), Execution::Isolated), 1),
            ])
        );
    }

    #[test]
    #[should_panic(expected = "horsies_tasks.execution holds \"EXCLUSIVE\"")]
    fn an_execution_outside_the_check_is_a_defect() {
        let _ = stored_execution("EXCLUSIVE");
    }
}
