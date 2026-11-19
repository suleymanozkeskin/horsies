//! The claim side of the task lifecycle on the statement layer, one 0004
//! function each:
//! - [`claim_pass`]: `horsies_claim`, one claim pass with a budget per
//!   execution class and the routes of the worker's claim plan.
//! - [`start_attempt`]: `horsies_start_attempt`, the RUNNING write of one
//!   claimed task with its first runner beat (and the node write of a
//!   workflow task).
//! - [`release_claims`]: `horsies_release_claims`, the return of claimed rows
//!   that no child started.
//!
//! And one read with no 0004 function:
//! - [`count_queue_starts`]: the start check of soft-cap mode, the count of
//!   each capped queue that has held rows.
//!
//! Each is one statement in autocommit on a [`CorePool`], so its locks end
//! with the statement. The claim pass and the start check are batch work
//! ([`CheckoutClass::Batch`]); the start and the release are per-task writes
//! ([`CheckoutClass::Reserved`]). The start, and the release after a failed
//! start ([`release_claims_within`]), are child writes with the bounds of
//! [`crate::broker::child_writes`].
//!
//! A row that breaks the function's row contract is
//! [`BrokerError::ClaimContract`]; a column that does not decode is
//! [`BrokerError::RowDecode`]; a failed statement is
//! [`BrokerError::Statement`].

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::child_writes::{connection_for, query_within, WriteDeadline};
use crate::broker::core_rows::{column, StatementLog};
use crate::broker::error::BrokerError;
use crate::core::lifecycle::OwnedClaim;
use crate::core::task::Execution;
use crate::db::{
    CheckoutClass, CorePool, ParameterType, PooledConnection, Row, StatementBound, TypedStatement,
};

/// The most rows one release statement takes (the bound of
/// `horsies_release_claims`).
pub const MAX_RELEASED_CLAIMS: usize = 1000;

/// `horsies_claim`. Parameters in order: worker id, queues, queue
/// priorities, queue caps, hard cap mode, budgets, routes, prefetch buffer,
/// claims per worker, claim batch, cluster cap, lease in ms, lock keys.
static CLAIM_PASS: TypedStatement = TypedStatement::new(
    "claim_pass",
    "SELECT * FROM horsies_claim($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    &[
        ParameterType::Text,
        ParameterType::Jsonb,
        ParameterType::Jsonb,
        ParameterType::Jsonb,
        ParameterType::Bool,
        ParameterType::Jsonb,
        ParameterType::Jsonb,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Int8,
        ParameterType::Jsonb,
    ],
);

/// `horsies_start_attempt`. Parameters in order: task id, worker id, claim
/// generation, pid, host name, process name, sender of the first beat.
static START_ATTEMPT: TypedStatement = TypedStatement::new(
    "start_attempt",
    "SELECT * FROM horsies_start_attempt($1, $2, $3, $4, $5, $6, $7)",
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
    ],
);

/// `horsies_release_claims`. Parameters in order: task ids, claim
/// generations, worker id, placement.
static RELEASE_CLAIMS: TypedStatement = TypedStatement::new(
    "release_claims",
    "SELECT * FROM horsies_release_claims($1, $2, $3, $4)",
    &[
        ParameterType::UuidArray,
        ParameterType::TimestamptzArray,
        ParameterType::Text,
        ParameterType::Text,
    ],
);

/// The start check of soft-cap mode, one row per queue that has a row it
/// counts: the queue's RUNNING rows on every worker, and its rows CLAIMED by
/// the worker at one of the given claims. Parameters in order: the queues,
/// the worker id, the task ids and the claim generations of the claims.
static COUNT_QUEUE_STARTS: TypedStatement = TypedStatement::new(
    "count_queue_starts",
    "SELECT t.queue_name::text AS queue_name, \
            count(*) FILTER (WHERE t.status = 'RUNNING') AS running, \
            count(*) FILTER (WHERE t.status = 'CLAIMED') AS started_not_running \
     FROM horsies_tasks t \
     WHERE t.queue_name = ANY($1::text[]) \
       AND (t.status = 'RUNNING' \
            OR (t.status = 'CLAIMED' \
                AND t.claimed_by_worker_id = $2 \
                AND (t.id, t.claimed_at) IN ( \
                    SELECT s.id, s.claimed_at \
                    FROM unnest($3::uuid[], $4::timestamptz[]) AS s(id, claimed_at)))) \
     GROUP BY t.queue_name",
    &[
        ParameterType::TextArray,
        ParameterType::Text,
        ParameterType::UuidArray,
        ParameterType::TimestamptzArray,
    ],
);

// ---------------------------------------------------------------------------
// Claim pass
// ---------------------------------------------------------------------------

/// The budgets and routes of one claim pass: the capacity of each budget
/// class, and the budget class that each served row class claims into
/// (Python `ClaimPlan`). Every route names a budget that the pass holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimClasses {
    budgets: HashMap<Execution, u32>,
    routes: HashMap<Execution, Execution>,
}

/// A route names a budget class that the claim pass does not hold. The
/// worker builds both maps from one claim plan, so this is a defect of the
/// plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "the route of {} rows names the budget {}, which the claim pass does not hold",
    .row_class.as_str(),
    .budget_class.as_str()
)]
pub struct UnheldBudget {
    pub row_class: Execution,
    pub budget_class: Execution,
}

impl ClaimClasses {
    /// The classes of a pass. A worker that serves nothing passes two empty
    /// maps.
    ///
    /// Errors: [`UnheldBudget`] for the first route whose budget class has no
    /// budget.
    pub fn new(
        budgets: HashMap<Execution, u32>,
        routes: HashMap<Execution, Execution>,
    ) -> Result<ClaimClasses, UnheldBudget> {
        match routes
            .iter()
            .find(|(_, budget_class)| !budgets.contains_key(budget_class))
        {
            Some((row_class, budget_class)) => Err(UnheldBudget {
                row_class: *row_class,
                budget_class: *budget_class,
            }),
            None => Ok(ClaimClasses { budgets, routes }),
        }
    }

    /// `p_budgets`: `{budget class: capacity}`.
    fn budgets_json(&self) -> serde_json::Value {
        serde_json::Value::Object(
            self.budgets
                .iter()
                .map(|(class, capacity)| (class.as_str().to_owned(), (*capacity).into()))
                .collect(),
        )
    }

    /// `p_routes`: `{row class: budget class}`.
    fn routes_json(&self) -> serde_json::Value {
        serde_json::Value::Object(
            self.routes
                .iter()
                .map(|(row_class, budget_class)| {
                    (
                        row_class.as_str().to_owned(),
                        budget_class.as_str().to_owned().into(),
                    )
                })
                .collect(),
        )
    }
}

/// The arguments of one claim pass.
#[derive(Debug, Clone)]
pub struct ClaimPassRequest {
    pub worker_id: String,
    /// The queues of the pass, in claim order.
    pub queues: Vec<String>,
    /// The priority of each served queue.
    pub queue_priority: HashMap<String, i32>,
    /// `max_concurrency` of the capped queues only.
    pub queue_max_concurrency: HashMap<String, u32>,
    /// `prefetch_buffer == 0`: the budgets count RUNNING and live CLAIMED rows.
    pub hard_cap_mode: bool,
    pub classes: ClaimClasses,
    pub prefetch_buffer: u32,
    pub max_claim_per_worker: u32,
    pub max_claim_batch: u32,
    pub cluster_wide_cap: Option<u32>,
    /// The lease of every claim of the pass. Every claim has a lease (the
    /// function refuses a lease that is not above 0).
    pub claim_lease_ms: NonZeroU32,
    /// The advisory keys of the cap accounting; the function takes them in
    /// ascending order.
    pub lock_keys: Vec<i64>,
}

/// One row the pass claimed: everything the worker needs to dispatch it and
/// to write a failed attempt with no read before the write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedRow {
    pub id: Uuid,
    pub task_name: String,
    pub queue_name: String,
    /// The encoded task input as one JSON value, as the send wrote it.
    pub input: String,
    pub task_options: Option<String>,
    pub execution: Execution,
    pub is_workflow_task: bool,
    /// The claim generation of this claim.
    pub claimed_at: DateTime<Utc>,
    pub retry_count: i32,
    pub max_retries: i32,
}

/// Whether the pass took every row of its plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimTake {
    /// The pass took every planned row, or planned none.
    Full,
    /// The pass took fewer rows than it planned (rows locked by another
    /// session, for example); `short_by` rows were left.
    Short { short_by: NonZeroU32 },
}

/// What one claim pass returned: the claimed rows in claim order and
/// whether the take was short.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimPass {
    pub claimed: Vec<ClaimedRow>,
    pub take: ClaimTake,
}

/// The columns of one `horsies_claim` row, as returned.
#[derive(Debug, Clone, Default)]
struct ClaimWireRow {
    row_kind: String,
    id: Option<Uuid>,
    task_name: Option<String>,
    queue_name: Option<String>,
    input: Option<String>,
    task_options: Option<String>,
    execution: Option<String>,
    is_workflow_task: Option<bool>,
    claimed_at: Option<DateTime<Utc>>,
    retry_count: Option<i32>,
    max_retries: Option<i32>,
    short_by: Option<i32>,
}

/// Run one claim pass: `horsies_claim` on `pool`, as batch work.
///
/// Claims rows (CLAIMED, a new claim generation, a lease) in the same
/// statement; writes nothing else. Does not dispatch, start or release a
/// row.
///
/// Errors: [`BrokerError::InvalidClaimArgument`] for a count that the
/// function's integer arguments cannot hold (refused, never clamped; nothing
/// sent); [`BrokerError::Statement`]; [`BrokerError::RowDecode`];
/// [`BrokerError::ClaimContract`] for rows that break the claim's row
/// contract. After `OutcomeUnknown` the pass may have claimed rows; they
/// return to the queue when their lease expires.
pub(crate) async fn claim_pass(
    pool: &CorePool,
    log: StatementLog,
    request: &ClaimPassRequest,
) -> Result<ClaimPass, BrokerError> {
    let prefetch_buffer = claim_integer("prefetch_buffer", request.prefetch_buffer)?;
    let max_claim_per_worker = claim_integer("max_claim_per_worker", request.max_claim_per_worker)?;
    let max_claim_batch = claim_integer("max_claim_batch", request.max_claim_batch)?;
    let cluster_wide_cap = request
        .cluster_wide_cap
        .map(|cap| claim_integer("cluster_wide_cap", cap))
        .transpose()?;
    let queues = serde_json::to_value(&request.queues)?;
    let queue_priority = serde_json::to_value(&request.queue_priority)?;
    let queue_max_concurrency = serde_json::to_value(&request.queue_max_concurrency)?;
    let lock_keys = serde_json::to_value(&request.lock_keys)?;
    let budgets = request.classes.budgets_json();
    let routes = request.classes.routes_json();
    let lease_ms = i64::from(request.claim_lease_ms.get());
    let statement = &CLAIM_PASS;
    let bound = statement
        .bind()
        .value(&request.worker_id)
        .value(&queues)
        .value(&queue_priority)
        .value(&queue_max_concurrency)
        .value(&request.hard_cap_mode)
        .value(&budgets)
        .value(&routes)
        .value(&prefetch_buffer)
        .value(&max_claim_per_worker)
        .value(&max_claim_batch)
        .value(&cluster_wide_cap)
        .value(&lease_ms)
        .value(&lock_keys)
        .finish();
    log.log(statement);
    let rows = pool
        .query(
            CheckoutClass::Batch,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    let wire = rows
        .iter()
        .map(|row| claim_wire_row(row, statement))
        .collect::<Result<Vec<_>, _>>()?;
    let pass = decode_claim_pass(wire).map_err(BrokerError::ClaimContract)?;
    tracing::debug!(
        worker_id = %request.worker_id,
        count = pass.claimed.len(),
        take = ?pass.take,
        "claim pass completed",
    );
    Ok(pass)
}

/// A count as the function's `integer` argument.
fn claim_integer(name: &str, value: u32) -> Result<i32, BrokerError> {
    i32::try_from(value).map_err(|_| {
        BrokerError::InvalidClaimArgument(format!(
            "{name} = {value} exceeds the claim function's integer range (max {})",
            i32::MAX
        ))
    })
}

fn claim_wire_row(row: &Row, statement: &TypedStatement) -> Result<ClaimWireRow, BrokerError> {
    Ok(ClaimWireRow {
        row_kind: column(row, statement, "row_kind")?,
        id: column(row, statement, "id")?,
        task_name: column(row, statement, "task_name")?,
        queue_name: column(row, statement, "queue_name")?,
        input: column(row, statement, "input")?,
        task_options: column(row, statement, "task_options")?,
        execution: column(row, statement, "execution")?,
        is_workflow_task: column(row, statement, "is_workflow_task")?,
        claimed_at: column(row, statement, "claimed_at")?,
        retry_count: column(row, statement, "retry_count")?,
        max_retries: column(row, statement, "max_retries")?,
        short_by: column(row, statement, "short_by")?,
    })
}

/// The claimed rows in claim order and at most one `SHORT` row. Pure.
///
/// Errors: the contract breach as text: a row kind other than `CLAIMED` and
/// `SHORT`, a second `SHORT` row, a `CLAIMED` row with a column missing or
/// with `short_by`, a `SHORT` row with a task column or a count that is not
/// above 0, an unknown execution class.
fn decode_claim_pass(rows: Vec<ClaimWireRow>) -> Result<ClaimPass, String> {
    let mut claimed = Vec::with_capacity(rows.len());
    let mut take = ClaimTake::Full;
    for row in rows {
        match row.row_kind.as_str() {
            "CLAIMED" => claimed.push(decode_claimed_row(row)?),
            "SHORT" => {
                let short_by = decode_short_row(row)?;
                take = match take {
                    ClaimTake::Full => ClaimTake::Short { short_by },
                    ClaimTake::Short { .. } => {
                        return Err("horsies_claim returned a second SHORT row".to_owned())
                    }
                };
            }
            other => return Err(format!("horsies_claim returned row kind {other:?}")),
        }
    }
    Ok(ClaimPass { claimed, take })
}

fn decode_claimed_row(row: ClaimWireRow) -> Result<ClaimedRow, String> {
    if row.short_by.is_some() {
        return Err("a CLAIMED row of horsies_claim carries short_by".to_owned());
    }
    let required = |name: &str| format!("a CLAIMED row of horsies_claim has no {name}");
    let id = row.id.ok_or_else(|| required("id"))?;
    let execution_text = row.execution.ok_or_else(|| required("execution"))?;
    // Python `parse_row_class` (`clients/python/horsies/core/worker/row_class.py:75-88`):
    // the refusal names the task and the stored value.
    let execution = Execution::from_stored(&execution_text).ok_or_else(|| {
        format!(
            "a CLAIMED row of horsies_claim: claimed task {id} has execution \
             {execution_text:?}, which is not an Execution value"
        )
    })?;
    Ok(ClaimedRow {
        id,
        task_name: row.task_name.ok_or_else(|| required("task_name"))?,
        queue_name: row.queue_name.ok_or_else(|| required("queue_name"))?,
        input: row.input.ok_or_else(|| required("input"))?,
        task_options: row.task_options,
        execution,
        is_workflow_task: row
            .is_workflow_task
            .ok_or_else(|| required("is_workflow_task"))?,
        claimed_at: row.claimed_at.ok_or_else(|| required("claimed_at"))?,
        retry_count: row.retry_count.ok_or_else(|| required("retry_count"))?,
        max_retries: row.max_retries.ok_or_else(|| required("max_retries"))?,
    })
}

fn decode_short_row(row: ClaimWireRow) -> Result<NonZeroU32, String> {
    let ClaimWireRow {
        row_kind: _,
        id,
        task_name,
        queue_name,
        input,
        task_options,
        execution,
        is_workflow_task,
        claimed_at,
        retry_count,
        max_retries,
        short_by,
    } = row;
    let task_columns = [
        id.is_some(),
        task_name.is_some(),
        queue_name.is_some(),
        input.is_some(),
        task_options.is_some(),
        execution.is_some(),
        is_workflow_task.is_some(),
        claimed_at.is_some(),
        retry_count.is_some(),
        max_retries.is_some(),
    ];
    if task_columns.contains(&true) {
        return Err("a SHORT row of horsies_claim carries a task column".to_owned());
    }
    let short_by = short_by.ok_or("a SHORT row of horsies_claim has no short_by")?;
    u32::try_from(short_by)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| format!("a SHORT row of horsies_claim has short_by {short_by}"))
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

/// The RUNNING write of one claimed task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartAttempt {
    pub task_id: Uuid,
    /// The worker and the claim generation that hold the row.
    pub fence: OwnedClaim,
    /// The process that runs the task.
    pub worker_pid: i32,
    pub worker_hostname: String,
    pub worker_process_name: String,
    /// The sender id of the first runner beat.
    pub beat_sender_id: String,
}

impl StartAttempt {
    /// The start of `task_id` under `fence` by the process `worker_pid` on
    /// `worker_hostname`: the process name is `worker-<pid>` and the beat
    /// sender `<worker_id>:<pid>` (Python `start_attempt_params`). Pure.
    pub fn of_process(
        task_id: Uuid,
        fence: OwnedClaim,
        worker_pid: i32,
        worker_hostname: String,
    ) -> StartAttempt {
        let beat_sender_id = format!("{}:{worker_pid}", fence.worker_id);
        StartAttempt {
            task_id,
            fence,
            worker_pid,
            worker_hostname,
            worker_process_name: format!("worker-{worker_pid}"),
            beat_sender_id,
        }
    }
}

/// Why a start wrote nothing: the first reason that holds, in the
/// function's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartMiss {
    /// The row is CLAIMED by this worker (any claim generation) and its
    /// `good_until` passed: the expiry applies.
    DeadlinePassed,
    /// The row is absent, or its status, owner or claim generation differs.
    NotHeld,
    /// The claim of this generation expired.
    LeaseExpired,
    /// The workflow of the task is PAUSED.
    WorkflowPaused,
    /// The workflow of the task is CANCELLED.
    WorkflowCancelled,
    /// No live node links the workflow task.
    NodeNotLinked,
}

impl StartMiss {
    /// The function's `miss_reason` text of this miss.
    pub const fn as_str(self) -> &'static str {
        match self {
            StartMiss::DeadlinePassed => "DEADLINE_PASSED",
            StartMiss::NotHeld => "NOT_HELD",
            StartMiss::LeaseExpired => "LEASE_EXPIRED",
            StartMiss::WorkflowPaused => "WORKFLOW_PAUSED",
            StartMiss::WorkflowCancelled => "WORKFLOW_CANCELLED",
            StartMiss::NodeNotLinked => "NODE_NOT_LINKED",
        }
    }

    fn from_wire(value: &str) -> Option<StartMiss> {
        match value {
            "DEADLINE_PASSED" => Some(StartMiss::DeadlinePassed),
            "NOT_HELD" => Some(StartMiss::NotHeld),
            "LEASE_EXPIRED" => Some(StartMiss::LeaseExpired),
            "WORKFLOW_PAUSED" => Some(StartMiss::WorkflowPaused),
            "WORKFLOW_CANCELLED" => Some(StartMiss::WorkflowCancelled),
            "NODE_NOT_LINKED" => Some(StartMiss::NodeNotLinked),
            _ => None,
        }
    }
}

/// What the start reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartOutcome {
    /// The row is RUNNING with its first runner beat (and, for a workflow
    /// task, its node RUNNING), from `started_at` on the database clock.
    Started { started_at: DateTime<Utc> },
    /// Nothing was written.
    Missed(StartMiss),
}

/// Until when a start may send its statement, judged after the pool gave
/// the connection (Python's `start_check`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartWindow {
    /// The start sends its statement whenever the pool gives the connection.
    Open,
    /// The start sends no statement once this instant has passed: the
    /// deadline of the run fired while the start waited for a connection.
    Until(std::time::Instant),
}

/// What a start with a [`StartWindow`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowedStart {
    /// The statement ran.
    Ran(StartOutcome),
    /// The window closed while the start waited for a connection: no
    /// statement ran, so the row stays CLAIMED (Python
    /// `DeadlineBeforeStart`).
    WindowClosed,
}

/// Start one claimed task: `horsies_start_attempt` on `pool`, as a child
/// write within `deadline`, the deadline of its start attempt
/// ([`crate::broker::child_writes::connection_for`]). Writes the RUNNING
/// row, the first runner beat and the node write in one statement, or
/// nothing on a miss. Does not expire the row or stop the task on a miss;
/// that is the caller's next step, within the same deadline.
///
/// Errors: [`BrokerError::Statement`]: nothing sent (a failed checkout, or
/// `deadline` passed first); refused, a cancel at the bound included
/// (nothing written: the cancelled start does not commit later); or
/// `OutcomeUnknown` (the row may be RUNNING; the same start sent again
/// misses with `NotHeld`, the row being no longer CLAIMED).
/// [`BrokerError::RowDecode`]; [`BrokerError::ClaimContract`] when the
/// function does not return exactly one row of its contract.
pub(crate) async fn start_attempt(
    pool: &CorePool,
    log: StatementLog,
    command: &StartAttempt,
    deadline: WriteDeadline,
) -> Result<StartOutcome, BrokerError> {
    let (mut connection, statement_bound) =
        connection_for(pool, START_ATTEMPT.name(), deadline).await?;
    start_on(&mut connection, log, command, statement_bound).await
}

/// [`start_attempt`] with a window: after the pool gave the connection, a
/// closed window ends the start with no statement
/// ([`WindowedStart::WindowClosed`]).
///
/// Errors: as [`start_attempt`].
pub(crate) async fn start_attempt_within(
    pool: &CorePool,
    log: StatementLog,
    command: &StartAttempt,
    window: StartWindow,
    deadline: WriteDeadline,
) -> Result<WindowedStart, BrokerError> {
    let (mut connection, statement_bound) =
        connection_for(pool, START_ATTEMPT.name(), deadline).await?;
    match window {
        StartWindow::Until(run_deadline) if std::time::Instant::now() >= run_deadline => {
            Ok(WindowedStart::WindowClosed)
        }
        StartWindow::Until(_) | StartWindow::Open => {
            start_on(&mut connection, log, command, statement_bound)
                .await
                .map(WindowedStart::Ran)
        }
    }
}

/// Run `horsies_start_attempt` on a checked-out connection with
/// `statement_bound` and decode its row. Errors: as [`start_attempt`].
async fn start_on(
    connection: &mut PooledConnection,
    log: StatementLog,
    command: &StartAttempt,
    statement_bound: StatementBound,
) -> Result<StartOutcome, BrokerError> {
    let statement = &START_ATTEMPT;
    let bound = statement
        .bind()
        .value(&command.task_id)
        .value(&command.fence.worker_id)
        .value(&command.fence.claimed_at)
        .value(&command.worker_pid)
        .value(&command.worker_hostname)
        .value(&command.worker_process_name)
        .value(&command.beat_sender_id)
        .finish();
    log.log(statement);
    let rows = connection.query(&bound, statement_bound).await?;
    let [row] = rows.as_slice() else {
        return Err(BrokerError::ClaimContract(format!(
            "horsies_start_attempt returned {} rows for task {}; it returns one",
            rows.len(),
            command.task_id
        )));
    };
    let outcome: String = column(row, statement, "outcome")?;
    let miss_reason: Option<String> = column(row, statement, "miss_reason")?;
    let started_at: Option<DateTime<Utc>> = column(row, statement, "started_at")?;
    decode_start(&outcome, miss_reason.as_deref(), started_at).map_err(|detail| {
        BrokerError::ClaimContract(format!(
            "horsies_start_attempt for task {}: {detail}",
            command.task_id
        ))
    })
}

/// The outcome of one start row. Pure.
fn decode_start(
    outcome: &str,
    miss_reason: Option<&str>,
    started_at: Option<DateTime<Utc>>,
) -> Result<StartOutcome, String> {
    match (outcome, miss_reason, started_at) {
        ("STARTED", None, Some(started_at)) => Ok(StartOutcome::Started { started_at }),
        ("MISSED", Some(reason), None) => StartMiss::from_wire(reason)
            .map(StartOutcome::Missed)
            .ok_or_else(|| format!("unknown miss reason {reason:?}")),
        (outcome, miss_reason, started_at) => Err(format!(
            "outcome {outcome:?} with miss_reason {miss_reason:?} and started_at {started_at:?}"
        )),
    }
}

// ---------------------------------------------------------------------------
// Start check
// ---------------------------------------------------------------------------

/// The arguments of one start check of soft-cap mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStartCheck {
    pub worker_id: String,
    /// The capped queues that have held rows.
    pub queues: Vec<String>,
    /// The claims (task id, claim generation) of capped queues that the
    /// worker submitted and whose outcome write has not ended.
    pub started: Vec<(Uuid, DateTime<Utc>)>,
}

/// The count of each queue of a start check: its RUNNING rows on every
/// worker, plus the started claims of the check that are still CLAIMED
/// because their child has not set them RUNNING yet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueStarts {
    counts: HashMap<String, u64>,
}

impl QueueStarts {
    /// The counts of the queues that have one. A queue with no counted row
    /// is absent.
    pub fn new(counts: HashMap<String, u64>) -> QueueStarts {
        QueueStarts { counts }
    }

    /// The count of `queue_name`; 0 when the statement counted no row of it.
    pub fn of(&self, queue_name: &str) -> u64 {
        self.counts.get(queue_name).copied().unwrap_or(0)
    }

    /// Every counted queue with its count. A queue with no counted row is
    /// absent.
    pub fn into_counts(self) -> HashMap<String, u64> {
        self.counts
    }
}

/// Count the queues of `check`: one statement on `pool`, as batch work.
/// Reads only; locks nothing. The counts are read without a lock, so a
/// start of another worker in the same instant is not seen.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`];
/// [`BrokerError::ClaimContract`] for a queue counted twice, a queue that
/// the check did not name, or a negative count.
pub(crate) async fn count_queue_starts(
    pool: &CorePool,
    log: StatementLog,
    check: &QueueStartCheck,
) -> Result<QueueStarts, BrokerError> {
    let task_ids: Vec<Uuid> = check.started.iter().map(|(task_id, _)| *task_id).collect();
    let claimed_ats: Vec<DateTime<Utc>> = check
        .started
        .iter()
        .map(|(_, claimed_at)| *claimed_at)
        .collect();
    let statement = &COUNT_QUEUE_STARTS;
    let bound = statement
        .bind()
        .value(&check.queues)
        .value(&check.worker_id)
        .value(&task_ids)
        .value(&claimed_ats)
        .finish();
    log.log(statement);
    let rows = pool
        .query(
            CheckoutClass::Batch,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    let wire = rows
        .iter()
        .map(|row| -> Result<(String, i64, i64), BrokerError> {
            Ok((
                column::<String>(row, statement, "queue_name")?,
                column::<i64>(row, statement, "running")?,
                column::<i64>(row, statement, "started_not_running")?,
            ))
        })
        .collect::<Result<Vec<_>, BrokerError>>()?;
    decode_queue_starts(&check.queues, wire).map_err(BrokerError::ClaimContract)
}

/// The count of each queue: RUNNING plus started and not RUNNING. Pure.
///
/// Errors: the contract breach as text: a queue counted twice, a queue that
/// `queues` does not name, a negative count.
fn decode_queue_starts(
    queues: &[String],
    rows: Vec<(String, i64, i64)>,
) -> Result<QueueStarts, String> {
    let mut counts: HashMap<String, u64> = HashMap::with_capacity(rows.len());
    for (queue_name, running, started_not_running) in rows {
        if !queues.contains(&queue_name) {
            return Err(format!(
                "count_queue_starts counted queue {queue_name:?}, which the check did not name"
            ));
        }
        let (Ok(running_rows), Ok(started_rows)) =
            (u64::try_from(running), u64::try_from(started_not_running))
        else {
            return Err(format!(
                "count_queue_starts counted {running} RUNNING and {started_not_running} started \
                 rows of queue {queue_name:?}"
            ));
        };
        match counts.entry(queue_name) {
            Entry::Occupied(counted) => {
                return Err(format!(
                    "count_queue_starts counted queue {:?} twice",
                    counted.key()
                ))
            }
            Entry::Vacant(counted) => {
                let _count = counted.insert(running_rows + started_rows);
            }
        }
    }
    Ok(QueueStarts { counts })
}

// ---------------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------------

/// Where a released row runs next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleasePlacement {
    /// The row keeps its execution class. No wake.
    SameClass,
    /// The row moves to the `ISOLATED` class; each queue of a released row
    /// gets one `class` wake.
    Isolated,
}

impl ReleasePlacement {
    fn as_sql(self) -> &'static str {
        match self {
            ReleasePlacement::SameClass => "SAME_CLASS",
            ReleasePlacement::Isolated => "ISOLATED",
        }
    }
}

/// The claims of one release: at most [`MAX_RELEASED_CLAIMS`] rows, each
/// task once, each with the claim generation it was claimed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseBatch {
    task_ids: Vec<Uuid>,
    claimed_ats: Vec<DateTime<Utc>>,
}

/// A release batch that the function refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReleaseBatchError {
    #[error("a release takes at most {} rows, got {count}", MAX_RELEASED_CLAIMS)]
    TooManyRows { count: usize },
    #[error("task {task_id} is in the release twice")]
    RepeatedTask { task_id: Uuid },
}

impl ReleaseBatch {
    /// The batch of `claims`, in their order.
    ///
    /// Errors: [`ReleaseBatchError`]; nothing is built.
    pub fn new(claims: &[(Uuid, DateTime<Utc>)]) -> Result<ReleaseBatch, ReleaseBatchError> {
        if claims.len() > MAX_RELEASED_CLAIMS {
            return Err(ReleaseBatchError::TooManyRows {
                count: claims.len(),
            });
        }
        let mut seen = HashSet::with_capacity(claims.len());
        if let Some((task_id, _)) = claims.iter().find(|(task_id, _)| !seen.insert(*task_id)) {
            return Err(ReleaseBatchError::RepeatedTask { task_id: *task_id });
        }
        Ok(ReleaseBatch {
            task_ids: claims.iter().map(|(task_id, _)| *task_id).collect(),
            claimed_ats: claims.iter().map(|(_, claimed_at)| *claimed_at).collect(),
        })
    }

    /// Whether the batch holds no claim.
    pub fn is_empty(&self) -> bool {
        self.task_ids.is_empty()
    }
}

/// The return of claimed rows that no child started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseClaims {
    pub worker_id: String,
    pub batch: ReleaseBatch,
    pub placement: ReleasePlacement,
}

/// What the release did with one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The row was CLAIMED by this worker at its claim generation and is
    /// PENDING again with every claim and run field cleared.
    Released,
    /// The row is absent, or is not CLAIMED by this worker at this claim
    /// generation (a re-claimed row included). It was not touched.
    NotHeld,
}

/// Release claimed rows: `horsies_release_claims` on `pool`, as a per-task
/// write with the bounds of the pool and the connection. Writes no attempt
/// row. Returns one outcome per input row, in the input order.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`];
/// [`BrokerError::ClaimContract`] when the rows do not answer each input
/// row exactly once. After `OutcomeUnknown` the rows may be released; the
/// same release sent again answers `NotHeld` for each released row.
pub(crate) async fn release_claims(
    pool: &CorePool,
    log: StatementLog,
    command: &ReleaseClaims,
) -> Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError> {
    release_on(pool, log, command, ReleaseRun::PoolBounds).await
}

/// [`release_claims`] as a child write within `deadline`
/// ([`crate::broker::child_writes::query_within`]): the release of the
/// claim of a start that failed, within the deadline of its start attempt.
///
/// Errors: as [`release_claims`]; [`BrokerError::Statement`] with nothing
/// sent when `deadline` passed first, and refused when the cancel at the
/// bound ended the statement (nothing released).
pub(crate) async fn release_claims_within(
    pool: &CorePool,
    log: StatementLog,
    command: &ReleaseClaims,
    deadline: WriteDeadline,
) -> Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError> {
    release_on(pool, log, command, ReleaseRun::ChildWrite(deadline)).await
}

/// The bounds of one release statement.
#[derive(Debug, Clone, Copy)]
enum ReleaseRun {
    /// The checkout wait of the pool and the connection's own timeouts.
    PoolBounds,
    /// A child write within this deadline.
    ChildWrite(WriteDeadline),
}

/// Run one release with the bounds of `run` and decode its rows. Errors:
/// as [`release_claims`].
async fn release_on(
    pool: &CorePool,
    log: StatementLog,
    command: &ReleaseClaims,
    run: ReleaseRun,
) -> Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError> {
    let placement = command.placement.as_sql();
    let statement = &RELEASE_CLAIMS;
    let bound = statement
        .bind()
        .value(&command.batch.task_ids)
        .value(&command.batch.claimed_ats)
        .value(&command.worker_id)
        .value(&placement)
        .finish();
    log.log(statement);
    let rows = match run {
        ReleaseRun::PoolBounds => {
            pool.query(
                CheckoutClass::Reserved,
                &bound,
                StatementBound::ConnectionTimeouts,
            )
            .await?
        }
        ReleaseRun::ChildWrite(deadline) => query_within(pool, &bound, deadline).await?,
    };
    let wire = rows
        .iter()
        .map(|row| -> Result<(i64, Uuid, String), BrokerError> {
            Ok((
                column::<i64>(row, statement, "ordinality")?,
                column::<Uuid>(row, statement, "task_id")?,
                column::<String>(row, statement, "outcome")?,
            ))
        })
        .collect::<Result<Vec<_>, BrokerError>>()?;
    decode_release(&command.batch.task_ids, wire).map_err(BrokerError::ClaimContract)
}

/// One outcome per input row, in the input order. Pure.
fn decode_release(
    task_ids: &[Uuid],
    rows: Vec<(i64, Uuid, String)>,
) -> Result<Vec<(Uuid, ReleaseOutcome)>, String> {
    let mut by_position: Vec<Option<ReleaseOutcome>> = vec![None; task_ids.len()];
    for (ordinality, task_id, outcome) in rows {
        let position = usize::try_from(ordinality)
            .ok()
            .and_then(|ordinality| ordinality.checked_sub(1))
            .filter(|position| *position < task_ids.len())
            .ok_or_else(|| format!("ordinality {ordinality} outside 1..={}", task_ids.len()))?;
        if task_ids[position] != task_id {
            return Err(format!(
                "ordinality {ordinality} names task {task_id}, the input holds {}",
                task_ids[position]
            ));
        }
        let decoded = match outcome.as_str() {
            "RELEASED" => ReleaseOutcome::Released,
            "NOT_HELD" => ReleaseOutcome::NotHeld,
            other => return Err(format!("unknown release outcome {other:?}")),
        };
        if by_position[position].replace(decoded).is_some() {
            return Err(format!("ordinality {ordinality} answered twice"));
        }
    }
    task_ids
        .iter()
        .zip(by_position)
        .enumerate()
        .map(|(position, (task_id, outcome))| match outcome {
            Some(outcome) => Ok((*task_id, outcome)),
            None => Err(format!("ordinality {} has no answer", position + 1)),
        })
        .collect()
}

#[cfg(test)]
mod tests;
