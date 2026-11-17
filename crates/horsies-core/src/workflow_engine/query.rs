use std::collections::HashMap;
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::time::Instant;
use uuid::Uuid;

use crate::broker::core_rows::{column, first_row, RowDecodeError};
use crate::broker::process_listener::{fixed_channel, ListenError, WORKFLOW_DONE_CHANNEL};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::workflow::handle_types::{HandleErrorCode, HandleOperationError, HandleResult};
use crate::core::workflow::summary::SubWorkflowSummary;
use crate::core::{
    OperationalErrorCode, OutcomeCode, RetrievalCode, TaskError, TaskResult, WorkflowStatus,
    WorkflowTaskStatus,
};
use crate::db::{
    CheckoutClass, FromSql, ParameterType, PayloadFilter, Row, TypedStatement, WakeEvent,
    WakeSubscription,
};

use crate::core::task::result::TASK_RESULT_ENVELOPE_KEY;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::info::WorkflowTaskInfo;
use crate::workflow_engine::lifecycle::handle_operation_error;
use crate::workflow_engine::steps::run_alone;

/// The marker key of the result of a workflow with no output node (Python `__h_outputless_terminals__`,
/// `core/workflows/engine.py:3973-3983`; the baseline writes it in `horsies_workflow_outputless_result`).
const OUTPUTLESS_TERMINALS_KEY: &str = "__h_outputless_terminals__";

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

// Every read runs alone in autocommit on the main core pool of the broker, as
// handle work ([`CheckoutClass::Batch`]).

/// The status of one workflow.
static WORKFLOW_STATUS: TypedStatement = TypedStatement::new(
    "workflow_status",
    "SELECT status FROM horsies_workflows WHERE id = $1",
    &[ParameterType::Uuid],
);

/// The status, the stored result and the stored error of one workflow.
static WORKFLOW_RESULT: TypedStatement = TypedStatement::new(
    "workflow_result",
    "SELECT status, result, error FROM horsies_workflows WHERE id = $1",
    &[ParameterType::Uuid],
);

/// Every node of one workflow, in index order, from the workflow row (Python
/// `GET_WORKFLOW_TASKS_SQL`, `python/horsies/core/models/workflow/handle.py`): no row means no
/// workflow has the id; one row whose `workflow_task_id` is NULL means the workflow has no node.
static WORKFLOW_NODES: TypedStatement = TypedStatement::new(
    "workflow_nodes",
    "SELECT wt.id AS workflow_task_id, wt.node_id, wt.task_index, wt.task_name, wt.status,
            wt.result, wt.started_at, wt.completed_at, wt.sub_workflow_id,
            wt.sub_workflow_summary, wt.is_subworkflow, wt.sub_definition_key
     FROM horsies_workflows w
     LEFT JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
     WHERE w.id = $1
     ORDER BY wt.task_index",
    &[ParameterType::Uuid],
);

/// The nodes of one workflow that store a result, from the workflow row (Python
/// `GET_WORKFLOW_TASK_RESULTS_SQL`): no row means no workflow has the id; one row whose
/// `workflow_task_id` is NULL means no node stores a result.
static WORKFLOW_NODE_RESULTS: TypedStatement = TypedStatement::new(
    "workflow_node_results",
    "SELECT wt.id AS workflow_task_id, wt.node_id, wt.task_name, wt.result,
            wt.is_subworkflow, wt.sub_definition_key
     FROM horsies_workflows w
     LEFT JOIN horsies_workflow_tasks wt
            ON wt.workflow_id = w.id
           AND wt.result IS NOT NULL
     WHERE w.id = $1",
    &[ParameterType::Uuid],
);

/// The stored result of the node `$2` of the workflow `$1`, when it has one (Python
/// `GET_WORKFLOW_TASK_RESULT_BY_NODE_SQL`).
static NODE_RESULT: TypedStatement = TypedStatement::new(
    "workflow_node_result",
    "SELECT task_name, result, is_subworkflow, sub_definition_key
     FROM horsies_workflow_tasks
     WHERE workflow_id = $1 AND node_id = $2
       AND result IS NOT NULL",
    &[ParameterType::Uuid, ParameterType::Text],
);

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct WorkflowResultRow {
    status: String,
    result: Option<String>,
    error: Option<String>,
}

/// One node row of [`WORKFLOW_NODES`], with its stored values as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredNode {
    pub node_id: Option<String>,
    pub task_index: i32,
    pub task_name: String,
    /// The stored node status, as stored.
    pub status: String,
    /// The stored result envelope, as stored.
    pub result: Option<String>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub sub_workflow_id: Option<Uuid>,
    /// The stored sub-workflow summary, as stored.
    pub sub_workflow_summary: Option<String>,
    pub is_subworkflow: bool,
    pub sub_definition_key: Option<String>,
}

/// The node rows of one workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodesRead {
    /// No workflow has the id.
    WorkflowAbsent,
    /// The workflow's nodes in index order; empty when it has none.
    Nodes(Vec<StoredNode>),
}

/// One node that stores a result ([`WORKFLOW_NODE_RESULTS`], [`NODE_RESULT`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredNodeResult {
    pub node_id: Option<String>,
    pub task_name: String,
    /// The stored result envelope, as stored.
    pub result: String,
    pub is_subworkflow: bool,
    pub sub_definition_key: Option<String>,
}

/// The nodes of one workflow that store a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeResultsRead {
    /// No workflow has the id.
    WorkflowAbsent,
    /// The nodes that store a result; empty when none does.
    Nodes(Vec<StoredNodeResult>),
}

/// The rows of a read that starts from the workflow row and left-joins its nodes. Pure.
///
/// No row: no workflow has the id (`None`). One row whose `workflow_task_id` is NULL: the
/// workflow exists and no node matched (`Some` of no rows). Otherwise every row is a node.
fn node_rows_of_workflow<'rows>(
    rows: &'rows [Row],
    statement: &TypedStatement,
) -> Result<Option<&'rows [Row]>, WorkflowError> {
    match rows {
        [] => Ok(None),
        [only] => {
            let node: Option<Uuid> = read_column(only, statement, "workflow_task_id")?;
            match node {
                None => Ok(Some(&[])),
                Some(_) => Ok(Some(rows)),
            }
        }
        _ => Ok(Some(rows)),
    }
}

/// Runs one read with the workflow id as `$1` and returns its rows.
///
/// Errors: [`WorkflowError::Broker`] for a failed checkout or statement (the
/// outcomes of the statement layer).
async fn read_rows(
    broker: &PostgresBroker,
    statement: &TypedStatement,
    workflow_id: Uuid,
) -> Result<Vec<Row>, WorkflowError> {
    Ok(run_alone(
        broker,
        CheckoutClass::Batch,
        &statement.bind().value(&workflow_id).finish(),
    )
    .await?)
}

/// A column of a read row. A value that does not decode breaks the read's
/// contract with its table: [`WorkflowError::StepContract`].
fn read_column<'row, T: FromSql<'row>>(
    row: &'row Row,
    statement: &TypedStatement,
    name: &'static str,
) -> Result<T, WorkflowError> {
    column(row, statement, name)
        .map_err(|error: RowDecodeError| WorkflowError::StepContract(error.into()))
}

/// The workflow row of [`WORKFLOW_RESULT`]. Pure.
fn workflow_result_row(row: &Row) -> Result<WorkflowResultRow, WorkflowError> {
    Ok(WorkflowResultRow {
        status: read_column(row, &WORKFLOW_RESULT, "status")?,
        result: read_column(row, &WORKFLOW_RESULT, "result")?,
        error: read_column(row, &WORKFLOW_RESULT, "error")?,
    })
}

/// One node row of [`WORKFLOW_NODES`]. Pure.
fn stored_node(row: &Row) -> Result<StoredNode, WorkflowError> {
    Ok(StoredNode {
        node_id: read_column(row, &WORKFLOW_NODES, "node_id")?,
        task_index: read_column(row, &WORKFLOW_NODES, "task_index")?,
        task_name: read_column(row, &WORKFLOW_NODES, "task_name")?,
        status: read_column(row, &WORKFLOW_NODES, "status")?,
        result: read_column(row, &WORKFLOW_NODES, "result")?,
        started_at: read_column(row, &WORKFLOW_NODES, "started_at")?,
        completed_at: read_column(row, &WORKFLOW_NODES, "completed_at")?,
        sub_workflow_id: read_column(row, &WORKFLOW_NODES, "sub_workflow_id")?,
        sub_workflow_summary: read_column(row, &WORKFLOW_NODES, "sub_workflow_summary")?,
        is_subworkflow: read_column(row, &WORKFLOW_NODES, "is_subworkflow")?,
        sub_definition_key: read_column(row, &WORKFLOW_NODES, "sub_definition_key")?,
    })
}

/// One node row of [`WORKFLOW_NODE_RESULTS`] or [`NODE_RESULT`]. Pure.
fn stored_node_result(
    row: &Row,
    statement: &TypedStatement,
    node_id: Option<String>,
) -> Result<StoredNodeResult, WorkflowError> {
    Ok(StoredNodeResult {
        node_id,
        task_name: read_column(row, statement, "task_name")?,
        result: read_column(row, statement, "result")?,
        is_subworkflow: read_column(row, statement, "is_subworkflow")?,
        sub_definition_key: read_column(row, statement, "sub_definition_key")?,
    })
}

/// The node rows of `workflow_id`, in index order: one read on the main core pool of `broker`
/// (Python `WorkflowHandle.tasks_async`).
///
/// Errors: [`WorkflowError::Broker`] for a failed read; [`WorkflowError::StepContract`] for a row
/// that does not decode.
pub async fn read_workflow_nodes(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<NodesRead, WorkflowError> {
    let rows = read_rows(broker, &WORKFLOW_NODES, workflow_id).await?;
    match node_rows_of_workflow(&rows, &WORKFLOW_NODES)? {
        None => Ok(NodesRead::WorkflowAbsent),
        Some(nodes) => Ok(NodesRead::Nodes(
            nodes.iter().map(stored_node).collect::<Result<_, _>>()?,
        )),
    }
}

/// The nodes of `workflow_id` that store a result: one read on the main core pool of `broker`
/// (Python `WorkflowHandle.results_async`).
///
/// Errors: as [`read_workflow_nodes`].
pub async fn read_workflow_node_results(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<NodeResultsRead, WorkflowError> {
    let rows = read_rows(broker, &WORKFLOW_NODE_RESULTS, workflow_id).await?;
    match node_rows_of_workflow(&rows, &WORKFLOW_NODE_RESULTS)? {
        None => Ok(NodeResultsRead::WorkflowAbsent),
        Some(nodes) => Ok(NodeResultsRead::Nodes(
            nodes
                .iter()
                .map(|row| {
                    let node_id = read_column(row, &WORKFLOW_NODE_RESULTS, "node_id")?;
                    stored_node_result(row, &WORKFLOW_NODE_RESULTS, node_id)
                })
                .collect::<Result<_, _>>()?,
        )),
    }
}

/// The stored result of the node `node_id` of `workflow_id`: one read on the main core pool of
/// `broker` (Python `WorkflowHandle.result_for_async`). `None`: no node of this id stores a
/// result, or no workflow has the id; the read does not tell the two apart, as Python's.
///
/// Errors: as [`read_workflow_nodes`].
pub async fn read_workflow_node_result(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    node_id: &str,
) -> Result<Option<StoredNodeResult>, WorkflowError> {
    let rows = run_alone(
        broker,
        CheckoutClass::Batch,
        &NODE_RESULT
            .bind()
            .value(&workflow_id)
            .value(&node_id)
            .finish(),
    )
    .await?;
    match first_row(rows) {
        None => Ok(None),
        Some(row) => Ok(Some(stored_node_result(
            &row,
            &NODE_RESULT,
            Some(node_id.to_owned()),
        )?)),
    }
}

/// The stored status text of `workflow_id`, or `None` when no workflow has
/// this id.
async fn stored_status(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<Option<String>, WorkflowError> {
    match first_row(read_rows(broker, &WORKFLOW_STATUS, workflow_id).await?) {
        Some(row) => Ok(Some(read_column(&row, &WORKFLOW_STATUS, "status")?)),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Get the current status of a workflow: one read on the main core pool of `broker`.
///
/// Errors: [`WorkflowError::WorkflowNotFound`]; [`WorkflowError::InvalidStatus`] for a stored status
/// outside the enum; [`WorkflowError::Broker`] for a failed read.
pub async fn get_workflow_status(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<WorkflowStatus, WorkflowError> {
    match stored_status(broker, workflow_id).await? {
        Some(status) => parse_workflow_status(&status),
        None => Err(WorkflowError::WorkflowNotFound { workflow_id }),
    }
}

/// Repoll interval of a workflow result wait with no listener (Python's
/// 1 s, `python/horsies/core/models/workflow/handle.py`, `WorkflowHandle.get`).
pub(crate) const WORKFLOW_RESULT_REPOLL_POLLING: Duration = Duration::from_secs(1);

/// Get the typed result of a workflow, optionally waiting for completion: the wait of
/// [`wait_for_terminal_row`], then the decode of the row with Python's codes
/// ([`parse_workflow_result`]).
///
/// A missing workflow answers `WORKFLOW_NOT_FOUND`; a wait past `timeout` answers `WAIT_TIMEOUT`.
pub async fn get_workflow_result<T: DeserializeOwned>(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    timeout: Option<Duration>,
) -> Result<TaskResult<T>, WorkflowError> {
    match wait_for_terminal_row(broker, workflow_id, timeout).await? {
        TerminalWait::Terminal(row) => Ok(parse_workflow_result::<T>(workflow_id, &row)),
        TerminalWait::NotFound => Ok(TaskResult::Err(TaskError::builtin(
            RetrievalCode::WorkflowNotFound,
            format!("workflow {} not found", workflow_id),
        ))),
        TerminalWait::TimedOut { waited } => Ok(TaskResult::Err(TaskError::builtin(
            RetrievalCode::WaitTimeout,
            format!(
                "workflow {} not terminal after {}ms",
                workflow_id,
                waited.as_millis()
            ),
        ))),
    }
}

/// The end of a workflow, with its stored values as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowEnd {
    /// COMPLETED, with the stored result envelope (NULL or empty only when a write outside the
    /// library stored it so).
    Completed { result: Option<String> },
    /// FAILED, CANCELLED or EXPIRED, with the stored error.
    Ended {
        status: TerminalErrorStatus,
        error: Option<String>,
    },
    /// PAUSED: the workflow waits for a resume.
    Paused,
}

/// What a wait for the end of a workflow found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowWait {
    /// The workflow is COMPLETED, FAILED, CANCELLED, EXPIRED or PAUSED.
    Ended(WorkflowEnd),
    /// No workflow has the id.
    NotFound,
    /// The workflow is PENDING or RUNNING after the timeout.
    TimedOut,
}

/// Wait for the end of a workflow, with its stored values as text (Python
/// `WorkflowHandle.get_async`, before its decode): the wait of [`wait_for_terminal_row`]. The
/// client decodes the stored result with its own output type.
///
/// Errors: as [`wait_for_terminal_row`].
pub async fn wait_workflow_end(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    timeout: Option<Duration>,
) -> Result<WorkflowWait, WorkflowError> {
    match wait_for_terminal_row(broker, workflow_id, timeout).await? {
        TerminalWait::Terminal(row) => Ok(WorkflowWait::Ended(workflow_end(workflow_id, row)?)),
        TerminalWait::NotFound => Ok(WorkflowWait::NotFound),
        TerminalWait::TimedOut { .. } => Ok(WorkflowWait::TimedOut),
    }
}

/// The end of a row that [`try_fetch_terminal_result`] returned as terminal. Pure.
///
/// Errors: [`WorkflowError::InvalidStatus`] for a stored status outside the enum.
///
/// Panics: a PENDING or RUNNING row, which the fetch never returns as terminal (a defect).
fn workflow_end(workflow_id: Uuid, row: WorkflowResultRow) -> Result<WorkflowEnd, WorkflowError> {
    match parse_workflow_status(&row.status)? {
        WorkflowStatus::Completed => Ok(WorkflowEnd::Completed { result: row.result }),
        WorkflowStatus::Failed => Ok(WorkflowEnd::Ended {
            status: TerminalErrorStatus::Failed,
            error: row.error,
        }),
        WorkflowStatus::Cancelled => Ok(WorkflowEnd::Ended {
            status: TerminalErrorStatus::Cancelled,
            error: row.error,
        }),
        WorkflowStatus::Expired => Ok(WorkflowEnd::Ended {
            status: TerminalErrorStatus::Expired,
            error: row.error,
        }),
        WorkflowStatus::Paused => Ok(WorkflowEnd::Paused),
        WorkflowStatus::Pending | WorkflowStatus::Running => panic!(
            "the wait for workflow {workflow_id} took a {} row as terminal (a defect)",
            row.status
        ),
    }
}

/// What [`wait_for_terminal_row`] found.
#[derive(Debug)]
enum TerminalWait {
    Terminal(WorkflowResultRow),
    NotFound,
    TimedOut { waited: Duration },
}

/// Wait until the workflow row is COMPLETED, FAILED, CANCELLED, EXPIRED or PAUSED, or until
/// `timeout` (none: no limit), with the pattern of `PostgresBroker::get_raw_result_record`:
/// 1. read the row; a terminal or absent row returns at once;
/// 2. subscribe a wake for the workflow id on `workflow_done` on the broker's listener;
/// 3. read again (race guard);
/// 4. wait for a wake, a listener gap, the repoll interval or the deadline, then read again;
///    at the deadline one last read decides.
///
/// A finished workflow is one read. A broker with no listener, or a refused subscription, makes
/// the wait poll every [`WORKFLOW_RESULT_REPOLL_POLLING`], as Python; a subscribed wait reads
/// again at least every [`WORKFLOW_RESULT_REPOLL_SUBSCRIBED`].
///
/// Errors: [`WorkflowError::Broker`] for a failed read, and `BrokerError::ListenerClosed` when the
/// listener stops during the wait; [`WorkflowError::InvalidStatus`] for a stored status outside
/// the enum.
async fn wait_for_terminal_row(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    timeout: Option<Duration>,
) -> Result<TerminalWait, WorkflowError> {
    let start = Instant::now();
    match try_fetch_terminal_result(broker, workflow_id).await? {
        TerminalFetch::Terminal(row) => return Ok(TerminalWait::Terminal(row)),
        TerminalFetch::NotFound => return Ok(TerminalWait::NotFound),
        TerminalFetch::NotTerminal => {}
    }

    let wait_mode = WorkflowWaitMode::subscribe(broker, workflow_id).await;

    // Re-check after subscribing (race guard).
    match try_fetch_terminal_result(broker, workflow_id).await? {
        TerminalFetch::Terminal(row) => return Ok(TerminalWait::Terminal(row)),
        TerminalFetch::NotFound => return Ok(TerminalWait::NotFound),
        TerminalFetch::NotTerminal => {}
    }

    let deadline = timeout.map(|t| start + t);

    loop {
        // For a timed wait, one last read once the deadline has passed.
        if let Some(d) = deadline {
            if Instant::now() >= d {
                return match try_fetch_terminal_result(broker, workflow_id).await? {
                    TerminalFetch::Terminal(row) => Ok(TerminalWait::Terminal(row)),
                    TerminalFetch::NotFound => Ok(TerminalWait::NotFound),
                    TerminalFetch::NotTerminal => Ok(TerminalWait::TimedOut {
                        waited: start.elapsed(),
                    }),
                };
            }
        }

        // Cap each wait at the re-poll interval so a lost NOTIFY is recovered
        // by a fresh poll rather than hanging the no-timeout wait forever.
        // Never wait past the deadline.
        let interval = wait_mode.repoll_interval();
        let wait = match deadline {
            Some(d) => d.saturating_duration_since(Instant::now()).min(interval),
            None => interval,
        };

        // A wake, a gap or the end of the interval reads again; a stopped
        // listener fails the wait.
        wait_mode.pause(wait).await?;
        match try_fetch_terminal_result(broker, workflow_id).await? {
            TerminalFetch::Terminal(row) => return Ok(TerminalWait::Terminal(row)),
            TerminalFetch::NotFound => return Ok(TerminalWait::NotFound),
            TerminalFetch::NotTerminal => {}
        }
    }
}

/// Repoll interval of a subscribed workflow result wait: a lost `workflow_done` wake costs at
/// most this much (Python's 5 s, `WorkflowHandle.get_async`).
pub(crate) const WORKFLOW_RESULT_REPOLL_SUBSCRIBED: Duration = Duration::from_secs(5);

/// How a workflow result wait learns that the workflow may have ended.
enum WorkflowWaitMode {
    /// A wake for the workflow id on `workflow_done`; a listener gap also
    /// wakes it. Reads again at least every [`WORKFLOW_RESULT_REPOLL_SUBSCRIBED`].
    Subscribed(WakeSubscription),
    /// No listener: reads again every [`WORKFLOW_RESULT_REPOLL_POLLING`].
    Polling,
}

impl WorkflowWaitMode {
    /// Subscribe the wait on the broker's listener. A broker with no
    /// listener, or a refused subscription, polls instead, as Python.
    async fn subscribe(broker: &PostgresBroker, workflow_id: Uuid) -> WorkflowWaitMode {
        let filter = PayloadFilter::Payload(workflow_id.to_string());
        match broker
            .listener()
            .subscribe_wake(fixed_channel(WORKFLOW_DONE_CHANNEL), filter)
            .await
        {
            Ok(subscription) => WorkflowWaitMode::Subscribed(subscription),
            Err(error @ ListenError::NoSessionUrl) => {
                tracing::debug!(%workflow_id, error = %error, "the workflow wait polls: {error}");
                WorkflowWaitMode::Polling
            }
            Err(error @ (ListenError::PlanRefused(_) | ListenError::Subscribe(_))) => {
                tracing::warn!(
                    %workflow_id,
                    error = %error,
                    "LISTEN unavailable; falling back to polling for workflow_done. If using \
                     PgBouncer transaction pooling, configure PostgresConfig.session_database_url \
                     with a direct/session-capable Postgres URL. Original error: {error}",
                );
                WorkflowWaitMode::Polling
            }
        }
    }

    fn repoll_interval(&self) -> Duration {
        match self {
            WorkflowWaitMode::Subscribed(_) => WORKFLOW_RESULT_REPOLL_SUBSCRIBED,
            WorkflowWaitMode::Polling => WORKFLOW_RESULT_REPOLL_POLLING,
        }
    }

    /// Pause until a wake, a gap, or until `wait` passes. A stopped listener
    /// is `BrokerError::ListenerClosed`.
    async fn pause(&self, wait: Duration) -> Result<(), WorkflowError> {
        match self {
            WorkflowWaitMode::Subscribed(subscription) => {
                match tokio::time::timeout(wait, subscription.recv()).await {
                    Ok(WakeEvent::Notified | WakeEvent::Gap) | Err(_) => Ok(()),
                    Ok(WakeEvent::Stopped) => {
                        Err(WorkflowError::Broker(BrokerError::ListenerClosed))
                    }
                }
            }
            WorkflowWaitMode::Polling => {
                tokio::time::sleep(wait).await;
                Ok(())
            }
        }
    }
}

/// Get all task results keyed by node_id: one read on the main core pool of `broker`
/// ([`read_workflow_node_results`]). A node with no id is left out.
///
/// Errors: [`WorkflowError::WorkflowNotFound`] when no workflow has the id (Python
/// `WorkflowHandle.results_async`); the errors of the read.
pub async fn get_workflow_results(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<HashMap<String, TaskResult<serde_json::Value>>, WorkflowError> {
    let nodes = match read_workflow_node_results(broker, workflow_id).await? {
        NodeResultsRead::WorkflowAbsent => {
            return Err(WorkflowError::WorkflowNotFound { workflow_id });
        }
        NodeResultsRead::Nodes(nodes) => nodes,
    };

    let mut results = HashMap::new();
    for node in nodes {
        let Some(node_id) = node.node_id else {
            continue;
        };
        let parsed = match serde_json::from_str::<TaskResult<serde_json::Value>>(&node.result) {
            Ok(value) => value,
            Err(e) => TaskResult::Err(TaskError::builtin(
                OperationalErrorCode::ResultDeserializationError,
                format!(
                    "failed to parse workflow result for node_id '{}': {}",
                    node_id, e
                ),
            )),
        };
        results.insert(node_id, parsed);
    }

    Ok(results)
}

/// Get a single typed result by node key: the node read, and the workflow read when the node has no
/// result, on the main core pool of `broker`.
pub async fn get_workflow_result_for<T: DeserializeOwned>(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    node_id: &str,
) -> Result<TaskResult<T>, WorkflowError> {
    let Some(stored) = read_workflow_node_result(broker, workflow_id, node_id).await? else {
        if stored_status(broker, workflow_id).await?.is_none() {
            return Ok(TaskResult::Err(TaskError::builtin(
                RetrievalCode::WorkflowNotFound,
                format!("workflow {} not found", workflow_id),
            )));
        }
        return Ok(TaskResult::Err(TaskError::builtin(
            RetrievalCode::ResultNotReady,
            format!(
                "result not ready for workflow {} node {}",
                workflow_id, node_id
            ),
        )));
    };

    let parsed: TaskResult<serde_json::Value> = match serde_json::from_str(&stored.result) {
        Ok(value) => value,
        Err(e) => {
            return Ok(TaskResult::Err(TaskError::builtin(
                OperationalErrorCode::ResultDeserializationError,
                format!(
                    "failed to parse workflow result for node_id '{}': {}",
                    node_id, e
                ),
            )));
        }
    };
    Ok(map_task_result_value(parsed, workflow_id, Some(node_id)))
}

/// Get all tasks in a workflow with their metadata (Python `WorkflowHandle.tasks_async`).
///
/// Errors (the handle catalog):
/// - `WORKFLOW_NOT_FOUND`: no workflow has the id (Python's rule; a workflow with no node is
///   `Ok` of no task).
/// - `DB_OPERATION_FAILED`: the read failed (retryable by the core's rule).
/// - `STORED_DATA_INVALID`, not retryable: a stored node status outside the enum (the message names the
///   workflow, the node and the value, `parse_task_status`), or a stored sub-workflow summary that is
///   not JSON (`sub_workflow_summary JSON corrupt for node ...`) or that breaks Python's summary rules
///   (`sub_workflow_summary invalid for node ...`, [`SubWorkflowSummary::from_stored_value`]). A summary
///   is never read as another status.
pub async fn get_workflow_tasks(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> HandleResult<Vec<WorkflowTaskInfo>> {
    let read = read_workflow_nodes(broker, workflow_id)
        .await
        .map_err(|error| handle_operation_error(&error, workflow_id))?;
    match read {
        NodesRead::WorkflowAbsent => Err(handle_operation_error(
            &WorkflowError::WorkflowNotFound { workflow_id },
            workflow_id,
        )),
        NodesRead::Nodes(nodes) => nodes
            .into_iter()
            .map(|row| workflow_task_info(workflow_id, row))
            .collect(),
    }
}

/// One node row as a [`WorkflowTaskInfo`]. Errors as [`get_workflow_tasks`]. Pure.
fn workflow_task_info(workflow_id: Uuid, row: StoredNode) -> HandleResult<WorkflowTaskInfo> {
    let status = parse_task_status(workflow_id, row.node_id.as_deref(), &row.status)?;
    // Python reads a summary only when the column is a non-empty text (`if row.sub_workflow_summary:`).
    let sub_workflow_summary = match row.sub_workflow_summary.as_deref() {
        None | Some("") => None,
        Some(text) => Some(stored_summary(workflow_id, row.node_id.as_deref(), text)?),
    };
    let result = row
        .result
        .and_then(|json| serde_json::from_str::<TaskResult<serde_json::Value>>(&json).ok());
    Ok(WorkflowTaskInfo {
        node_id: row.node_id,
        index: row.task_index,
        name: row.task_name,
        status,
        result,
        started_at: row.started_at,
        completed_at: row.completed_at,
        sub_workflow_id: row.sub_workflow_id,
        sub_workflow_summary,
    })
}

/// A stored sub-workflow summary, read with Python's rules (`WorkflowHandle.tasks_async`,
/// `core/models/workflow/handle.py`). A text that is not JSON or a value that the rules refuse is
/// `STORED_DATA_INVALID` (`HandleStoredDataInvalid`), not retryable. Pure.
fn stored_summary(
    workflow_id: Uuid,
    node_id: Option<&str>,
    text: &str,
) -> HandleResult<SubWorkflowSummary> {
    // Python formats a node with no id as `None`.
    let node = node_id.unwrap_or("None");
    let invalid = |message: String| HandleOperationError {
        code: HandleErrorCode::StoredDataInvalid,
        message,
        retryable: false,
        workflow_id,
        cause: crate::failure_cause::FailureCause::ContractViolation,
    };
    let value = crate::core::codec::from_json_str::<serde_json::Value>(text).map_err(|error| {
        invalid(format!(
            "sub_workflow_summary JSON corrupt for node {node}: {error}"
        ))
    })?;
    SubWorkflowSummary::from_stored_value(&value).map_err(|refused| {
        invalid(format!(
            "sub_workflow_summary invalid for node {node}: {refused}"
        ))
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum TerminalFetch {
    Terminal(WorkflowResultRow),
    NotTerminal,
    NotFound,
}

/// Try to fetch a terminal workflow result. Returns None if not terminal yet.
async fn try_fetch_terminal_result(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<TerminalFetch, WorkflowError> {
    let row = match first_row(read_rows(broker, &WORKFLOW_RESULT, workflow_id).await?) {
        Some(row) => Some(workflow_result_row(&row)?),
        None => None,
    };

    match row {
        Some(r) => match parse_workflow_status(&r.status)? {
            WorkflowStatus::Completed
            | WorkflowStatus::Failed
            | WorkflowStatus::Paused
            | WorkflowStatus::Cancelled
            | WorkflowStatus::Expired => Ok(TerminalFetch::Terminal(r)),
            WorkflowStatus::Pending | WorkflowStatus::Running => Ok(TerminalFetch::NotTerminal),
        },
        None => Ok(TerminalFetch::NotFound),
    }
}

/// Parse a terminal workflow result into a typed TaskResult, with Python's codes
/// (`WorkflowHandle._get_result` and `_get_error`, `core/models/workflow/handle.py`):
///
/// - COMPLETED with a NULL or empty result: `RESULT_DESERIALIZATION_ERROR` with
///   `data.workflow_id` (the library always stores a result with COMPLETED).
/// - FAILED, CANCELLED or EXPIRED: the stored error ([`stored_workflow_error`]).
/// - PAUSED: `WORKFLOW_PAUSED`. Any other status: `RESULT_NOT_READY`.
fn parse_workflow_result<T: DeserializeOwned>(
    workflow_id: Uuid,
    row: &WorkflowResultRow,
) -> TaskResult<T> {
    match row.status.as_str() {
        "COMPLETED" => match row.result.as_deref() {
            None => TaskResult::Err(completed_without_stored_result(workflow_id, "NULL")),
            Some("") => TaskResult::Err(completed_without_stored_result(workflow_id, "empty")),
            Some(json) => match read_completed_result(workflow_id, json) {
                Ok(raw) => map_task_result_value(raw, workflow_id, None),
                Err(error) => TaskResult::Err(error),
            },
        },
        "FAILED" => TaskResult::Err(stored_workflow_error(
            workflow_id,
            TerminalErrorStatus::Failed,
            row.error.as_deref(),
        )),
        "CANCELLED" => TaskResult::Err(stored_workflow_error(
            workflow_id,
            TerminalErrorStatus::Cancelled,
            row.error.as_deref(),
        )),
        "EXPIRED" => TaskResult::Err(stored_workflow_error(
            workflow_id,
            TerminalErrorStatus::Expired,
            row.error.as_deref(),
        )),
        "PAUSED" => TaskResult::Err(TaskError::builtin(
            OutcomeCode::WorkflowPaused,
            "workflow is paused awaiting intervention",
        )),
        _ => TaskResult::Err(TaskError::builtin(
            RetrievalCode::ResultNotReady,
            format!("workflow {} result not ready", workflow_id),
        )),
    }
}

/// A terminal workflow status whose `get()` answer is an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalErrorStatus {
    Failed,
    Cancelled,
    Expired,
}

impl TerminalErrorStatus {
    /// The stored status text.
    pub fn wire_value(self) -> &'static str {
        match self {
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Expired => "EXPIRED",
        }
    }

    /// The code of the status when no error is stored (Python `_terminal_outcome_code`).
    fn outcome_code(self) -> OutcomeCode {
        match self {
            Self::Failed => OutcomeCode::WorkflowFailed,
            Self::Cancelled => OutcomeCode::WorkflowCancelled,
            Self::Expired => OutcomeCode::WorkflowExpired,
        }
    }

    /// Python's message for the status with no stored error (`f'Workflow {status.value.lower()}'`).
    fn no_error_message(self) -> &'static str {
        match self {
            Self::Failed => "Workflow failed",
            Self::Cancelled => "Workflow cancelled",
            Self::Expired => "Workflow expired",
        }
    }
}

/// `RESULT_DESERIALIZATION_ERROR` for a COMPLETED workflow whose stored result is NULL or empty
/// (Python `_completed_without_stored_result`). `stored_form` is `NULL` or `empty`.
fn completed_without_stored_result(workflow_id: Uuid, stored_form: &str) -> TaskError {
    unreadable_workflow_value(
        workflow_id,
        format!("Workflow {workflow_id} is COMPLETED and its stored result is {stored_form}"),
    )
}

/// The `get()` error of a FAILED, CANCELLED or EXPIRED workflow (Python `_get_error` and
/// `_decode_stored_workflow_error`):
///
/// - no stored error: the code of `status` ([`TerminalErrorStatus::outcome_code`]);
/// - an empty stored error: `RESULT_DESERIALIZATION_ERROR`;
/// - an error object that decodes ([`TaskError::from_stored_value`]): that error, for every status, a
///   cancel included;
/// - a text that is not JSON, JSON that is not an object, or an object that does not decode:
///   `RESULT_DESERIALIZATION_ERROR` with the cause. The plain expiry text of 0.5.0 and 0.5.1 is refused like
///   any other text: the clean slate keeps no 0.x stored values.
///
/// Every `RESULT_DESERIALIZATION_ERROR` carries `data.workflow_id`. Pure, except for one warning log record
/// per refused stored error.
fn stored_workflow_error(
    workflow_id: Uuid,
    status: TerminalErrorStatus,
    stored: Option<&str>,
) -> TaskError {
    match stored {
        None => TaskError::builtin(status.outcome_code(), status.no_error_message()),
        Some("") => {
            tracing::warn!(%workflow_id, "workflow error payload is empty");
            unreadable_workflow_value(workflow_id, "Workflow error payload is empty".to_owned())
        }
        Some(text) => decode_stored_workflow_error(workflow_id, text),
    }
}

/// Decodes a stored workflow error that is not NULL and not empty. See [`stored_workflow_error`].
fn decode_stored_workflow_error(workflow_id: Uuid, text: &str) -> TaskError {
    let refusal = match crate::core::codec::from_json_str::<serde_json::Value>(text) {
        Err(error) => format!("Workflow error payload corrupt: {error}"),
        Ok(value @ serde_json::Value::Object(_)) => match TaskError::from_stored_value(value) {
            Ok(error) => return error,
            Err(refused) => format!("Workflow error payload validation failed: {refused}"),
        },
        Ok(other) => format!(
            "Workflow error payload is not a JSON object: got {}",
            crate::core::workflow::summary::python_type_name(Some(&other)),
        ),
    };
    tracing::warn!(%workflow_id, cause = %refusal, "workflow error payload does not decode");
    unreadable_workflow_value(workflow_id, refusal)
}

/// `RESULT_DESERIALIZATION_ERROR` with `message` and `data.workflow_id`.
fn unreadable_workflow_value(workflow_id: Uuid, message: String) -> TaskError {
    TaskError {
        data: Some(serde_json::json!({ "workflow_id": workflow_id.to_string() })),
        ..TaskError::builtin(OperationalErrorCode::ResultDeserializationError, message)
    }
}

/// The stored result of a COMPLETED workflow. A result with the outputless marker
/// (`OUTPUTLESS_TERMINALS_KEY`) is read by [`outputless_entries`]; any other text is a result envelope.
fn read_completed_result(
    workflow_id: Uuid,
    text: &str,
) -> Result<TaskResult<serde_json::Value>, TaskError> {
    let unreadable = |cause: String| {
        TaskError::builtin(
            OperationalErrorCode::ResultDeserializationError,
            format!("failed to parse workflow result: {cause}"),
        )
    };
    let value: serde_json::Value =
        crate::core::codec::from_json_str(text).map_err(|e| unreadable(e.to_string()))?;
    match value.get(OUTPUTLESS_TERMINALS_KEY) == Some(&serde_json::Value::Bool(true)) {
        true => Ok(outputless_entries(workflow_id, value)),
        false => serde_json::from_value(value).map_err(|e| unreadable(e.to_string())),
    }
}

/// The result of a workflow with no output node: an ok map of the terminal nodes' entries, each a result
/// envelope. Python's handle (`core/models/workflow/handle.py:600-735`):
/// - an envelope without the result marker, or a malformed `ok` slot, is `RESULT_DESERIALIZATION_ERROR`;
/// - a non-null `err` slot is that error;
/// - an entry that is `null`, has no task name, or is not a result envelope becomes a
///   `RESULT_DESERIALIZATION_ERROR` entry naming the node; the other entries stay readable.
fn outputless_entries(
    workflow_id: Uuid,
    value: serde_json::Value,
) -> TaskResult<serde_json::Value> {
    let malformed = |message: String| -> TaskResult<serde_json::Value> {
        TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::ResultDeserializationError,
            message,
        ))
    };
    let serde_json::Value::Object(mut envelope) = value else {
        return malformed(format!(
            "Workflow {workflow_id} result envelope is not a dict"
        ));
    };
    if envelope.get(TASK_RESULT_ENVELOPE_KEY) != Some(&serde_json::Value::Bool(true)) {
        return malformed(format!(
            "Workflow {workflow_id} result missing __h_task_result__ envelope marker"
        ));
    }
    if let Some(err_slot) = envelope.remove("err").filter(|slot| !slot.is_null()) {
        return match TaskError::from_stored_value(err_slot) {
            Ok(error) => TaskResult::Err(error),
            Err(error) => malformed(format!("TaskError decode failed: {error}")),
        };
    }
    let Some(serde_json::Value::Object(mut ok_slot)) = envelope.remove("ok") else {
        return malformed("Outputless workflow envelope ok slot malformed".to_owned());
    };
    let maps = (
        ok_slot.remove("results_by_id"),
        ok_slot.remove("task_name_by_id"),
        ok_slot.remove("definition_key_by_id"),
    );
    let (results, task_names) = match maps {
        (
            None | Some(serde_json::Value::Object(_)),
            None | Some(serde_json::Value::Object(_)),
            None | Some(serde_json::Value::Object(_)),
        ) => (object_or_empty(maps.0), object_or_empty(maps.1)),
        _ => return malformed(
            "Outputless workflow envelope missing or malformed results_by_id / task_name_by_id / \
                 definition_key_by_id"
                .to_owned(),
        ),
    };
    let mut entries = serde_json::Map::with_capacity(results.len());
    for (node_id, entry) in results {
        let has_task_name = matches!(task_names.get(&node_id), Some(serde_json::Value::String(_)));
        let read = match (entry, has_task_name) {
            (serde_json::Value::Null, _) | (_, false) => Err(format!(
                "Outputless workflow {workflow_id} node '{node_id}' missing payload or task name"
            )),
            (entry, true) => serde_json::from_value::<TaskResult<serde_json::Value>>(entry)
                .map_err(|error| {
                    format!("decode_task_result failed for node '{node_id}': {error}")
                }),
        };
        let result = read.unwrap_or_else(|message| {
            TaskResult::Err(TaskError {
                data: Some(serde_json::json!({ "node_id": node_id })),
                ..TaskError::builtin(OperationalErrorCode::ResultDeserializationError, message)
            })
        });
        match serde_json::to_value(&result) {
            Ok(entry) => {
                entries.insert(node_id, entry);
            }
            Err(error) => {
                return malformed(format!(
                    "Outputless workflow {workflow_id} node '{node_id}' does not encode: {error}"
                ))
            }
        }
    }
    TaskResult::Ok(serde_json::Value::Object(entries))
}

fn object_or_empty(value: Option<serde_json::Value>) -> serde_json::Map<String, serde_json::Value> {
    match value {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) | None => serde_json::Map::new(),
    }
}

fn map_task_result_value<T: DeserializeOwned>(
    raw: TaskResult<serde_json::Value>,
    workflow_id: Uuid,
    node_id: Option<&str>,
) -> TaskResult<T> {
    let scope = match node_id {
        Some(id) => format!("workflow '{}' node '{}'", workflow_id, id),
        None => format!("workflow '{}'", workflow_id),
    };
    match raw {
        TaskResult::Ok(value) => match serde_json::from_value(value) {
            Ok(parsed) => TaskResult::Ok(parsed),
            Err(e) => TaskResult::Err(TaskError::builtin(
                OperationalErrorCode::ResultDeserializationError,
                format!("failed to deserialize result for {}: {}", scope, e),
            )),
        },
        TaskResult::Err(err) => TaskResult::Err(err),
    }
}

fn parse_workflow_status(s: &str) -> Result<WorkflowStatus, WorkflowError> {
    WorkflowStatus::try_from(s).map_err(WorkflowError::InvalidStatus)
}

/// The status that a node row stores (Python `stored_node_status`, `core/models/workflow/enums.py`).
///
/// A value outside [`WorkflowTaskStatus`] is `STORED_DATA_INVALID`, not retryable: the same read meets the
/// same row. The message names the workflow, the node (`None` for a row with no node id, as Python
/// formats it) and the value in Python's `repr`. The baseline CHECK `horsies_workflow_tasks_status_known`
/// forbids such a value, so the row is corrupt. Pure.
fn parse_task_status(
    workflow_id: Uuid,
    node_id: Option<&str>,
    stored: &str,
) -> HandleResult<WorkflowTaskStatus> {
    match stored {
        "PENDING" => Ok(WorkflowTaskStatus::Pending),
        "READY" => Ok(WorkflowTaskStatus::Ready),
        "ENQUEUED" => Ok(WorkflowTaskStatus::Enqueued),
        "RUNNING" => Ok(WorkflowTaskStatus::Running),
        "COMPLETED" => Ok(WorkflowTaskStatus::Completed),
        "FAILED" => Ok(WorkflowTaskStatus::Failed),
        "SKIPPED" => Ok(WorkflowTaskStatus::Skipped),
        unknown => Err(HandleOperationError {
            code: HandleErrorCode::StoredDataInvalid,
            message: format!(
                "workflow {workflow_id} node {} stores status {}, which is not a \
                 WorkflowTaskStatus member; the CHECK horsies_workflow_tasks_status_known \
                 must forbid it",
                node_id.unwrap_or("None"),
                crate::core::python_text::python_repr(unknown),
            ),
            retryable: false,
            workflow_id,
            cause: crate::failure_cause::FailureCause::ContractViolation,
        }),
    }
}

#[cfg(test)]
fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "COMPLETED" | "FAILED" | "PAUSED" | "CANCELLED" | "EXPIRED"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{OperationalErrorCode, OutcomeCode, RetrievalCode, TaskErrorCode};

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    fn row(status: &str, result: Option<&str>, error: Option<&str>) -> WorkflowResultRow {
        WorkflowResultRow {
            status: status.to_owned(),
            result: result.map(|s| s.to_owned()),
            error: error.map(|s| s.to_owned()),
        }
    }

    // -----------------------------------------------------------------------
    // parse_workflow_result
    // -----------------------------------------------------------------------

    #[test]
    fn completed_with_valid_result() {
        let wrapped = TaskResult::Ok(42);
        let json = serde_json::to_string(&wrapped).unwrap();
        let r = row("COMPLETED", Some(&json), None);

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        assert_eq!(result.unwrap(), 42);
    }

    /// Python's outputless result (`core/workflows/engine.py:3973-3983`) reads as a map of the terminal
    /// nodes' entries; an entry that does not read names its node (`core/models/workflow/handle.py:672-735`).
    #[test]
    fn an_outputless_result_reads_as_a_map_of_entries() {
        let text = serde_json::json!({
            "__h_task_result__": true,
            "__h_outputless_terminals__": true,
            "ok": {
                "results_by_id": {
                    "a": {"__h_task_result__": true, "ok": 1, "err": null},
                    "b": null,
                    "c": {"not": "an envelope"},
                    "d": {"__h_task_result__": true, "ok": 2, "err": null},
                },
                "task_name_by_id": {"a": "t", "b": "t", "c": "t"},
                "definition_key_by_id": {"a": null, "b": null, "c": null, "d": null},
            },
            "err": null,
        })
        .to_string();
        let r = row("COMPLETED", Some(&text), None);
        let result: TaskResult<HashMap<String, TaskResult<i32>>> =
            parse_workflow_result(Uuid::nil(), &r);
        let entries = result.unwrap();
        assert_eq!(entries["a"].clone().unwrap(), 1);
        for node in ["b", "c", "d"] {
            let error = entries[node].clone().unwrap_err();
            assert_eq!(
                error.error_code.to_string(),
                "RESULT_DESERIALIZATION_ERROR",
                "{node}"
            );
            assert_eq!(error.data, Some(serde_json::json!({ "node_id": node })));
        }
    }

    #[test]
    fn a_malformed_outputless_result_is_refused() {
        for ok in [
            serde_json::json!(5),
            serde_json::json!({"results_by_id": [1]}),
        ] {
            let text = serde_json::json!({
                "__h_task_result__": true,
                "__h_outputless_terminals__": true,
                "ok": ok,
                "err": null,
            })
            .to_string();
            let r = row("COMPLETED", Some(&text), None);
            let result: TaskResult<serde_json::Value> = parse_workflow_result(Uuid::nil(), &r);
            assert_eq!(
                result.unwrap_err().error_code,
                TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError)
            );
        }
    }

    /// Python `_completed_without_stored_result`: the library always stores a
    /// result with COMPLETED, so a NULL or empty one is stored data that does
    /// not parse.
    #[test]
    fn completed_with_null_result() {
        let workflow_id = Uuid::new_v4();
        for stored in [None, Some("")] {
            let r = row("COMPLETED", stored, None);

            let result: TaskResult<i32> = parse_workflow_result(workflow_id, &r);
            let err = result.unwrap_err();
            assert_eq!(
                err.error_code,
                TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError),
                "{stored:?}",
            );
            assert_eq!(
                err.data,
                Some(serde_json::json!({"workflow_id": workflow_id.to_string()})),
            );
        }
    }

    #[test]
    fn completed_with_malformed_json() {
        let r = row("COMPLETED", Some("{not valid json"), None);

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError,),
        );
    }

    #[test]
    fn completed_with_wrong_type() {
        // Result is a string but we try to deserialize as i32.
        let wrapped = TaskResult::Ok("hello".to_owned());
        let json = serde_json::to_string(&wrapped).unwrap();
        let r = row("COMPLETED", Some(&json), None);

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError,),
        );
    }

    #[test]
    fn failed_with_valid_error() {
        let task_err = TaskError::new("VALIDATION", "bad input").unwrap();
        let err_json = serde_json::to_string(&task_err).unwrap();
        let r = row("FAILED", None, Some(&err_json));

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::User(crate::core::UserCode::parse("VALIDATION").unwrap()),
        );
        assert_eq!(err.message.as_deref(), Some("bad input"));
    }

    /// Python `_decode_stored_workflow_error`: a stored error that is not JSON,
    /// not an object, or not a `TaskError` is `RESULT_DESERIALIZATION_ERROR`.
    #[test]
    fn failed_with_malformed_error_json() {
        let workflow_id = Uuid::new_v4();
        for stored in ["{garbage", "[1]", r#"{"error_code": 5}"#] {
            let r = row("FAILED", None, Some(stored));

            let result: TaskResult<i32> = parse_workflow_result(workflow_id, &r);
            let err = result.unwrap_err();
            assert_eq!(
                err.error_code,
                TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError),
                "{stored}",
            );
            assert_eq!(
                err.data,
                Some(serde_json::json!({"workflow_id": workflow_id.to_string()})),
            );
        }
    }

    /// Python `_terminal_outcome_code`: a FAILED workflow with no stored
    /// error is `WORKFLOW_FAILED`, not a broker error.
    #[test]
    fn failed_with_null_error() {
        let r = row("FAILED", None, None);

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OutcomeCode::WorkflowFailed),
        );
    }

    /// Python `_decode_stored_workflow_error`: the stored error of a terminal
    /// workflow is its answer for every status, a cancel included; an empty one
    /// is `RESULT_DESERIALIZATION_ERROR`.
    #[test]
    fn a_stored_error_is_the_answer_of_every_terminal_error_status() {
        let workflow_id = Uuid::new_v4();
        let stored = TaskError::new("STOPPED_BY_OPERATOR", "stopped").unwrap();
        let stored_json = serde_json::to_string(&stored).unwrap();
        for status in ["FAILED", "CANCELLED", "EXPIRED"] {
            let result: TaskResult<i32> =
                parse_workflow_result(workflow_id, &row(status, None, Some(&stored_json)));
            assert_eq!(result.unwrap_err(), stored, "{status}");

            let empty: TaskResult<i32> =
                parse_workflow_result(workflow_id, &row(status, None, Some("")));
            let empty = empty.unwrap_err();
            assert_eq!(
                empty.error_code,
                TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError),
                "{status}",
            );
            assert_eq!(
                empty.data,
                Some(serde_json::json!({"workflow_id": workflow_id.to_string()})),
            );
        }
    }

    #[test]
    fn cancelled_produces_workflow_cancelled() {
        let r = row("CANCELLED", None, None);

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OutcomeCode::WorkflowCancelled),
        );
    }

    #[test]
    fn expired_is_terminal_and_produces_workflow_expired() {
        let r = row("EXPIRED", None, None);

        assert_eq!(
            parse_workflow_status("EXPIRED").unwrap(),
            WorkflowStatus::Expired
        );
        assert!(is_terminal("EXPIRED"));

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OutcomeCode::WorkflowExpired),
        );
    }

    #[test]
    fn paused_produces_workflow_paused() {
        let r = row("PAUSED", None, None);

        assert!(is_terminal("PAUSED"));

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OutcomeCode::WorkflowPaused),
        );
    }

    #[test]
    fn unknown_status_produces_result_not_ready() {
        let r = row("RUNNING", None, None);

        let result: TaskResult<i32> = parse_workflow_result(Uuid::nil(), &r);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(RetrievalCode::ResultNotReady),
        );
        assert!(err
            .message
            .as_deref()
            .unwrap()
            .contains(&Uuid::nil().to_string()));
    }

    #[test]
    fn a_node_status_outside_the_enum_is_stored_data_invalid_and_names_its_subject() {
        let workflow_id = Uuid::nil();

        let refused = parse_task_status(workflow_id, Some("node-9"), "BOGUS")
            .expect_err("a status outside the enum is refused");

        assert_eq!(refused.code, HandleErrorCode::StoredDataInvalid);
        assert!(!refused.retryable);
        assert_eq!(refused.workflow_id, workflow_id);
        assert_eq!(
            refused.cause,
            crate::failure_cause::FailureCause::ContractViolation
        );
        assert_eq!(
            refused.message,
            "workflow 00000000-0000-0000-0000-000000000000 node node-9 stores status 'BOGUS', \
             which is not a WorkflowTaskStatus member; the CHECK \
             horsies_workflow_tasks_status_known must forbid it",
        );
    }

    #[test]
    fn a_node_status_outside_the_enum_on_a_row_with_no_node_id_names_the_node_none() {
        let refused = parse_task_status(Uuid::nil(), None, "BOGUS")
            .expect_err("a status outside the enum is refused");

        assert!(
            refused.message.contains(" node None stores status 'BOGUS'"),
            "{}",
            refused.message
        );
    }

    #[test]
    fn unknown_persisted_status_fails_closed() {
        assert!(matches!(
            parse_workflow_status("CORRUPT"),
            Err(WorkflowError::InvalidStatus(status)) if status == "CORRUPT"
        ));
    }

    // -----------------------------------------------------------------------
    // map_task_result_value
    // -----------------------------------------------------------------------

    #[test]
    fn map_ok_with_matching_type() {
        let raw = TaskResult::Ok(serde_json::json!(42));

        let result: TaskResult<i32> = map_task_result_value(raw, Uuid::nil(), None);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn map_ok_with_mismatched_type() {
        let raw = TaskResult::Ok(serde_json::json!("not a number"));

        let result: TaskResult<i32> = map_task_result_value(raw, Uuid::nil(), None);
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError,),
        );
        assert!(err
            .message
            .as_deref()
            .unwrap()
            .contains(&Uuid::nil().to_string()));
    }

    #[test]
    fn map_ok_with_mismatched_type_includes_node_id() {
        let raw = TaskResult::Ok(serde_json::json!("not a number"));

        let result: TaskResult<i32> = map_task_result_value(raw, Uuid::nil(), Some("step-a"));
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError,),
        );
        let msg = err.message.as_deref().unwrap();
        assert!(
            msg.contains(&Uuid::nil().to_string()),
            "expected workflow_id in message: {msg}"
        );
        assert!(msg.contains("step-a"), "expected node_id in message: {msg}");
    }

    #[test]
    fn map_err_passes_through() {
        let original = TaskError::new("MY_CODE", "original error").unwrap();
        let raw: TaskResult<serde_json::Value> = TaskResult::Err(original.clone());

        let result: TaskResult<i32> = map_task_result_value(raw, Uuid::nil(), None);
        let err = result.unwrap_err();
        assert_eq!(err.error_code, original.error_code);
        assert_eq!(err.message, original.message);
    }
}

#[cfg(test)]
mod wait_tests {
    //! C3 (workflow path): `get_workflow_result` must never block forever. A
    //! no-timeout wait on a workflow that does not exist returns `WorkflowNotFound`
    //! promptly. The lost-NOTIFY re-poll bound mirrors the task-path fix; its 5 s
    //! interval is not deterministically unit-testable without injecting it, so
    //! this test pins the deterministic anti-hang property (missing workflow).
    use super::*;
    use crate::broker::postgres::PostgresBroker;
    use crate::core::{OutcomeCode, TaskErrorCode};
    use crate::workflow_engine::bound_handle::WorkflowHandle;
    use serial_test::serial;
    use std::sync::Arc;

    #[tokio::test]
    #[serial]
    async fn get_workflow_result_no_timeout_returns_not_found_for_missing_workflow() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);
        broker.ensure_schema_initialized().await.expect("schema");
        let missing = Uuid::new_v4();

        // Wrap in an outer timeout: a regression to an unbounded wait hangs here.
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            get_workflow_result::<i32>(&broker, missing, None),
        )
        .await
        .expect("get_workflow_result(None) must not hang for a missing workflow");

        let outcome = res.expect("no workflow error");
        let err = outcome.unwrap_err();
        assert_eq!(
            err.error_code,
            TaskErrorCode::from(RetrievalCode::WorkflowNotFound),
        );
    }

    #[tokio::test]
    #[serial]
    async fn workflow_handle_get_returns_immediately_for_paused_workflow() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        ));
        broker.ensure_schema_initialized().await.expect("schema");
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_paused_get', 'PAUSED', 'fail', $2, 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.p7.paused-get.{workflow_id}"))
        .execute(broker.pool())
        .await
        .expect("seed paused workflow");

        let handle = WorkflowHandle::<i32>::new(
            workflow_id,
            Arc::clone(&broker),
            Arc::new(crate::core::registry::WorkflowSpecRegistry::new(
                crate::core::registry::BuilderHookName::SPEC_BUILDER,
            )),
            crate::core::config::payload::PayloadPolicy::default(),
            crate::core::RetentionConfig::default(),
        );
        let result = tokio::time::timeout(Duration::from_secs(5), handle.get(None))
            .await
            .expect("PAUSED must complete get() without a terminal notification");
        let error = result.unwrap_err();
        assert_eq!(
            error.error_code,
            TaskErrorCode::from(OutcomeCode::WorkflowPaused),
        );

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup paused workflow");
    }

    #[tokio::test]
    #[serial]
    async fn workflow_handle_get_preserves_structured_expiry_error() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        ));
        broker.ensure_schema_initialized().await.expect("schema");
        let workflow_id = Uuid::new_v4();
        let persisted = TaskError {
            error_code: OutcomeCode::WorkflowExpired.into(),
            message: Some("paused_workflow_auto_cancel_after elapsed: 3600 seconds".to_owned()),
            exception: None,
            data: Some(serde_json::json!({
                "policy": "paused_workflow_auto_cancel_after",
                "older_than_seconds": 3600.0,
            })),
            kind: crate::core::TaskErrorKind::Plain,
        };
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at,
                 completed_at, updated_at
             ) VALUES ($1, 'p7_expired_get', 'EXPIRED', 'fail', $2, $3, 0, $1,
                       NOW(), NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(serde_json::to_string(&persisted).unwrap())
        .bind(format!("test.p7.expired-get.{workflow_id}"))
        .execute(broker.pool())
        .await
        .expect("seed expired workflow");

        let handle = WorkflowHandle::<i32>::new(
            workflow_id,
            Arc::clone(&broker),
            Arc::new(crate::core::registry::WorkflowSpecRegistry::new(
                crate::core::registry::BuilderHookName::SPEC_BUILDER,
            )),
            crate::core::config::payload::PayloadPolicy::default(),
            crate::core::RetentionConfig::default(),
        );
        let error = handle.get(None).await.unwrap_err();
        assert_eq!(error.error_code, persisted.error_code);
        assert_eq!(error.message, persisted.message);
        assert_eq!(error.data, persisted.data);

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup expired workflow");
    }

    #[tokio::test]
    #[serial]
    async fn workflow_handle_get_fails_closed_on_malformed_expiry_error() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        ));
        broker.ensure_schema_initialized().await.expect("schema");
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at,
                 completed_at, updated_at
             ) VALUES ($1, 'p7_expired_corrupt_get', 'EXPIRED', 'fail',
                       '{corrupt', $2, 0, $1, NOW(), NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.p7.expired-corrupt-get.{workflow_id}"))
        .execute(broker.pool())
        .await
        .expect("seed malformed expired workflow");

        let handle = WorkflowHandle::<i32>::new(
            workflow_id,
            Arc::clone(&broker),
            Arc::new(crate::core::registry::WorkflowSpecRegistry::new(
                crate::core::registry::BuilderHookName::SPEC_BUILDER,
            )),
            crate::core::config::payload::PayloadPolicy::default(),
            crate::core::RetentionConfig::default(),
        );
        let error = handle.get(None).await.unwrap_err();
        assert_eq!(
            error.error_code,
            TaskErrorCode::from(OperationalErrorCode::ResultDeserializationError,),
        );
        assert!(error
            .message
            .as_deref()
            .unwrap()
            .contains("Workflow error payload corrupt"));
        assert_eq!(
            error.data,
            Some(serde_json::json!({"workflow_id": workflow_id.to_string()})),
        );

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup malformed expired workflow");
    }
}
