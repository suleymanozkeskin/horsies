use std::time::Duration;

use tokio::time::Instant;
use uuid::Uuid;

use crate::broker::core_rows::{column, first_row, RowDecodeError};
use crate::broker::process_listener::{fixed_channel, ListenError, WORKFLOW_DONE_CHANNEL};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::WorkflowStatus;
use crate::db::{
    CheckoutClass, FromSql, ParameterType, PayloadFilter, Row, TypedStatement, WakeEvent,
    WakeSubscription,
};

use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::steps::run_alone;

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
        TerminalWait::TimedOut => Ok(WorkflowWait::TimedOut),
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
    TimedOut,
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
                    TerminalFetch::NotTerminal => Ok(TerminalWait::TimedOut),
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
}

fn parse_workflow_status(s: &str) -> Result<WorkflowStatus, WorkflowError> {
    WorkflowStatus::try_from(s).map_err(WorkflowError::InvalidStatus)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_persisted_status_fails_closed() {
        assert!(matches!(
            parse_workflow_status("CORRUPT"),
            Err(WorkflowError::InvalidStatus(status)) if status == "CORRUPT"
        ));
    }
}

#[cfg(test)]
mod wait_tests {
    //! The wait for the end of a workflow (`wait_workflow_end`) never blocks
    //! forever: a no-timeout wait on a workflow that does not exist returns
    //! at once, and a PAUSED workflow ends the wait. The stored values come
    //! back as text; the clients decode them.
    use super::*;
    use crate::broker::postgres::PostgresBroker;
    use crate::core::{OutcomeCode, TaskError};
    use serial_test::serial;
    use std::sync::Arc;

    /// Many concurrent result waits on one broker share its one listener
    /// connection, not a connection each: twice as many waits as the session
    /// pool holds all end at their limit (`TimedOut`), and none fails on an
    /// exhausted session pool.
    #[tokio::test]
    #[serial]
    async fn many_concurrent_result_waits_share_one_listener() {
        let broker = Arc::new(PostgresBroker::from_pool_and_session_url(
            crate::broker::terminalization_matrix::migrated_pool().await,
            &crate::broker::terminalization_matrix::migrated_database_url().await,
            crate::ClientFamily::Rust,
        ));
        let pool = broker.pool().clone();
        let workflow_id = Uuid::new_v4();

        // A RUNNING workflow that never completes.
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index, definition_key, depth,
                root_workflow_id, sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'p2_wf', 'RUNNING', 'fail', NULL, 'test.p2.v1', 0, $1,
                      NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("insert workflow");

        // 8 concurrent waits: twice the connection limit of the session pool.
        let waits = (0..8).map(|_| {
            let broker = Arc::clone(&broker);
            async move {
                wait_workflow_end(&broker, workflow_id, Some(Duration::from_millis(400))).await
            }
        });
        let results = futures::future::join_all(waits).await;

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .expect("delete the workflow");
        for result in results {
            assert_eq!(
                result.expect("each wait reads through the shared listener"),
                WorkflowWait::TimedOut,
                "each wait ends at its limit"
            );
        }
    }

    #[tokio::test]
    #[serial]
    async fn a_wait_with_no_timeout_returns_not_found_for_a_missing_workflow() {
        let broker = PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
            crate::ClientFamily::Rust,
        );
        broker.ensure_schema_initialized().await.expect("schema");
        let missing = Uuid::new_v4();

        // Wrap in an outer timeout: a regression to an unbounded wait hangs here.
        let wait = tokio::time::timeout(
            Duration::from_secs(5),
            wait_workflow_end(&broker, missing, None),
        )
        .await
        .expect("wait_workflow_end(None) must not hang for a missing workflow");

        assert_eq!(wait.expect("no workflow error"), WorkflowWait::NotFound);
    }

    #[tokio::test]
    #[serial]
    async fn the_wait_returns_immediately_for_a_paused_workflow() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
            crate::ClientFamily::Rust,
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

        let wait = tokio::time::timeout(
            Duration::from_secs(5),
            wait_workflow_end(&broker, workflow_id, None),
        )
        .await
        .expect("PAUSED must end the wait without a terminal notification");
        assert_eq!(
            wait.expect("the wait"),
            WorkflowWait::Ended(WorkflowEnd::Paused)
        );

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup paused workflow");
    }

    #[tokio::test]
    #[serial]
    async fn the_wait_returns_a_structured_expiry_error_as_stored() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
            crate::ClientFamily::Rust,
        ));
        broker.ensure_schema_initialized().await.expect("schema");
        let workflow_id = Uuid::new_v4();
        let persisted = TaskError {
            error_code: OutcomeCode::WorkflowExpired.into(),
            message: Some("paused_workflow_auto_cancel_after elapsed: 3600 seconds".to_owned()),
            exception: None,
            data: Some(crate::core::codec::JsonText::of_engine_value(
                &serde_json::json!({
                    "policy": "paused_workflow_auto_cancel_after",
                    "older_than_seconds": 3600.0,
                }),
            )),
            kind: crate::core::TaskErrorKind::Plain,
        };
        let stored = serde_json::to_string(&persisted).unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at,
                 completed_at, updated_at
             ) VALUES ($1, 'p7_expired_get', 'EXPIRED', 'fail', $2, $3, 0, $1,
                       NOW(), NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(&stored)
        .bind(format!("test.p7.expired-get.{workflow_id}"))
        .execute(broker.pool())
        .await
        .expect("seed expired workflow");

        let wait = wait_workflow_end(&broker, workflow_id, None)
            .await
            .expect("the wait");
        let text = match wait {
            WorkflowWait::Ended(WorkflowEnd::Ended {
                status: TerminalErrorStatus::Expired,
                error: Some(text),
            }) => text,
            other => panic!("expected an EXPIRED end with its error, got {other:?}"),
        };
        let error: TaskError = serde_json::from_str(&text).expect("the stored error decodes");
        assert_eq!(error.error_code, persisted.error_code);
        assert_eq!(error.message, persisted.message);
        assert_eq!(error.data, persisted.data);

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup expired workflow");
    }

    /// The wait returns a stored error that does not decode as stored; a
    /// client refuses it (`RESULT_DESERIALIZATION_ERROR`).
    #[tokio::test]
    #[serial]
    async fn the_wait_returns_a_malformed_expiry_error_as_stored() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
            crate::ClientFamily::Rust,
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

        let wait = wait_workflow_end(&broker, workflow_id, None)
            .await
            .expect("the wait");
        assert_eq!(
            wait,
            WorkflowWait::Ended(WorkflowEnd::Ended {
                status: TerminalErrorStatus::Expired,
                error: Some("{corrupt".to_owned()),
            })
        );

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(broker.pool())
            .await
            .expect("cleanup malformed expired workflow");
    }
}
