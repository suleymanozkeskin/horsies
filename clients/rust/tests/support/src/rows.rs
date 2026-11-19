//! Reads of the task and workflow tables, for the waits and the checks that
//! the client API does not offer.
//!
//! A task that reaches a terminal status leaves `horsies_tasks` and gets its
//! row in `horsies_task_history` in one transaction, so every task read
//! here looks at both tables in one statement (Python `itest_task_rows`).
//! Each read panics with its subject on a database error.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};
use uuid::Uuid;

use crate::poll::poll_until;

/// The terminal statuses of a workflow. `PAUSED` is not one: a resume
/// continues it.
pub const WORKFLOW_END_STATUSES: [&str; 4] = ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"];

/// One task as the tests read it, from whichever table holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRow {
    pub task_id: Uuid,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub status: String,
    pub retry_count: i32,
    pub max_retries: i32,
    /// The stored error code; only a history row has one.
    pub error_code: Option<String>,
    /// The worker that holds the claim now (live) or that held it last
    /// (history).
    pub worker_id: Option<String>,
    pub sent_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    /// The time of the terminal status; only a history row has one.
    pub terminal_at: Option<DateTime<Utc>>,
    pub is_workflow_task: bool,
}

const TASK_COLUMNS: &str = "task_id, task_name, queue_name, priority, status, retry_count, \
                            max_retries, error_code, worker_id, sent_at, claimed_at, started_at, \
                            terminal_at, is_workflow_task";

/// Both tables as one relation with the columns of [`TaskRow`].
const TASK_ROWS: &str = "(SELECT id AS task_id, task_name::text, queue_name::text, priority, \
                         status::text, retry_count, max_retries, NULL::text AS error_code, \
                         claimed_by_worker_id::text AS worker_id, sent_at, claimed_at, \
                         started_at, NULL::timestamptz AS terminal_at, is_workflow_task \
                         FROM horsies_tasks \
                         UNION ALL \
                         SELECT task_id, task_name::text, queue_name::text, priority, \
                         status::text, retry_count, max_retries, error_code, \
                         last_claimed_worker_id::text AS worker_id, sent_at, claimed_at, \
                         started_at, terminal_at, is_workflow_task \
                         FROM horsies_task_history) AS task_rows";

fn task_row(row: &Row) -> TaskRow {
    TaskRow {
        task_id: row.get(0),
        task_name: row.get(1),
        queue_name: row.get(2),
        priority: row.get(3),
        status: row.get(4),
        retry_count: row.get(5),
        max_retries: row.get(6),
        error_code: row.get(7),
        worker_id: row.get(8),
        sent_at: row.get(9),
        claimed_at: row.get(10),
        started_at: row.get(11),
        terminal_at: row.get(12),
        is_workflow_task: row.get(13),
    }
}

/// The row of `task_id`, or `None` when neither table holds it.
pub async fn read_task(client: &Client, task_id: Uuid) -> Option<TaskRow> {
    let statement = format!("SELECT {TASK_COLUMNS} FROM {TASK_ROWS} WHERE task_id = $1");
    client
        .query_opt(&statement, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read task {task_id}: {error}"))
        .map(|row| task_row(&row))
}

/// The rows of every task of `task_name`, oldest send first.
pub async fn read_tasks_named(client: &Client, task_name: &str) -> Vec<TaskRow> {
    let statement = format!(
        "SELECT {TASK_COLUMNS} FROM {TASK_ROWS} WHERE task_name = $1 ORDER BY sent_at, task_id"
    );
    client
        .query(&statement, &[&task_name])
        .await
        .unwrap_or_else(|error| panic!("read the tasks named {task_name}: {error}"))
        .iter()
        .map(task_row)
        .collect()
}

/// The status of `task_id`, or `None` when neither table holds it.
pub async fn task_status(client: &Client, task_id: Uuid) -> Option<String> {
    read_task(client, task_id).await.map(|row| row.status)
}

/// Wait until `task_id` has `status`. Panics when `limit` passes first.
pub async fn wait_for_task_status(client: &Client, task_id: Uuid, status: &str, limit: Duration) {
    poll_until(
        limit,
        &format!("task {task_id} reaches {status}"),
        async || (task_status(client, task_id).await.as_deref() == Some(status)).then_some(()),
    )
    .await;
}

/// Wait until every task of `task_ids` has its history row. Panics when
/// `limit` passes first.
pub async fn wait_for_terminal(client: &Client, task_ids: &[Uuid], limit: Duration) {
    let ids = task_ids.to_vec();
    let expected = i64::try_from(ids.len()).expect("a test sends fewer than i64::MAX tasks");
    poll_until(
        limit,
        &format!("{expected} tasks reach a terminal status"),
        async || {
            let ended = count(
                client,
                "SELECT count(*) FROM horsies_task_history WHERE task_id = ANY($1)",
                &[&ids],
            )
            .await;
            (ended == expected).then_some(())
        },
    )
    .await;
}

/// The single `bigint` of `statement`, a count.
pub async fn count(client: &Client, statement: &str, params: &[&(dyn ToSql + Sync)]) -> i64 {
    client
        .query_one(statement, params)
        .await
        .unwrap_or_else(|error| panic!("count with {statement:?}: {error}"))
        .get(0)
}

/// The number of tasks of `task_name` with `status`, in either table.
pub async fn count_tasks(client: &Client, task_name: &str, status: &str) -> i64 {
    count(
        client,
        &format!("SELECT count(*) FROM {TASK_ROWS} WHERE task_name = $1 AND status = $2"),
        &[&task_name, &status],
    )
    .await
}

/// The status of workflow `workflow_id`, or `None` when it has no row.
pub async fn workflow_status(client: &Client, workflow_id: Uuid) -> Option<String> {
    client
        .query_opt(
            "SELECT status::text FROM horsies_workflows WHERE id = $1",
            &[&workflow_id],
        )
        .await
        .unwrap_or_else(|error| panic!("read workflow {workflow_id}: {error}"))
        .map(|row| row.get(0))
}

/// Wait until workflow `workflow_id` has `status`. Panics when `limit`
/// passes first.
pub async fn wait_for_workflow_status(
    client: &Client,
    workflow_id: Uuid,
    status: &str,
    limit: Duration,
) {
    poll_until(
        limit,
        &format!("workflow {workflow_id} reaches {status}"),
        async || {
            (workflow_status(client, workflow_id).await.as_deref() == Some(status)).then_some(())
        },
    )
    .await;
}

/// Wait until workflow `workflow_id` has one of [`WORKFLOW_END_STATUSES`] and
/// return it. Panics when `limit` passes first.
pub async fn wait_for_workflow_end(client: &Client, workflow_id: Uuid, limit: Duration) -> String {
    poll_until(limit, &format!("workflow {workflow_id} ends"), async || {
        workflow_status(client, workflow_id)
            .await
            .filter(|status| WORKFLOW_END_STATUSES.contains(&status.as_str()))
    })
    .await
}

/// One node of a workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRow {
    pub task_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    pub status: String,
    pub task_id: Option<Uuid>,
    pub sub_workflow_id: Option<Uuid>,
    /// The stored result envelope text.
    pub result: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// The nodes of workflow `workflow_id`, by task index.
pub async fn workflow_nodes(client: &Client, workflow_id: Uuid) -> Vec<NodeRow> {
    client
        .query(
            "SELECT task_index, node_id::text, task_name::text, status::text, task_id, \
             sub_workflow_id, result, started_at, completed_at \
             FROM horsies_workflow_tasks WHERE workflow_id = $1 ORDER BY task_index",
            &[&workflow_id],
        )
        .await
        .unwrap_or_else(|error| panic!("read the nodes of workflow {workflow_id}: {error}"))
        .iter()
        .map(|row| NodeRow {
            task_index: row.get(0),
            node_id: row.get(1),
            task_name: row.get(2),
            status: row.get(3),
            task_id: row.get(4),
            sub_workflow_id: row.get(5),
            result: row.get(6),
            started_at: row.get(7),
            completed_at: row.get(8),
        })
        .collect()
}

/// The node of `workflow_id` at `task_index`. Panics when it has none.
pub async fn workflow_node(client: &Client, workflow_id: Uuid, task_index: i32) -> NodeRow {
    workflow_nodes(client, workflow_id)
        .await
        .into_iter()
        .find(|node| node.task_index == task_index)
        .unwrap_or_else(|| panic!("workflow {workflow_id} has no node {task_index}"))
}

/// Wait until node `task_index` of `workflow_id` has `status`. Panics when
/// `limit` passes first.
pub async fn wait_for_node_status(
    client: &Client,
    workflow_id: Uuid,
    task_index: i32,
    status: &str,
    limit: Duration,
) {
    poll_until(
        limit,
        &format!("node {task_index} of workflow {workflow_id} reaches {status}"),
        async || {
            workflow_nodes(client, workflow_id)
                .await
                .iter()
                .any(|node| node.task_index == task_index && node.status == status)
                .then_some(())
        },
    )
    .await;
}

/// The stored `error` text of workflow `workflow_id`.
pub async fn workflow_error(client: &Client, workflow_id: Uuid) -> Option<String> {
    client
        .query_one(
            "SELECT error FROM horsies_workflows WHERE id = $1",
            &[&workflow_id],
        )
        .await
        .unwrap_or_else(|error| panic!("read the error of workflow {workflow_id}: {error}"))
        .get(0)
}

/// The child workflows of `workflow_id`, by parent node index.
pub async fn child_workflows(client: &Client, workflow_id: Uuid) -> Vec<(i32, Uuid, String)> {
    client
        .query(
            "SELECT parent_task_index, id, status::text FROM horsies_workflows \
             WHERE parent_workflow_id = $1 ORDER BY parent_task_index",
            &[&workflow_id],
        )
        .await
        .unwrap_or_else(|error| panic!("read the children of workflow {workflow_id}: {error}"))
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}
