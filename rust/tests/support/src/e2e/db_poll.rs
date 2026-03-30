/// Database polling helpers for e2e tests.
///
/// Mirrors Python's `tests/e2e/helpers/db.py`.
use std::time::{Duration, Instant};

use sqlx::PgPool;

/// Wait until all given tasks reach a terminal status.
pub async fn wait_for_all_terminal(pool: &PgPool, task_ids: &[String], timeout: Duration) {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks \
             WHERE id = ANY($1) AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')",
        )
        .bind(task_ids)
        .fetch_one(pool)
        .await
        .unwrap_or(0);

        if pending == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!(
        "tasks did not reach terminal state within {}s",
        timeout.as_secs(),
    );
}

/// Wait until a single task reaches the given status.
pub async fn wait_for_task_status(
    pool: &PgPool,
    task_id: &str,
    target_status: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
                .bind(task_id)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();

        if status.as_deref() == Some(target_status) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!(
        "task {} did not reach status {} within {}s",
        task_id,
        target_status,
        timeout.as_secs(),
    );
}

/// Wait until a workflow reaches a terminal status. Returns the final status.
pub async fn wait_for_workflow_terminal(
    pool: &PgPool,
    workflow_id: &str,
    timeout: Duration,
) -> String {
    let deadline = Instant::now() + timeout;
    let mut last_status = "UNKNOWN".to_owned();

    while Instant::now() < deadline {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();

        if let Some(s) = status {
            last_status = s.clone();
            if matches!(s.as_str(), "COMPLETED" | "FAILED" | "CANCELLED") {
                return s;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!(
        "workflow {} did not reach terminal state within {}s (last: {})",
        workflow_id,
        timeout.as_secs(),
        last_status,
    );
}

/// Wait until a workflow reaches a specific status.
pub async fn wait_for_workflow_status(
    pool: &PgPool,
    workflow_id: &str,
    target_status: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();

        if status.as_deref() == Some(target_status) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!(
        "workflow {} did not reach status {} within {}s",
        workflow_id,
        target_status,
        timeout.as_secs(),
    );
}

/// Poll DB for a duration and return the max count observed for a query.
pub async fn poll_max_count(
    pool: &PgPool,
    sql: &str,
    duration: Duration,
    poll_interval: Duration,
) -> i64 {
    let deadline = Instant::now() + duration;
    let mut max_count: i64 = 0;

    while Instant::now() < deadline {
        let count: i64 = sqlx::query_scalar(sql).fetch_one(pool).await.unwrap_or(0);
        max_count = max_count.max(count);
        tokio::time::sleep(poll_interval).await;
    }

    max_count
}
