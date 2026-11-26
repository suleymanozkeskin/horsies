use chrono::Utc;
use uuid::Uuid;

use crate::broker::terminalization::on_test_pool::terminalize;
use crate::broker::PostgresBroker;
use crate::core::lifecycle::{TerminalizationCommand, TerminalizationOutcome, WorkerOwned};
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::migrated_pool;
use crate::test_support::sql::{self, TestPool, TestPoolOptions};
use crate::test_support::terminal_writes::{self, TerminalFailure};
use crate::{TaskStatus, WorkflowStatus};

use super::*;

#[derive(Clone)]
struct TaskSeed {
    id: Uuid,
    status: TaskStatus,
    worker: Option<String>,
    is_workflow_task: bool,
}

impl TaskSeed {
    fn pending() -> Self {
        Self {
            id: Uuid::new_v4(),
            status: TaskStatus::Pending,
            worker: None,
            is_workflow_task: false,
        }
    }

    fn claimed() -> Self {
        Self {
            status: TaskStatus::Claimed,
            worker: Some("w3-worker".to_owned()),
            ..Self::pending()
        }
    }

    fn running() -> Self {
        Self {
            status: TaskStatus::Running,
            worker: Some("w3-worker".to_owned()),
            ..Self::pending()
        }
    }
}

async fn clean_w3(pool: &TestPool) {
    for statement in [
        "DELETE FROM horsies_workflows WHERE name LIKE 'w3_%'",
        "DELETE FROM horsies_task_history WHERE task_name LIKE 'w3_%'",
        "DELETE FROM horsies_tasks WHERE task_name LIKE 'w3_%'",
    ] {
        sql::query(statement)
            .execute(pool)
            .await
            .expect("clean W3 rows");
    }
}

/// Insert one live row. Only a CLAIMED row holds a claim lease
/// (`horsies_tasks_status_live`; Python
/// `clients/python/tests/integration/monitoring_seeding.py:268-274`). A RUNNING row carries
/// the process facts that its start writes.
async fn seed_task(pool: &TestPool, seed: &TaskSeed) {
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
            started_at, claimed_at, claimed_by_worker_id, claim_expires_at, is_workflow_task,
            retry_count, max_retries, command_fingerprint_version, command_fingerprint,
            retention_class_key, retain_rerun_input, created_at, updated_at, execution,
            worker_pid, worker_hostname, worker_process_name
        ) VALUES (
            $1, $2, 'default', 100, 'null', $3, NOW(), NOW() - INTERVAL '60 seconds',
            CASE WHEN $3 = 'RUNNING' THEN NOW() - INTERVAL '30 seconds' END,
            CASE WHEN $4::text IS NOT NULL THEN NOW() - INTERVAL '40 seconds' END, $4,
            CASE WHEN $3 = 'CLAIMED' THEN NOW() + INTERVAL '1 minute' END, $5, 0, 3,
            CASE WHEN $5 THEN NULL ELSE 1 END,
            CASE WHEN $5 THEN NULL ELSE decode(repeat('0c', 32), 'hex') END, 'forever', FALSE,
            NOW(), NOW(), 'ISOLATED',
            CASE WHEN $3 = 'RUNNING' THEN 4242 END,
            CASE WHEN $3 = 'RUNNING' THEN 'w3-host' END,
            CASE WHEN $3 = 'RUNNING' THEN 'w3-process' END
        )",
    )
    .bind(seed.id)
    .bind(format!("w3_task_{}", seed.id.simple()))
    .bind(seed.status.to_string())
    .bind(&seed.worker)
    .bind(seed.is_workflow_task)
    .execute(pool)
    .await
    .expect("seed W3 task");
}

/// The success of a RUNNING plain task of `w3-worker`.
async fn complete_task(pool: &TestPool, task_id: Uuid) {
    let outcomes =
        terminal_writes::complete_plain_task(pool, task_id, "w3-worker", "{\"Ok\":true}")
            .await
            .expect("complete W3 task");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

/// The terminal failure of a RUNNING task of `w3-worker`, plain or workflow.
async fn fail_task(pool: &TestPool, task_id: Uuid) {
    let outcomes = terminal_writes::fail_running_task(
        pool,
        task_id,
        "w3-worker",
        &TerminalFailure::of_task("{\"Err\":{}}", Some("TASK_EXCEPTION"), Some("W3 failure")),
    )
    .await
    .expect("fail W3 task");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

async fn expire_task(pool: &TestPool, task_id: Uuid) {
    sql::query("UPDATE horsies_tasks SET good_until = NOW() - INTERVAL '1 minute' WHERE id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("make W3 task overdue");
    let outcomes = terminalize(
        pool,
        &TerminalizationCommand::ExpireOwnedClaim {
            task_id,
            fence: WorkerOwned {
                worker_id: "w3-worker".to_owned(),
            },
            result_json: "{\"Err\":{}}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("expire W3 task");
    assert!(matches!(
        outcomes.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

fn body_json(outcome: &ActionOutcome) -> String {
    serde_json::to_string(outcome.body()).expect("serialize action body")
}

async fn seed_workflow(pool: &TestPool, status: WorkflowStatus) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth,
             root_workflow_id, sent_at, created_at, started_at,
             completed_at, updated_at
         ) VALUES (
             $1, $2, $3, 'fail', 'w3.definition.v1', 0, $1,
             NOW(), NOW() - INTERVAL '2 minutes',
             NOW() - INTERVAL '1 minute',
             CASE WHEN $3 IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                  THEN NOW() END,
             NOW()
         )",
    )
    .bind(workflow_id)
    .bind(format!("w3_workflow_{}", workflow_id.simple()))
    .bind(status.to_string())
    .execute(pool)
    .await
    .expect("seed W3 workflow");
    workflow_id
}

async fn workflow_status(pool: &TestPool, workflow_id: Uuid) -> String {
    sql::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .expect("read W3 workflow status")
}

async fn seed_workflow_task_link(pool: &TestPool, workflow_id: Uuid, task_id: Uuid, status: &str) {
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
            downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
            dependencies, allow_failed_deps, join_type, status, task_id, is_subworkflow,
            created_at, started_at, max_retries, execution, retention_class_key
        ) VALUES (
            'NONE', $1, $2, 0, 'w3-node', 'w3_node_task', 'null', 'default', 100, ARRAY[]::integer[],
            FALSE, 'all', $3, $4, FALSE, NOW(),
            CASE WHEN $3 = 'RUNNING' THEN NOW() - INTERVAL '30 seconds' END, 0, 'ISOLATED',
            'standard_30d'
        )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(status)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("seed W3 workflow task link");
}

#[test]
fn action_models_and_task_mapping_serialize_exactly() {
    assert_eq!(
        serde_json::to_string(&CancelTaskBody::default()).unwrap(),
        r#"{"include_running":false}"#
    );
    let task_id = Uuid::nil();
    let success = task_action_outcome(Ok(TaskCancelled {
        task_id,
        was_status: TaskStatus::Pending,
    }));
    assert_eq!(success.status_code(), 200);
    assert_eq!(
        body_json(&success),
        r#"{"outcome":"cancelled","was_status":"PENDING","warning":null}"#
    );

    let cases = [
        (
            TaskActionError::NotFound {
                message: "Task missing".to_owned(),
                task_id,
            },
            404,
            r#"{"detail":"Task missing"}"#,
        ),
        (
            TaskActionError::WorkflowTask {
                message: "workflow".to_owned(),
                task_id,
                current_status: TaskStatus::Running,
            },
            400,
            r#"{"code":"TASK_IS_WORKFLOW_TASK"}"#,
        ),
        (
            TaskActionError::NotCancellable {
                message: "conflict".to_owned(),
                task_id,
                current_status: TaskStatus::Completed,
            },
            409,
            r#"{"code":"TASK_NOT_CANCELLABLE","current_status":"COMPLETED"}"#,
        ),
        (
            TaskActionError::DatabaseOperationFailed {
                message: "database".to_owned(),
                retryable: true,
                task_id,
                cause: Box::new(crate::broker::BrokerError::ConnectionFailed(
                    "refused".to_owned(),
                )),
            },
            503,
            r#"{"detail":"database"}"#,
        ),
        (
            TaskActionError::DatabaseOperationFailed {
                message: "refused".to_owned(),
                retryable: false,
                task_id,
                cause: Box::new(crate::broker::BrokerError::ClaimContract("row".to_owned())),
            },
            500,
            r#"{"detail":"refused"}"#,
        ),
        (
            TaskActionError::StoredDataInvalid {
                message: "stored".to_owned(),
                task_id,
                cause: crate::core::lifecycle::OutcomeDecodeError("NOT_KNOWN".to_owned()),
            },
            500,
            r#"{"detail":"stored"}"#,
        ),
    ];
    for (error, status_code, expected_json) in cases {
        let outcome = task_action_outcome(Err(error));
        assert_eq!(outcome.status_code(), status_code);
        assert_eq!(body_json(&outcome), expected_json);
    }
}

#[tokio::test]
async fn task_cancel_applies_only_to_the_three_eligible_live_states() {
    let pool = migrated_pool().await;
    clean_w3(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

    for (seed, include_running) in [
        (TaskSeed::pending(), false),
        (TaskSeed::claimed(), false),
        (TaskSeed::running(), true),
    ] {
        seed_task(&pool, &seed).await;
        let outcome = cancel_task_action(&broker, seed.id, include_running).await;
        assert_eq!(outcome.status_code(), 200);
        assert!(body_json(&outcome).contains(&format!(r#""was_status":"{}""#, seed.status)));
        let history = sql::query(
            "SELECT status, error_code, final_failed_reason,
                    terminalization_kind, terminal_at
             FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(seed.id)
        .fetch_one(&pool)
        .await
        .expect("read cancelled W3 history");
        assert_eq!(history.get::<_, String>("status"), "CANCELLED");
        assert_eq!(
            history.get::<_, Option<String>>("error_code").as_deref(),
            Some("TASK_CANCELLED")
        );
        assert_eq!(
            history
                .get::<_, Option<String>>("final_failed_reason")
                .as_deref(),
            Some("Cancelled via monitoring API")
        );
        assert_eq!(
            history.get::<_, String>("terminalization_kind"),
            "CANCEL_ADMIN"
        );
        assert!(history
            .get::<_, Option<chrono::DateTime<Utc>>>("terminal_at")
            .is_some());
        let live_count: i64 = sql::query_scalar("SELECT count(*) FROM horsies_tasks WHERE id = $1")
            .bind(seed.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let attempts: i64 =
            sql::query_scalar("SELECT count(*) FROM horsies_task_attempts WHERE task_id = $1")
                .bind(seed.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((live_count, attempts), (0, 0));
    }

    let running = TaskSeed::running();
    seed_task(&pool, &running).await;
    let refused = cancel_task_action(&broker, running.id, false).await;
    assert_eq!(refused.status_code(), 409);
    assert_eq!(
        body_json(&refused),
        r#"{"code":"TASK_NOT_CANCELLABLE","current_status":"RUNNING"}"#
    );
    assert_eq!(
        sql::query_scalar::<String>("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(running.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "RUNNING"
    );
    assert_eq!(
        cancel_task_action(&broker, running.id, true)
            .await
            .status_code(),
        200
    );

    clean_w3(&pool).await;
}

#[tokio::test]
async fn task_cancel_live_miss_and_workflow_diagnoses_are_exact() {
    let pool = migrated_pool().await;
    clean_w3(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

    let missing = Uuid::new_v4();
    let absent = cancel_task_action(&broker, missing, false).await;
    assert_eq!(absent.status_code(), 404);
    assert_eq!(
        body_json(&absent),
        format!(r#"{{"detail":"Task {missing} does not exist."}}"#)
    );

    let mut terminal = Vec::new();
    let completed = TaskSeed::running();
    seed_task(&pool, &completed).await;
    complete_task(&pool, completed.id).await;
    terminal.push((completed.id, TaskStatus::Completed));

    let failed = TaskSeed::running();
    seed_task(&pool, &failed).await;
    fail_task(&pool, failed.id).await;
    terminal.push((failed.id, TaskStatus::Failed));

    let cancelled = TaskSeed::pending();
    seed_task(&pool, &cancelled).await;
    assert!(cancel_task(&broker, cancelled.id, false).await.is_ok());
    terminal.push((cancelled.id, TaskStatus::Cancelled));

    let expired = TaskSeed::claimed();
    seed_task(&pool, &expired).await;
    expire_task(&pool, expired.id).await;
    terminal.push((expired.id, TaskStatus::Expired));

    for (task_id, status) in terminal {
        let outcome = cancel_task_action(&broker, task_id, true).await;
        assert_eq!(outcome.status_code(), 409);
        assert_eq!(
            body_json(&outcome),
            format!(r#"{{"code":"TASK_NOT_CANCELLABLE","current_status":"{status}"}}"#)
        );
    }

    let mut workflow_live = TaskSeed::pending();
    workflow_live.is_workflow_task = true;
    seed_task(&pool, &workflow_live).await;
    let live_workflow = seed_workflow(&pool, WorkflowStatus::Running).await;
    seed_workflow_task_link(&pool, live_workflow, workflow_live.id, "ENQUEUED").await;
    let live_refusal = cancel_task_action(&broker, workflow_live.id, false).await;
    assert_eq!(live_refusal.status_code(), 400);
    assert_eq!(
        body_json(&live_refusal),
        r#"{"code":"TASK_IS_WORKFLOW_TASK"}"#
    );
    assert_eq!(
        sql::query_scalar::<String>("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(workflow_live.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "PENDING"
    );

    let mut workflow_history = TaskSeed::running();
    workflow_history.is_workflow_task = true;
    seed_task(&pool, &workflow_history).await;
    let history_workflow = seed_workflow(&pool, WorkflowStatus::Running).await;
    seed_workflow_task_link(&pool, history_workflow, workflow_history.id, "RUNNING").await;
    fail_task(&pool, workflow_history.id).await;
    let history_refusal = cancel_task_action(&broker, workflow_history.id, true).await;
    assert_eq!(history_refusal.status_code(), 400);
    assert_eq!(
        body_json(&history_refusal),
        r#"{"code":"TASK_IS_WORKFLOW_TASK"}"#
    );

    clean_w3(&pool).await;
}

#[tokio::test]
async fn task_and_workflow_database_failures_are_service_unavailable() {
    let pool = TestPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(250))
        .connect_lazy("postgresql://postgres:none@127.0.0.1:1/none");
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task = cancel_task(&broker, Uuid::new_v4(), false)
        .await
        .expect_err("task database failure");
    assert_eq!(task.code(), TaskActionErrorCode::DatabaseOperationFailed);
    assert!(task.retryable());
    let workflow = pause_workflow_action(&broker, Uuid::new_v4()).await;
    assert_eq!(workflow.status_code(), 503);
    assert!(matches!(workflow.body(), ActionBody::Detail { .. }));
}

#[tokio::test]
async fn workflow_actions_resolve_success_noop_and_missing_outcomes() {
    let pool = migrated_pool().await;
    clean_w3(&pool).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let children = TestChildSpecs::empty();

    let running_to_pause = seed_workflow(&pool, WorkflowStatus::Running).await;
    let paused = pause_workflow_action(&broker, running_to_pause).await;
    assert_eq!(paused.status_code(), 200);
    assert_eq!(
        body_json(&paused),
        r#"{"outcome":"paused","was_status":null,"warning":null}"#
    );
    assert_eq!(workflow_status(&pool, running_to_pause).await, "PAUSED");

    let paused_to_resume = seed_workflow(&pool, WorkflowStatus::Paused).await;
    let resumed = resume_workflow_action(&broker, paused_to_resume, &children).await;
    assert_eq!(resumed.status_code(), 200);
    assert_eq!(
        body_json(&resumed),
        r#"{"outcome":"resumed","was_status":null,"warning":null}"#
    );

    let running_to_cancel = seed_workflow(&pool, WorkflowStatus::Running).await;
    let cancelled = cancel_workflow_action(&broker, running_to_cancel).await;
    assert_eq!(cancelled.status_code(), 200);
    assert_eq!(
        body_json(&cancelled),
        r#"{"outcome":"cancelled","was_status":null,"warning":null}"#
    );
    assert_eq!(workflow_status(&pool, running_to_cancel).await, "CANCELLED");

    for action in ["pause", "resume", "cancel"] {
        let completed = seed_workflow(&pool, WorkflowStatus::Completed).await;
        let outcome = match action {
            "pause" => pause_workflow_action(&broker, completed).await,
            "resume" => resume_workflow_action(&broker, completed, &children).await,
            "cancel" => cancel_workflow_action(&broker, completed).await,
            _ => unreachable!(),
        };
        assert_eq!(outcome.status_code(), 409);
        assert_eq!(
            body_json(&outcome),
            r#"{"code":"STATE_CONFLICT","current_status":"COMPLETED"}"#
        );
    }

    for action in ["pause", "resume", "cancel"] {
        let missing = Uuid::new_v4();
        let outcome = match action {
            "pause" => pause_workflow_action(&broker, missing).await,
            "resume" => resume_workflow_action(&broker, missing, &children).await,
            "cancel" => cancel_workflow_action(&broker, missing).await,
            _ => unreachable!(),
        };
        assert_eq!(outcome.status_code(), 404);
        assert_eq!(
            body_json(&outcome),
            format!(r#"{{"detail":"Workflow {missing} not found"}}"#)
        );
    }

    clean_w3(&pool).await;
}

/// A PENDING plain task with the columns of the baseline schema.
async fn seed_baseline_pending_task(pool: &TestPool) -> Uuid {
    let task_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at, command_fingerprint_version,
             command_fingerprint, status, enqueued_at, retry_count, max_retries,
             is_workflow_task, updated_at, retain_rerun_input
         ) VALUES (
             $1, $2, 'default', 100, '{}', NULL, 'ISOLATED',
             'forever', NOW(), NOW(), 1,
             decode(repeat('0c', 32), 'hex'), 'PENDING', NOW(), 0, 0,
             FALSE, NOW(), FALSE
         )",
    )
    .bind(task_id)
    .bind(format!("w3_task_{}", task_id.simple()))
    .execute(pool)
    .await
    .expect("seed a baseline PENDING task");
    task_id
}

/// Bounds the wait for the cancel backend to block on the advisory lock.
const LOCK_WAIT_POLLS: usize = 250;
const LOCK_WAIT_POLL: std::time::Duration = std::time::Duration::from_millis(20);

/// A monitoring cancel against a finalize: the cancel
/// (`horsies_cancel_task`) takes the task advisory lock before the row lock,
/// the order of every move function. While
/// another transaction holds the advisory lock, the cancel waits for it and
/// holds no row lock, so the holder can still lock the row. With the row
/// lock first, the holder's `FOR UPDATE NOWAIT` fails with 55P03 and a
/// finalize deadlocks (40P01).
#[tokio::test]
async fn task_cancel_takes_the_advisory_lock_before_the_row_lock() {
    let pool = migrated_pool().await;
    clean_w3(&pool).await;
    let broker = std::sync::Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));
    let task_id = seed_baseline_pending_task(&pool).await;

    let mut holder = pool.begin().await.expect("holder transaction");
    sql::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 731))")
        .bind(task_id)
        .execute(&mut *holder)
        .await
        .expect("hold the task advisory lock");

    let cancel = tokio::spawn({
        let broker = std::sync::Arc::clone(&broker);
        async move { cancel_task(&broker, task_id, false).await }
    });

    let mut cancel_waits = false;
    for _ in 0..LOCK_WAIT_POLLS {
        let waiting: i64 = sql::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database()
               AND wait_event_type = 'Lock' AND wait_event = 'advisory'
               AND query LIKE '%horsies_cancel_task(%'",
        )
        .fetch_one(&pool)
        .await
        .expect("read lock waits");
        if waiting == 1 {
            cancel_waits = true;
            break;
        }
        tokio::time::sleep(LOCK_WAIT_POLL).await;
    }
    assert!(
        cancel_waits,
        "the cancel must wait on the task advisory lock"
    );

    sql::query("SELECT 1 FROM horsies_tasks WHERE id = $1 FOR UPDATE NOWAIT")
        .bind(task_id)
        .execute(&mut *holder)
        .await
        .expect("the waiting cancel holds no row lock");
    holder.rollback().await.expect("release the holder's locks");

    let cancelled = tokio::time::timeout(std::time::Duration::from_secs(10), cancel)
        .await
        .expect("the cancel ends once the advisory lock is free")
        .expect("the cancel task does not panic")
        .expect("the cancel applies");
    assert_eq!(cancelled.was_status, TaskStatus::Pending);
    clean_w3(&pool).await;
}

/// A task cancel whose statement the server refused answers by the SQLSTATE
/// class: 40 (serialization), 55 (lock not available) and 57 (statement
/// timeout) may clear, 503; 23 (a CHECK violation), 22, 42 and an internal
/// error (XX) stay, 500. The detail is the error's message.
#[tokio::test]
async fn a_refused_cancel_statement_answers_by_its_sqlstate_class() {
    for (code, status_code) in [
        ("40001", 503),
        ("55P03", 503),
        ("57014", 503),
        ("23514", 500),
        ("22023", 500),
        ("42P01", 500),
        ("XX000", 500),
    ] {
        let server_error = crate::test_support::raised_server_error(code, "test refusal").await;
        let error = TaskActionError::cancel_failed(
            Uuid::nil(),
            crate::broker::BrokerError::Statement(crate::db::StatementFailure::StatementRefused {
                statement: "cancel_task",
                cause: crate::db::RefusalCause::Server(Box::new(server_error)),
            }),
        );
        let message = error.message().to_owned();

        let outcome = task_action_outcome(Err(error));

        assert_eq!(outcome.status_code(), status_code, "{code}");
        assert_eq!(
            outcome.into_body(),
            ActionBody::Detail { detail: message },
            "{code}"
        );
    }
}

/// Python `test_monitoring_task_actions.py::TestCancelTaskRefuses::
/// test_history_row_with_an_unknown_status_is_stored_data_invalid`, at the
/// mapping level: stored data that does not parse is 500, not 503.
#[test]
fn stored_data_invalid_is_a_server_error() {
    let outcome = task_action_outcome(Err(TaskActionError::StoredDataInvalid {
        message: "stored data does not parse".to_owned(),
        task_id: Uuid::nil(),
        cause: crate::core::lifecycle::OutcomeDecodeError("NOT_KNOWN".to_owned()),
    }));

    assert_eq!(outcome.status_code(), 500);
    assert_eq!(
        serde_json::to_string(&TaskActionErrorCode::StoredDataInvalid).unwrap(),
        r#""STORED_DATA_INVALID""#
    );
}
