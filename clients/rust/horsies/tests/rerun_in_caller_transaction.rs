//! The rerun in the caller's transaction through both adapters, against
//! PostgreSQL: a rerun commits or rolls back with the caller's transaction
//! (Python `rerun_task(connection, ...)`), and each adapter encodes the
//! parameters and decodes every column of the source read, its NULLs
//! included.
//!
//! Each test runs on a database of its own, a copy of the core's test
//! template (`core/src/test_support/database.rs`): the
//! installed schema, Rust family.

#![cfg(all(feature = "tokio-postgres", feature = "sqlx"))]
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use horsies::rerun::{
    rerun_task_in, AppRerunError, CallerRerunError, RerunEnqueuePolicy, RerunOutcome, RerunTask,
    SqlxCaller, TaskAbsenceReason, TokioPostgresCaller, UnknownStoredFamily,
};
use horsies::{AppConfig, Horsies};
use horsies_core::{BrokerError, OperationalErrorCode, TaskError, TaskResult};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

/// The test databases of the core. This binary creates copies of the
/// template only.
#[allow(dead_code)]
#[path = "../../../../core/src/test_support/database.rs"]
mod test_database;

use test_database::TestDatabase;

/// Installs the schema into a test template through the public API, with
/// the Rust client family.
async fn install_template(url: &str) {
    let broker = horsies_core::PostgresBroker::connect(url, horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

/// The retention class of the seeded source and of the reruns.
const CLASS: &str = "forever";
const WORKER: &str = "rerun-adapter-worker";

async fn pool_of(database: &TestDatabase) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(database.url())
        .await
        .unwrap()
}

fn policy() -> RerunEnqueuePolicy {
    RerunEnqueuePolicy::new(CLASS, true, chrono::Duration::hours(24)).unwrap()
}

/// A FAILED plain task with a retained input, moved to history by the
/// failure function (the seed of the core's rerun tests).
async fn seed_failed_source(pool: &PgPool) -> Uuid {
    let task_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             status, sent_at, enqueued_at, started_at, claimed_at,
             claimed_by_worker_id, claim_expires_at, good_until, is_workflow_task,
             worker_pid, worker_hostname, worker_process_name,
             retry_count, max_retries,
             command_fingerprint_version, command_fingerprint,
             retention_class_key, rerun_of_task_id,
             rerun_root_task_id, retain_rerun_input,
             created_at, updated_at
         ) VALUES (
             $1, 'rerun.adapter', 'default', 50, '{\"k\":\"v\"}', '{\"timeout_ms\":5}',
             'ISOLATED', 'RUNNING', NOW(), NOW(), NOW(), NOW(), $2, NULL, NULL, FALSE,
             81, 'adapter-host', 'adapter-process', 0, 3, 1, $3, $4, NULL, NULL, TRUE,
             NOW(), NOW()
         )",
    )
    .bind(task_id)
    .bind(WORKER)
    .bind(vec![9_u8; 32])
    .bind(CLASS)
    .execute(pool)
    .await
    .unwrap();
    let result_json = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(
        TaskError::builtin(OperationalErrorCode::TaskException, "adapter source"),
    ))
    .unwrap();
    let finished: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM horsies_finish_failed_attempt(
             $1, $2, (SELECT t.claimed_at FROM horsies_tasks t WHERE t.id = $1),
             'NONE', NULL, NULL, NULL, $3, $4, $5, $6, $7, $8, 'REFUSE', TRUE)",
    )
    .bind(task_id)
    .bind(WORKER)
    .bind(result_json.as_str())
    .bind(Some("ADAPTER"))
    .bind(Some("adapter failure"))
    .bind("FAILED")
    .bind(None::<&str>)
    .bind(Some("adapter failure"))
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(finished, 1);
    task_id
}

/// Whether the task `task_id` exists in the live table.
async fn task_exists(pool: &PgPool, task_id: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = $1)")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The new task id of an enqueued rerun of `source`.
fn enqueued(outcome: &RerunOutcome, source: Uuid) -> Uuid {
    match outcome {
        RerunOutcome::Enqueued {
            new_task_id,
            source_task_id,
            ..
        } if *source_task_id == source => *new_task_id,
        other => panic!("expected an enqueued rerun of {source}, got {other:?}"),
    }
}

/// tokio-postgres: a rerun that the caller rolls back leaves no task; one
/// that the caller commits leaves the new task.
#[tokio::test]
async fn a_tokio_postgres_rerun_commits_or_rolls_back_with_the_caller() {
    let database = TestDatabase::create().await;
    let pool = pool_of(&database).await;
    let source = seed_failed_source(&pool).await;
    let command = RerunTask::new(source, None, None);
    let mut client = database.connect().await;

    let mut transaction = client.transaction().await.unwrap();
    let rolled_back = rerun_task_in(
        &mut TokioPostgresCaller::new(&mut transaction),
        &command,
        &policy(),
        Utc::now(),
    )
    .await
    .unwrap();
    let rolled_back_id = enqueued(&rolled_back, source);
    transaction.rollback().await.unwrap();
    assert!(!task_exists(&pool, rolled_back_id).await);

    let mut transaction = client.transaction().await.unwrap();
    let committed = rerun_task_in(
        &mut TokioPostgresCaller::new(&mut transaction),
        &command,
        &policy(),
        Utc::now(),
    )
    .await
    .unwrap();
    let committed_id = enqueued(&committed, source);
    transaction.commit().await.unwrap();
    assert!(task_exists(&pool, committed_id).await);

    pool.close().await;
    database.drop().await;
}

/// sqlx: a rerun that the caller rolls back leaves no task; one that the
/// caller commits leaves the new task.
#[tokio::test]
async fn a_sqlx_rerun_commits_or_rolls_back_with_the_caller() {
    let database = TestDatabase::create().await;
    let pool = pool_of(&database).await;
    let source = seed_failed_source(&pool).await;
    let command = RerunTask::new(source, None, None);

    let mut transaction = pool.begin().await.unwrap();
    let rolled_back = rerun_task_in(
        &mut SqlxCaller::new(&mut transaction),
        &command,
        &policy(),
        Utc::now(),
    )
    .await
    .unwrap();
    let rolled_back_id = enqueued(&rolled_back, source);
    transaction.rollback().await.unwrap();
    assert!(!task_exists(&pool, rolled_back_id).await);

    let mut transaction = pool.begin().await.unwrap();
    let committed = rerun_task_in(
        &mut SqlxCaller::new(&mut transaction),
        &command,
        &policy(),
        Utc::now(),
    )
    .await
    .unwrap();
    let committed_id = enqueued(&committed, source);
    transaction.commit().await.unwrap();
    assert!(task_exists(&pool, committed_id).await);

    pool.close().await;
    database.drop().await;
}

/// A source that no table holds: each adapter decodes the read's row, NULL
/// columns of every type included, and the rerun answers `SourceAbsent`.
#[tokio::test]
async fn an_absent_source_reads_through_both_adapters() {
    let database = TestDatabase::create().await;
    let pool = pool_of(&database).await;
    let command = RerunTask::new(Uuid::new_v4(), None, None);

    let mut client = database.connect().await;
    let mut transaction = client.transaction().await.unwrap();
    let through_tokio_postgres = rerun_task_in(
        &mut TokioPostgresCaller::new(&mut transaction),
        &command,
        &policy(),
        Utc::now(),
    )
    .await
    .unwrap();
    transaction.rollback().await.unwrap();

    let mut transaction = pool.begin().await.unwrap();
    let through_sqlx = rerun_task_in(
        &mut SqlxCaller::new(&mut transaction),
        &command,
        &policy(),
        Utc::now(),
    )
    .await
    .unwrap();
    transaction.rollback().await.unwrap();

    for outcome in [through_tokio_postgres, through_sqlx] {
        assert!(
            matches!(
                outcome,
                RerunOutcome::SourceAbsent { task_id, reason: TaskAbsenceReason::NotClassified }
                    if task_id == command.source_task_id()
            ),
            "{outcome:?}"
        );
    }
    pool.close().await;
    database.drop().await;
}

/// Writes `value` into the family row past its CHECK
/// (`horsies_client_family_family_known`), which it drops first: stored data
/// that no horsies path writes.
async fn corrupt_family_row(pool: &PgPool, value: &str) {
    sqlx::query(
        "ALTER TABLE horsies_client_family \
         DROP CONSTRAINT horsies_client_family_family_known",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE horsies_client_family SET family = $1")
        .bind(value)
        .execute(pool)
        .await
        .unwrap();
}

/// A family row that holds no client family is corrupt stored data. A rerun
/// in the caller's transaction ends with `UnknownStoredFamily` and the stored
/// value before its write; the app's own rerun is refused at its schema
/// install with `BrokerError::UnknownStoredFamily`. No new task exists.
#[tokio::test]
async fn a_family_row_that_holds_no_client_family_is_refused_with_its_value() {
    let database = TestDatabase::create().await;
    let pool = pool_of(&database).await;
    let source = seed_failed_source(&pool).await;
    corrupt_family_row(&pool, "GO").await;
    let command = RerunTask::new(source, None, None);
    let tasks_before: i64 = sqlx::query_scalar("SELECT count(*) FROM horsies_tasks")
        .fetch_one(&pool)
        .await
        .unwrap();

    let mut client = database.connect().await;
    let mut transaction = client.transaction().await.unwrap();
    let refused = rerun_task_in(
        &mut TokioPostgresCaller::new(&mut transaction),
        &command,
        &policy(),
        Utc::now(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            &refused,
            CallerRerunError::UnknownStoredFamily(UnknownStoredFamily { stored }) if stored == "GO"
        ),
        "{refused:?}"
    );
    transaction.rollback().await.unwrap();

    let app = Horsies::new(AppConfig::for_database_url(database.url())).unwrap();
    match app.rerun_task(command, policy()).await {
        Err(AppRerunError::Connect(BrokerError::UnknownStoredFamily(UnknownStoredFamily {
            stored,
        }))) => assert_eq!(stored, "GO"),
        other => panic!("expected the unknown stored family at the install, got {other:?}"),
    }
    let tasks_after: i64 = sqlx::query_scalar("SELECT count(*) FROM horsies_tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(tasks_after, tasks_before);

    pool.close().await;
    database.drop().await;
}
