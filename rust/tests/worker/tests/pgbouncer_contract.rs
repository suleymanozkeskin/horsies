#![allow(clippy::unwrap_used)]

//! PgBouncer compatibility contracts.
//!
//! These tests are opt-in because they require the Docker fixture stack:
//!
//! ```text
//! docker compose -f tests/fixtures/pgbouncer/compose.yaml up -d --wait
//! HORSIES_PGBOUNCER_TEST=1 cargo test -p horsies-test-worker --test pgbouncer_contract -- --test-threads=1
//! docker compose -f tests/fixtures/pgbouncer/compose.yaml down -v
//! ```

use std::error::Error;
use std::io::Write;
use std::sync::Once;
use std::time::Duration;

use horsies::{
    AppConfig, Horsies, PostgresBroker, PostgresConfig, TaskResult, WorkflowSpecBuilder,
};
use horsies_test_support::{
    e2e::{
        db_poll::wait_for_task_status, worker::start_worker, workflow::wait_for_workflow_completion,
    },
    pgbouncer,
};
use horsies_test_worker::tasks::{wf_double, wf_produce_int, DoubleInput};
use serial_test::serial;
use sqlx::PgPool;

fn skip_if_disabled() -> bool {
    if !pgbouncer::enabled() {
        eprintln!("skipping PgBouncer contract test; set HORSIES_PGBOUNCER_TEST=1");
        return true;
    }
    false
}

fn pgbouncer_config(runtime_url: &str, session_url: &str) -> PostgresConfig {
    let mut config = PostgresConfig::from_pgbouncer_urls(runtime_url, session_url);
    config.pool_size = 2;
    config.max_overflow = 0;
    config.pool_timeout = 5;
    config.pool_recycle = 600;
    config
}

fn direct_config(url: &str) -> PostgresConfig {
    let mut config = PostgresConfig::from_url(url);
    config.pool_size = 2;
    config.max_overflow = 0;
    config.pool_timeout = 5;
    config.pool_recycle = 600;
    config
}

fn app_config(runtime_url: &str, session_url: &str) -> AppConfig {
    let mut config = AppConfig::for_database_url(runtime_url);
    config.broker = pgbouncer_config(runtime_url, session_url);
    config.resilience.db_retry_initial_ms = 500;
    config.resilience.db_retry_max_ms = 500;
    config.resilience.db_retry_max_attempts = 3;
    config
}

fn ensure_task_handles_registered() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let mut app = Horsies::new(AppConfig::for_database_url(
            "postgresql://postgres:testpassword@localhost:15432/horsies",
        ))
        .unwrap();
        horsies_test_worker::tasks::register(&mut app).unwrap();
    });
}

fn prefer_current_test_worker_binary() {
    if let Some(path) = option_env!("CARGO_BIN_EXE_horsies-test-worker") {
        std::env::set_var("HORSIES_TEST_WORKER_BIN", path);
    }
}

async fn enqueue_contract_task(
    broker: &PostgresBroker,
    label: &str,
) -> Result<String, horsies::BrokerError> {
    broker
        .enqueue(
            "pgbouncer_contract_task",
            None,
            Some("{}"),
            "default",
            100,
            None,
            None,
            None,
            None,
            label,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .map(|task_id| task_id.to_string())
}

#[tokio::test]
#[serial]
async fn transaction_pool_split_urls_migrate_health_check_and_enqueue() -> Result<(), Box<dyn Error>>
{
    if skip_if_disabled() {
        return Ok(());
    }

    let urls = pgbouncer::create_isolated_database("horsies_pgbouncer_contract").await;
    let result = async {
        let broker =
            PostgresBroker::connect_with(&pgbouncer_config(&urls.transaction, &urls.direct))
                .await?;
        broker.migrate().await?;
        broker.health_check().await?;
        broker.check_listener_delivery().await?;

        let mut task_ids = Vec::new();
        for i in 0..30 {
            let label = format!("pgbouncer-contract-split-{i}");
            task_ids.push(enqueue_contract_task(&broker, &label).await?);
        }
        let pool = PgPool::connect(&urls.direct).await?;
        let persisted: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_tasks WHERE id = ANY($1::TEXT[])")
                .bind(&task_ids)
                .fetch_one(&pool)
                .await?;
        pool.close().await;
        assert_eq!(persisted, 30);
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    pgbouncer::drop_isolated_database(&urls).await;
    result
}

#[tokio::test]
#[serial]
async fn session_pool_mode_supports_listen_without_transaction_mode_flag(
) -> Result<(), Box<dyn Error>> {
    if skip_if_disabled() {
        return Ok(());
    }

    let urls = pgbouncer::create_isolated_database("horsies_pgbouncer_session").await;
    let result = async {
        let broker = PostgresBroker::connect_with(&direct_config(&urls.session)).await?;
        broker.migrate().await?;
        broker.health_check().await?;
        broker.check_listener_delivery().await?;
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    pgbouncer::drop_isolated_database(&urls).await;
    result
}

#[tokio::test]
#[serial]
async fn transaction_pool_as_session_url_fails_loudly() -> Result<(), Box<dyn Error>> {
    if skip_if_disabled() {
        return Ok(());
    }

    let urls = pgbouncer::create_isolated_database("horsies_pgbouncer_bad_session").await;
    let result = async {
        let broker =
            PostgresBroker::connect_with(&pgbouncer_config(&urls.transaction, &urls.transaction))
                .await?;
        broker.health_check().await?;
        let err = broker.check_listener_delivery().await.unwrap_err();
        assert!(
            err.to_string().contains("LISTEN delivery probe")
                || err.to_string().to_lowercase().contains("listen"),
            "unexpected error: {err}",
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    pgbouncer::drop_isolated_database(&urls).await;
    result
}

#[tokio::test]
#[serial]
async fn statement_pool_as_session_url_fails_loudly() -> Result<(), Box<dyn Error>> {
    if skip_if_disabled() {
        return Ok(());
    }

    let urls = pgbouncer::create_isolated_database("horsies_pgbouncer_statement").await;
    let result = async {
        let broker =
            PostgresBroker::connect_with(&pgbouncer_config(&urls.statement, &urls.statement))
                .await?;
        broker.health_check().await?;
        let err = broker.check_listener_delivery().await.unwrap_err();
        let text = err.to_string().to_lowercase();
        assert!(
            text.contains("listen") || text.contains("statement") || text.contains("pool"),
            "unexpected error: {err}",
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    pgbouncer::drop_isolated_database(&urls).await;
    result
}

#[tokio::test]
#[serial]
async fn split_url_connect_fails_when_session_url_is_unreachable() -> Result<(), Box<dyn Error>> {
    if skip_if_disabled() {
        return Ok(());
    }

    let urls = pgbouncer::create_isolated_database("horsies_pgbouncer_bad_session_host").await;
    let result = async {
        let err = match PostgresBroker::connect_with(&pgbouncer_config(
            &urls.transaction,
            "postgresql://postgres:testpassword@127.0.0.1:1/horsies",
        ))
        .await
        {
            Ok(_) => panic!("expected unreachable session_database_url to fail during connect"),
            Err(err) => err,
        };
        let text = err.to_string().to_lowercase();
        assert!(
            text.contains("connection") || text.contains("database error"),
            "unexpected error: {err}",
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    pgbouncer::drop_isolated_database(&urls).await;
    result
}

#[tokio::test]
#[serial]
async fn transaction_pool_without_prepared_statement_tracking_fails_loudly(
) -> Result<(), Box<dyn Error>> {
    if skip_if_disabled() {
        return Ok(());
    }

    let urls = pgbouncer::create_isolated_database("horsies_pgbouncer_prepared").await;
    let result = async {
        let direct_pool = PgPool::connect(&urls.direct).await?;
        horsies::run_horsies_migrations(&direct_pool).await?;
        direct_pool.close().await;

        let mut prepared_error = None;
        match PostgresBroker::connect_with(&direct_config(&urls.prepared)).await {
            Ok(broker_without_fix) => {
                for i in 0..30 {
                    let label = format!("pgbouncer-prepared-negative-{i}");
                    if let Err(err) = enqueue_contract_task(&broker_without_fix, &label).await {
                        prepared_error = Some(err.to_string());
                        break;
                    }
                }
            }
            Err(err) => prepared_error = Some(err.to_string()),
        }
        assert!(
            prepared_error.is_some(),
            "expected PgBouncer prepared-statement failure without pgbouncer_transaction_mode",
        );

        let broker_without_tracking =
            PostgresBroker::connect_with(&pgbouncer_config(&urls.prepared, &urls.direct)).await?;
        let err = broker_without_tracking
            .check_listener_delivery()
            .await
            .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("max_prepared_statements") || text.contains("prepared statement"),
            "unexpected error: {err}",
        );

        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    pgbouncer::drop_isolated_database(&urls).await;
    result
}

#[tokio::test]
#[serial]
async fn worker_processes_task_and_workflow_through_pgbouncer_split_urls(
) -> Result<(), Box<dyn Error>> {
    if skip_if_disabled() {
        return Ok(());
    }

    ensure_task_handles_registered();
    prefer_current_test_worker_binary();
    let urls = pgbouncer::create_isolated_database("horsies_pgbouncer_e2e").await;
    let result = async {
        let config = app_config(&urls.transaction, &urls.direct);
        let mut config_file = tempfile::NamedTempFile::new()?;
        config_file.write_all(serde_json::to_string_pretty(&config)?.as_bytes())?;
        config_file.flush()?;
        let config_path = config_file.path().to_string_lossy().into_owned();

        let _worker = start_worker(
            &config_path,
            &["--concurrency", "2"],
            "worker started",
            Duration::from_secs(20),
        );

        let pool = PgPool::connect(&urls.direct).await?;
        let broker =
            PostgresBroker::connect_with(&pgbouncer_config(&urls.transaction, &urls.direct))
                .await?;
        broker.migrate().await?;

        let task_id = broker
            .enqueue(
                "e2e_simple",
                None,
                Some(r#"{"x": 21}"#),
                "default",
                100,
                None,
                None,
                None,
                None,
                "pgbouncer-e2e-task",
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        wait_for_task_status(
            &pool,
            &task_id.to_string(),
            "COMPLETED",
            Duration::from_secs(15),
        )
        .await;

        let mut app = Horsies::new(config)?;
        horsies_test_worker::tasks::register(&mut app).unwrap();
        let mut builder = WorkflowSpecBuilder::new("pgbouncer_e2e_workflow");
        let first = builder.task(
            wf_produce_int::node()
                .unwrap()
                .node_id("first")
                .kwargs(r#"{"value": 21}"#.to_owned()),
        );
        let second = builder.task(
            wf_double::node()
                .unwrap()
                .node_id("second")
                .arg_from(DoubleInput::field_input_result(), first),
        );
        builder.output(second);
        let spec = builder.build().unwrap();
        let handle = app.start::<i64>(spec).await?;
        let status =
            wait_for_workflow_completion(&pool, handle.workflow_id(), Duration::from_secs(20))
                .await;
        assert_eq!(status, "COMPLETED");

        let result_json: String =
            sqlx::query_scalar("SELECT result FROM horsies_workflows WHERE id = $1")
                .bind(handle.workflow_id())
                .fetch_one(&pool)
                .await?;
        let workflow_result: TaskResult<i64> = serde_json::from_str(&result_json)?;
        assert_eq!(workflow_result.unwrap(), 42);

        pool.close().await;
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    pgbouncer::drop_isolated_database(&urls).await;
    result
}
