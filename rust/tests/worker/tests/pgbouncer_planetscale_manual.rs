#![allow(clippy::unwrap_used)]

//! Manual managed-provider PgBouncer probe.
//!
//! This test is ignored by default because it drops all `horsies_*` objects in
//! the target database. Run it only against a disposable branch/database:
//!
//! ```text
//! HORSIES_PLANETSCALE_TEST=1 \
//! cargo test -p horsies-test-worker --test pgbouncer_planetscale_manual -- --ignored --nocapture --test-threads=1
//! ```

use std::error::Error;
use std::io::Write;
use std::sync::{Arc, Once};
use std::time::Duration;

use horsies::{
    AppConfig, Horsies, PostgresBroker, PostgresConfig, TaskResult, WorkflowSpecBuilder,
};
use horsies_test_support::{e2e::worker::start_worker, pgbouncer};
use horsies_test_worker::tasks::{wf_double, wf_produce_int, DoubleInput};
use serial_test::serial;
use sqlx::PgPool;
use tokio::sync::Barrier;

fn pgbouncer_config(runtime_url: &str, session_url: &str) -> PostgresConfig {
    let mut config = PostgresConfig::from_pgbouncer_urls(runtime_url, session_url);
    config.pool_size = 2;
    config.max_overflow = 0;
    config.pool_timeout = 10;
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

#[tokio::test]
#[serial]
#[ignore = "requires a disposable managed provider database and drops horsies_* objects"]
async fn planetscale_split_urls_concurrent_cold_start_task_and_workflow(
) -> Result<(), Box<dyn Error>> {
    if std::env::var("HORSIES_PLANETSCALE_TEST").as_deref() != Ok("1") {
        panic!("set HORSIES_PLANETSCALE_TEST=1 to confirm this destructive manual probe");
    }

    ensure_task_handles_registered();
    prefer_current_test_worker_binary();
    let (direct_url, transaction_url) = pgbouncer::managed_provider_split_urls_from_env()
        .expect("set HORSIES_MANAGED_DATABASE_URL_DIRECT/HORSIES_MANAGED_DATABASE_URL_TRANSACTION or DATABASE_* pieces");
    let direct_pool = PgPool::connect(&direct_url).await?;
    pgbouncer::drop_horsies_schema(&direct_pool).await;

    let result = async {
        let config = pgbouncer_config(&transaction_url, &direct_url);
        let barrier = Arc::new(Barrier::new(4));
        let mut joins = Vec::new();
        for i in 0..4 {
            let config = config.clone();
            let barrier = Arc::clone(&barrier);
            joins.push(tokio::spawn(async move {
                let broker = PostgresBroker::connect_with(&config).await?;
                barrier.wait().await;
                broker.migrate().await?;
                broker.health_check().await?;
                broker.check_listener_delivery().await?;
                broker
                    .enqueue(
                        "pgbouncer_manual_cold_start",
                        None,
                        Some("{}"),
                        "default",
                        100,
                        None,
                        None,
                        None,
                        None,
                        &format!("pgbouncer-manual-cold-start-{i}"),
                        None,
                    )
                    .await?;
                Ok::<(), horsies::BrokerError>(())
            }));
        }
        for join in joins {
            join.await??;
        }

        let task_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE task_name = 'pgbouncer_manual_cold_start'",
        )
        .fetch_one(&direct_pool)
        .await?;
        assert_eq!(task_count, 4);

        {
            let app_config = app_config(&transaction_url, &direct_url);
            let mut config_file = tempfile::NamedTempFile::new()?;
            config_file.write_all(serde_json::to_string_pretty(&app_config)?.as_bytes())?;
            config_file.flush()?;
            let config_path = config_file.path().to_string_lossy().into_owned();

            let _worker = start_worker(
                &config_path,
                &["--concurrency", "2"],
                "worker started",
                Duration::from_secs(30),
            );

            let broker =
                PostgresBroker::connect_with(&pgbouncer_config(&transaction_url, &direct_url))
                    .await?;
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
                    "pgbouncer-manual-task",
                    None,
                )
                .await?;
            let task_result: TaskResult<i64> = broker
                .get_result(&task_id, Some(Duration::from_secs(20)))
                .await?;
            assert_eq!(task_result.unwrap(), 42);

            let mut app = Horsies::new(app_config)?;
            horsies_test_worker::tasks::register(&mut app).unwrap();
            let mut builder = WorkflowSpecBuilder::new("pgbouncer_manual_workflow");
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
            let handle = app.start::<i64>(builder.build().unwrap()).await?;
            let workflow_result = handle.get(Some(Duration::from_secs(30))).await;
            assert_eq!(workflow_result.unwrap(), 42);
        }

        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    pgbouncer::drop_horsies_schema(&direct_pool).await;
    direct_pool.close().await;
    result
}
