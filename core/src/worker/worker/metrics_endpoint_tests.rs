//! A worker whose metrics endpoint address is in use at its start: the
//! worker starts and claims, its snapshot shows the endpoint down with the
//! cause, and once the port is free the endpoint binds, the snapshot shows
//! it serving, and its probes answer. Database test; binds `127.0.0.1`.

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

use super::Worker;
use crate::broker::PostgresBroker;
use crate::core::config::worker_profile::SelectedProfile;
use crate::core::config::AppConfig;
use crate::telemetry::endpoint::bind_loopback_free_port;
use crate::telemetry::listen::ListenAddress;
use crate::telemetry::worker::{DeclaredCodesByTask, TelemetryOn};
use crate::test_support::sql::{self, TestPool};
use crate::test_support::telemetry::worker_registry;
use crate::test_support::TestDatabase;
use crate::worker::children::{ChildLaunch, PreloadProgram};
use crate::worker::claim_pass::RegisteredClasses;
use crate::worker::config::WorkerConfig;
use crate::worker::executor::protocol::{
    ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl,
};
use crate::worker::executor::slots::scripted::idle_child_of_any_kind;

/// The longest the test waits for each fact of the run.
const FACT_BOUND: Duration = Duration::from_secs(60);

/// The wait between two reads of the database.
const POLL: Duration = Duration::from_millis(100);

/// The bound of the stop of the worker after its cancel.
const STOP_BOUND: Duration = Duration::from_secs(30);

/// The snapshot interval of the test worker: the least that the config
/// takes in the core tests, so the snapshot follows the endpoint state.
const SNAPSHOT_INTERVAL_MS: u64 = 1_000;

/// The app config of the test worker: the snapshot every second, a claim
/// loop that polls every 100 ms.
fn test_app_config(url: &str) -> AppConfig {
    let mut config = AppConfig::for_database_url(url);
    config.recovery.worker_state_snapshot_interval_ms = SNAPSHOT_INTERVAL_MS;
    config.resilience.notify_poll_interval_ms = 100;
    config
}

/// A worker of one isolated child that boots, then exits at the next frame
/// it reads.
fn worker_of_one_idle_child(broker: Arc<PostgresBroker>, app_config: AppConfig) -> Worker {
    let database_url = DatabaseUrl::new(app_config.broker.database_url.clone());
    Worker::of_child_specs(
        broker,
        crate::test_support::child_specs::no_child_specs(),
        app_config,
        WorkerConfig {
            queues: vec!["default".to_owned()],
            ..WorkerConfig::default()
        },
        SelectedProfile {
            name: "default".to_owned(),
            profile: serde_json::from_value(serde_json::json!({
                "isolated": {"kind": "isolated_children", "count": 1}
            }))
            .expect("a valid test profile"),
        },
        ChildLaunch {
            command: idle_child_of_any_kind(),
            preload: PreloadProgram::NoPreloadProgram,
            program: ChildProgram::Rust,
            log_level: ChildLogLevel::Info,
            database_url,
            connection: ChildConnection::default(),
        },
        RegisteredClasses::default(),
    )
    .expect("a valid test worker")
}

/// Log each claim that committed: an AFTER trigger writes a row in the
/// transaction of the claim.
async fn log_committed_claims(pool: &TestPool) {
    sql::raw_sql(
        "CREATE TABLE test_committed_claims (task_id uuid NOT NULL);
         CREATE FUNCTION test_log_committed_claim() RETURNS trigger LANGUAGE plpgsql AS $body$
         BEGIN
             INSERT INTO test_committed_claims VALUES (NEW.id);
             RETURN NULL;
         END
         $body$;
         CREATE TRIGGER test_log_committed_claim AFTER UPDATE ON horsies_tasks
             FOR EACH ROW WHEN (OLD.status = 'PENDING' AND NEW.status = 'CLAIMED')
             EXECUTE FUNCTION test_log_committed_claim();",
    )
    .execute(pool)
    .await
    .expect("install the claim log");
}

/// One PENDING task on the default queue.
async fn insert_pending_task(pool: &TestPool) -> Uuid {
    let task_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at,
             command_fingerprint_version, command_fingerprint, idempotency_key_digest,
             status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
             started_at, finalizing_at, finalizing_by_worker_id,
             worker_pid, worker_hostname, worker_process_name,
             retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
             updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
             $1, 'endpoint_test', 'default', 100, '{}', NULL, 'ISOLATED',
             'forever', NOW(), NOW(),
             1, decode(repeat('00', 32), 'hex'), NULL,
             'PENDING', NOW(), NULL, NULL, NULL,
             NULL, NULL, NULL,
             NULL, NULL, NULL,
             0, 0, NULL, NULL, FALSE,
             NOW(), FALSE, NULL, NULL
         )",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .expect("insert a pending task");
    task_id
}

async fn committed_claims(pool: &TestPool, task_id: Uuid) -> i64 {
    sql::query_scalar::<i64>("SELECT count(*) FROM test_committed_claims WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the committed claims")
}

/// The `telemetry` column of the latest snapshot of `worker_id`.
async fn latest_telemetry(pool: &TestPool, worker_id: &str) -> Option<serde_json::Value> {
    sql::query_scalar::<serde_json::Value>(
        "SELECT telemetry FROM horsies_worker_states WHERE worker_id = $1
         ORDER BY snapshot_at DESC LIMIT 1",
    )
    .bind(worker_id)
    .fetch_optional(pool)
    .await
    .expect("read the latest snapshot")
}

/// Wait until the latest snapshot shows the endpoint in `state`, and return
/// its endpoint member.
async fn wait_for_endpoint_state(
    pool: &TestPool,
    worker_id: &str,
    state: &str,
) -> serde_json::Value {
    tokio::time::timeout(FACT_BOUND, async {
        loop {
            if let Some(telemetry) = latest_telemetry(pool, worker_id).await {
                if telemetry["endpoint"]["state"] == state {
                    break telemetry["endpoint"].clone();
                }
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_elapsed| panic!("no snapshot showed the endpoint {state} in time"))
}

/// The status line and the body of `GET path` on `port`.
async fn get(port: u16, path: &str) -> (String, String) {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("the endpoint accepts");
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("the request is written");
    let mut text = String::new();
    let _read = tokio::time::timeout(FACT_BOUND, stream.read_to_string(&mut text))
        .await
        .expect("the endpoint answers in time")
        .expect("the answer is read");
    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("an HTTP answer has a head and a body");
    let status = head.lines().next().unwrap_or_default().to_owned();
    (status, body.to_owned())
}

#[tokio::test]
#[serial_test::serial]
async fn a_port_in_use_leaves_the_worker_claiming_and_the_endpoint_serves_once_free() {
    let database = TestDatabase::create().await;
    let pool = TestPool::connect(database.url())
        .await
        .expect("connect the test database");
    log_committed_claims(&pool).await;
    let held = bind_loopback_free_port().await;
    let port = held.local_address().port();
    let address = ListenAddress::parse(&format!("127.0.0.1:{port}")).expect("a valid address");

    let app_config = test_app_config(database.url());
    let broker = PostgresBroker::connect_worker_with(&app_config.broker, crate::ClientFamily::Rust)
        .await
        .expect("connect the broker of a worker");
    let telemetry = TelemetryOn::metrics_only(worker_registry(), DeclaredCodesByTask::none())
        .expect("the telemetry of a worker registry");
    let worker = worker_of_one_idle_child(Arc::new(broker), app_config)
        .with_metrics_endpoint(address, Arc::new(telemetry));
    let worker_id = worker.worker_id().to_owned();
    let pending = insert_pending_task(&pool).await;
    let cancel = worker.cancel_token();
    let run = tokio::spawn(async move { worker.run().await });

    // The port is held: the worker claims, and its snapshot shows the
    // endpoint down with the failed bind.
    tokio::time::timeout(FACT_BOUND, async {
        while committed_claims(&pool, pending).await == 0 {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("the worker claims while its endpoint is down");
    let down = wait_for_endpoint_state(&pool, &worker_id, "down").await;
    assert_eq!(down["host"], "127.0.0.1");
    assert_eq!(down["port"], port);
    assert_eq!(down["cause"]["kind"], "bind_failed");

    // The port is free: the endpoint binds, the snapshot shows it serving,
    // and the probes answer.
    drop(held);
    let serving = wait_for_endpoint_state(&pool, &worker_id, "serving").await;
    assert_eq!(serving["local_address"], format!("127.0.0.1:{port}"));
    let (status, body) = get(port, "/livez").await;
    assert!(status.starts_with("HTTP/1.1 200 "), "{status}: {body}");
    let (status, body) = get(port, "/metrics").await;
    assert!(status.starts_with("HTTP/1.1 200 "), "{status}");
    assert!(
        body.contains("horsies_worker_claim_passes_total{result=\"claimed\"}"),
        "{body}"
    );

    cancel.cancel();
    let _ended = tokio::time::timeout(STOP_BOUND, run)
        .await
        .expect("the worker stops within the bound")
        .expect("the worker task joins");
    pool.close().await;
    database.drop().await;
}
