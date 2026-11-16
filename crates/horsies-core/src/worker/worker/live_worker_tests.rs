//! Port of Python `tests/e2e/test_worker_health.py::test_ping_and_worker_state_correlate`:
//! one live worker answers a ping, and the worker state reads list the same
//! worker with its pid, host and queues.

use std::sync::Arc;
use std::time::Duration;

use super::Worker;
use crate::broker::PostgresBroker;
use crate::core::config::AppConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::TaskRegistry;
use crate::worker::config::WorkerConfig;

/// The bound of the wait for the first snapshot, which the worker writes at
/// start.
const FIRST_SNAPSHOT_BOUND: Duration = Duration::from_secs(30);

/// How often the test reads the worker state while it waits.
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The reply window of the ping.
const PING_WINDOW: Duration = Duration::from_secs(10);

/// The bound of the wait for the worker to stop after its cancel.
const STOP_BOUND: Duration = Duration::from_secs(30);

#[tokio::test]
#[serial_test::serial]
#[ignore = "waits on #678: the claim writes the removed claimed column"]
async fn a_live_worker_answers_a_ping_and_lists_its_state() {
    let url = crate::broker::terminalization_matrix::migrated_database_url().await;
    let app_config = AppConfig::for_database_url(url);
    let broker = Arc::new(
        PostgresBroker::connect_with(&app_config.broker)
            .await
            .expect("connect the broker"),
    );
    let worker = Worker::new(
        Arc::clone(&broker),
        Arc::new(TaskRegistry::new()),
        Arc::new(WorkflowSpecRegistry::new()),
        app_config,
        WorkerConfig {
            queues: vec!["default".to_owned()],
            concurrency: 1,
            ..WorkerConfig::default()
        },
    )
    .expect("worker");
    let worker_id = worker.worker_id().to_owned();
    let cancel = worker.cancel_token();
    let run = tokio::spawn(async move { worker.run().await });

    let snapshot = tokio::time::timeout(FIRST_SNAPSHOT_BOUND, async {
        loop {
            match broker
                .get_worker_state(&worker_id)
                .await
                .expect("read the worker state")
            {
                Some(snapshot) => break snapshot,
                None => tokio::time::sleep(STATE_POLL_INTERVAL).await,
            }
        }
    })
    .await;
    let pongs = broker
        .ping_workers(Some(worker_id.as_str()), PING_WINDOW, None)
        .await
        .expect("ping the worker");
    let listed = broker.list_worker_states().await.expect("list the workers");

    cancel.cancel();
    let ended = tokio::time::timeout(STOP_BOUND, run)
        .await
        .expect("the worker stops within the bound")
        .expect("the worker task joins");
    sqlx::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
        .bind(&worker_id)
        .execute(broker.pool())
        .await
        .expect("delete the snapshots");

    let snapshot = snapshot.expect("the worker lists its state within the bound");
    assert!(ended.is_ok(), "the worker stopped with an error: {ended:?}");
    assert_eq!(pongs.len(), 1, "one pong from the targeted worker");
    let pong = &pongs[0];
    assert_eq!(pong.worker_id, worker_id);
    assert!(snapshot.pid > 0);
    assert_eq!(pong.pid, snapshot.pid);
    assert_eq!(pong.hostname, snapshot.hostname);
    assert!(snapshot.queues.iter().any(|queue| queue == "default"));
    assert_eq!(
        listed
            .iter()
            .filter(|state| state.worker_id == worker_id)
            .count(),
        1,
        "the list holds the worker once"
    );
}
