//! Layer 2: several workers, caps, crash recovery and soft-cap leases
//! (Python `clients/python/tests/e2e/test_layer2_cluster.py`).

use std::time::Duration;

use horsies::{TaskInfoParts, TaskStatus};
use horsies_test_support::process::TestProcess;
use horsies_test_support::tokio_postgres::Client;
use horsies_test_support::{poll, rows};
use horsies_test_worker::harness::{failed_code, succeeded, Suite};
use horsies_test_worker::instance::{Instance, Profile, CLUSTER_WIDE_CAP, CUSTOM_QUEUES};
use horsies_test_worker::tasks::{basic, custom, retry};
use serial_test::serial;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for the outcome of a task that a running worker runs.
const RESULT_WAIT: Duration = Duration::from_secs(30);

/// The worker id that no worker has: the owner of a claim whose worker died.
const DEAD_WORKER_ID: &str = "dead_worker_00000000";

/// The cap of a custom queue.
fn queue_cap(name: &str) -> i64 {
    CUSTOM_QUEUES
        .iter()
        .find(|(queue, _, _)| *queue == name)
        .map(|(_, _, cap)| i64::from(*cap))
        .unwrap_or_else(|| panic!("no custom queue {name}"))
}

/// The largest number of RUNNING tasks of `queue` during `duration`.
async fn most_running(client: &Client, queue: Option<&str>, duration: Duration) -> i64 {
    poll::max_during(duration, Duration::from_millis(50), async || match queue {
        Some(queue) => {
            rows::count(
                client,
                "SELECT count(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = $1",
                &[&queue],
            )
            .await
        }
        None => {
            rows::count(
                client,
                "SELECT count(*) FROM horsies_tasks WHERE status = 'RUNNING'",
                &[],
            )
            .await
        }
    })
    .await
}

/// Make `task_id` a claim of a dead worker, claimed 10 s ago, whose lease
/// ends at `lease_end` (an SQL interval from now).
async fn claim_for_dead_worker(client: &Client, task_id: Uuid, lease_end: &str) {
    let statement = format!(
        "UPDATE horsies_tasks SET status = 'CLAIMED', \
         attempt_claims = 1, first_claimed_at = NOW() - INTERVAL '10 seconds', claimed_at = NOW() - INTERVAL '10 seconds', claimed_by_worker_id = $2, \
         claim_expires_at = NOW() + INTERVAL '{lease_end}' WHERE id = $1"
    );
    let changed = client
        .execute(&statement, &[&task_id, &DEAD_WORKER_ID])
        .await
        .unwrap_or_else(|error| panic!("make task {task_id} a dead claim: {error}"));
    assert_eq!(changed, 1, "task {task_id} is a PENDING live row");
}

/// Start `count` workers of `profile`, each ready.
async fn workers(suite: &Suite, count: usize, profile: Profile) -> Vec<TestProcess> {
    let mut started = Vec::with_capacity(count);
    for _worker in 0..count {
        started.push(suite.worker(profile).await);
    }
    started
}

/// Python: `test_layer2_cluster.py::test_multi_worker_distribution`.
#[tokio::test]
#[serial]
async fn test_multi_worker_distribution() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _workers = workers(&suite, 2, Profile::Isolated1).await;
    let slow = suite.function(basic::slow::function);
    let mut handles = Vec::new();
    for _task in 0..10 {
        handles.push(
            slow.send(basic::slow::Input { duration_ms: 500 })
                .await
                .expect("send"),
        );
    }
    for handle in &handles {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
    let client = suite.client().await;
    let mut owners = std::collections::BTreeSet::new();
    for handle in &handles {
        let row = rows::read_task(&client, handle.task_id())
            .await
            .expect("a row");
        assert_eq!(row.status, "COMPLETED");
        let _new = owners.insert(row.worker_id.expect("the last owner"));
    }
    assert!(owners.len() >= 2, "one worker ran every task: {owners:?}");
}

/// Python: `test_layer2_cluster.py::test_multi_worker_no_double_execution`.
#[tokio::test]
#[serial]
async fn test_multi_worker_no_double_execution() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _workers = workers(&suite, 2, Profile::Isolated2).await;
    let idempotent = suite.function(basic::idempotent::function);
    let mut handles = Vec::new();
    for index in 0..20 {
        handles.push(
            idempotent
                .send(basic::idempotent::Input {
                    token: format!("multiworker_task_{index}"),
                })
                .await
                .expect("send"),
        );
    }
    for handle in &handles {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
    assert_eq!(suite.markers.count("idempotent-multiworker_task_"), 20);
}

/// Python: `test_layer2_cluster.py::test_stale_running_marked_failed_on_crash`.
#[tokio::test]
#[serial]
async fn test_stale_running_marked_failed_on_crash() {
    let suite = Suite::start(BIN, Instance::Recovery).await;
    let client = suite.client().await;
    let mut crashed = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::slow::function)
        .send(basic::slow::Input {
            duration_ms: 60_000,
        })
        .await
        .expect("send");
    rows::wait_for_task_status(&client, handle.task_id(), "RUNNING", RESULT_WAIT).await;
    crashed.kill_group();
    let _exit = crashed.wait_exit(Duration::from_secs(5)).await;
    let _recovering = suite.worker(Profile::Isolated1).await;
    assert_eq!(failed_code(&handle, RESULT_WAIT).await, "WORKER_CRASHED");
}

/// Python: `test_layer2_cluster.py::test_cluster_cap_not_leaked_on_failure`.
#[tokio::test]
#[serial]
async fn test_cluster_cap_not_leaked_on_failure() {
    let suite = Suite::start(BIN, Instance::ClusterCap).await;
    let _workers = workers(&suite, 2, Profile::Isolated2).await;
    let failing = suite.function(basic::no_retry::function);
    let mut failed = Vec::new();
    for _task in 0..4 {
        failed.push(failing.send(()).await.expect("send a failing task"));
    }
    for handle in &failed {
        assert_eq!(failed_code(handle, RESULT_WAIT).await, "PERMANENT");
    }
    let slow = suite.function(basic::slow::function);
    let mut completed = Vec::new();
    for _task in 0..4 {
        completed.push(
            slow.send(basic::slow::Input { duration_ms: 300 })
                .await
                .expect("send a slow task"),
        );
    }
    for handle in &completed {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
}

/// Python: `test_layer2_cluster.py::test_retry_works_across_multiple_workers`.
#[tokio::test]
#[serial]
async fn test_retry_works_across_multiple_workers() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _workers = workers(&suite, 2, Profile::Isolated2).await;
    let exhausted = suite.function(retry::retry_exhausted::function);
    let mut handles = Vec::new();
    for _task in 0..10 {
        handles.push(exhausted.send(()).await.expect("send"));
    }
    for handle in &handles {
        assert_eq!(
            failed_code(handle, Duration::from_secs(60)).await,
            "TRANSIENT"
        );
        let info = handle
            .info(TaskInfoParts::default())
            .await
            .expect("read the info");
        assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 3));
    }
}

/// Python: `test_layer2_cluster.py::test_concurrent_enqueue_during_processing`.
#[tokio::test]
#[serial]
async fn test_concurrent_enqueue_during_processing() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let _worker = suite.worker(Profile::Isolated2).await;
    let client = suite.client().await;
    let slow = suite.function(basic::slow::function);
    let mut handles = Vec::new();
    for _task in 0..10 {
        handles.push(
            slow.send(basic::slow::Input { duration_ms: 200 })
                .await
                .expect("send the first batch"),
        );
    }
    rows::wait_for_task_status(&client, handles[0].task_id(), "COMPLETED", RESULT_WAIT).await;
    for _task in 0..10 {
        handles.push(
            slow.send(basic::slow::Input { duration_ms: 200 })
                .await
                .expect("send during processing"),
        );
    }
    for handle in &handles {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
}

/// Python: `test_layer2_cluster.py::test_cluster_wide_cap`.
#[tokio::test]
#[serial]
async fn test_cluster_wide_cap() {
    let suite = Suite::start(BIN, Instance::ClusterCap).await;
    let _workers = workers(&suite, 2, Profile::Isolated2).await;
    let client = suite.client().await;
    let slow = suite.function(basic::slow::function);
    let mut handles = Vec::new();
    for _task in 0..6 {
        handles.push(
            slow.send(basic::slow::Input { duration_ms: 500 })
                .await
                .expect("send"),
        );
    }
    let running = most_running(&client, None, Duration::from_secs(4)).await;
    for handle in &handles {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
    assert!(
        running > 0,
        "no RUNNING task was seen: the check is vacuous"
    );
    assert!(
        running <= i64::from(CLUSTER_WIDE_CAP),
        "{running} tasks ran at once against a cap of {CLUSTER_WIDE_CAP}"
    );
}

/// Python: `test_layer2_cluster.py::test_stale_claimed_requeued_and_completed`.
#[tokio::test]
#[serial]
async fn test_stale_claimed_requeued_and_completed() {
    let suite = Suite::start(BIN, Instance::Recovery).await;
    let client = suite.client().await;
    let handle = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 5 })
        .await
        .expect("send");
    claim_for_dead_worker(&client, handle.task_id(), "5 minutes").await;
    let _worker = suite.worker(Profile::Isolated1).await;
    assert_eq!(succeeded(&handle, RESULT_WAIT).await, 10);
    let row = rows::read_task(&client, handle.task_id())
        .await
        .expect("a row");
    assert_ne!(row.worker_id.as_deref(), Some(DEAD_WORKER_ID));
}

/// Python: `test_layer2_cluster.py::test_single_worker_crash_remaining_continue`.
#[tokio::test]
#[serial]
async fn test_single_worker_crash_remaining_continue() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let client = suite.client().await;
    let started = workers(&suite, 3, Profile::Isolated2).await;
    let slow = suite.function(basic::slow::function);
    let mut handles = Vec::new();
    for _task in 0..20 {
        handles.push(
            slow.send(basic::slow::Input { duration_ms: 300 })
                .await
                .expect("send"),
        );
    }
    rows::wait_for_task_status(&client, handles[0].task_id(), "RUNNING", RESULT_WAIT).await;
    let mut started = started.into_iter();
    let mut killed = started.next().expect("three workers");
    killed.kill_group();
    let _exit = killed.wait_exit(Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    let ids: Vec<Uuid> = handles.iter().map(|handle| handle.task_id()).collect();
    let completed = rows::count(
        &client,
        "SELECT count(*) FROM horsies_task_history WHERE task_id = ANY($1) AND status = 'COMPLETED'",
        &[&ids],
    )
    .await;
    // The killed worker held at most its two running tasks.
    assert!(completed >= 18, "{completed} of 20 tasks completed");
    let owners = rows::count(
        &client,
        "SELECT count(DISTINCT last_claimed_worker_id) FROM horsies_task_history \
         WHERE task_id = ANY($1) AND status = 'COMPLETED'",
        &[&ids],
    )
    .await;
    assert!(
        owners >= 2,
        "the two live workers ran the tasks: {owners} owners"
    );
}

/// Python: `test_layer2_cluster.py::test_per_queue_concurrency_cap`.
#[tokio::test]
#[serial]
async fn test_per_queue_concurrency_cap() {
    let suite = Suite::start(BIN, Instance::CustomQueues).await;
    let _worker = suite.worker(Profile::Isolated8).await;
    let client = suite.client().await;
    let slow = suite.function(custom::slow_high::function);
    let mut handles = Vec::new();
    for _task in 0..10 {
        handles.push(
            slow.send(custom::slow_high::Input { duration_ms: 400 })
                .await
                .expect("send"),
        );
    }
    let running = most_running(&client, Some("high"), Duration::from_secs(5)).await;
    for handle in &handles {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
    assert!(
        running > 0,
        "no RUNNING task was seen: the check is vacuous"
    );
    assert!(
        running <= queue_cap("high"),
        "{running} tasks of queue high ran at once"
    );
}

/// Python: `test_layer2_cluster.py::test_per_queue_caps_independent_across_queues`.
#[tokio::test]
#[serial]
async fn test_per_queue_caps_independent() {
    let suite = Suite::start(BIN, Instance::CustomQueues).await;
    let _worker = suite.worker(Profile::Isolated8).await;
    let client = suite.client().await;
    let high = suite.function(custom::slow_high::function);
    let normal = suite.function(custom::slow_normal::function);
    let mut high_handles = Vec::new();
    let mut normal_handles = Vec::new();
    for _task in 0..10 {
        high_handles.push(
            high.send(custom::slow_high::Input { duration_ms: 400 })
                .await
                .expect("send high"),
        );
        normal_handles.push(
            normal
                .send(custom::slow_normal::Input { duration_ms: 400 })
                .await
                .expect("send normal"),
        );
    }
    let (most_high, most_normal) = tokio::join!(
        most_running(&client, Some("high"), Duration::from_secs(5)),
        most_running(&client, Some("normal"), Duration::from_secs(5)),
    );
    for handle in high_handles.iter().chain(&normal_handles) {
        let _value = succeeded(handle, RESULT_WAIT).await;
    }
    assert!(most_high > 0 && most_normal > 0, "both queues ran");
    assert!(most_high <= queue_cap("high"), "high: {most_high} at once");
    assert!(
        most_normal <= queue_cap("normal"),
        "normal: {most_normal} at once"
    );
}

/// Python: `test_layer2_cluster.py::test_queue_priority_ordering`.
#[tokio::test]
#[serial]
async fn test_queue_priority_ordering() {
    let suite = Suite::start(BIN, Instance::CustomQueues).await;
    let low = suite.function(custom::slow_low::function);
    let high = suite.function(custom::slow_high::function);
    let mut ids = Vec::new();
    for _task in 0..5 {
        ids.push(
            low.send(custom::slow_low::Input { duration_ms: 100 })
                .await
                .expect("send low")
                .task_id(),
        );
    }
    for _task in 0..5 {
        ids.push(
            high.send(custom::slow_high::Input { duration_ms: 100 })
                .await
                .expect("send high")
                .task_id(),
        );
    }
    let _worker = suite.worker(Profile::Isolated1).await;
    let client = suite.client().await;
    rows::wait_for_terminal(&client, &ids, RESULT_WAIT).await;
    let mut ended = Vec::new();
    for id in &ids {
        let row = rows::read_task(&client, *id).await.expect("a row");
        ended.push((row.terminal_at.expect("a terminal time"), row.queue_name));
    }
    ended.sort();
    let high_first = ended
        .iter()
        .take(5)
        .filter(|(_, queue)| queue == "high")
        .count();
    assert!(high_first >= 4, "completion order: {ended:?}");
}

/// Python: `test_layer2_cluster.py::test_softcap_lease_no_double_execution`.
#[tokio::test]
#[serial]
async fn test_softcap_lease_no_double_execution() {
    let suite = Suite::start(BIN, Instance::SoftCap).await;
    let _workers = workers(&suite, 2, Profile::Isolated1).await;
    let task = suite.function(basic::slow_idempotent::function);
    let mut handles = Vec::new();
    for index in 0..6 {
        handles.push(
            task.send(basic::slow_idempotent::Input {
                token: format!("softcap_task_{index}"),
                duration_ms: 1_500,
            })
            .await
            .expect("send"),
        );
    }
    for handle in &handles {
        let _value = succeeded(handle, Duration::from_secs(60)).await;
    }
    assert_eq!(suite.markers.count("idempotent-softcap_task_"), 6);
}

/// Python: `test_layer2_cluster.py::test_softcap_expired_claim_requeued`.
#[tokio::test]
#[serial]
async fn test_softcap_expired_claim_requeued() {
    let suite = Suite::start(BIN, Instance::SoftCap).await;
    let client = suite.client().await;
    let token = "softcap_expired_claim";
    let handle = suite
        .function(basic::slow_idempotent::function)
        .send(basic::slow_idempotent::Input {
            token: token.to_owned(),
            duration_ms: 100,
        })
        .await
        .expect("send");
    claim_for_dead_worker(&client, handle.task_id(), "-5 seconds").await;
    let _worker = suite.worker(Profile::Isolated1).await;
    assert_eq!(
        succeeded(&handle, RESULT_WAIT).await,
        format!("executed:{token}")
    );
    let row = rows::read_task(&client, handle.task_id())
        .await
        .expect("a row");
    assert!(row.worker_id.is_some(), "the final owner is recorded");
    assert_ne!(row.worker_id.as_deref(), Some(DEAD_WORKER_ID));
    assert_eq!(suite.markers.count("idempotent-"), 1);
}

/// Python: `test_layer2_cluster.py::test_softcap_db_ledger_race_single_execution`.
/// The ledger of each body run is its exclusive marker.
#[tokio::test]
#[serial]
async fn test_softcap_db_ledger_race_single_execution() {
    let suite = Suite::start(BIN, Instance::SoftCap).await;
    let _workers = workers(&suite, 2, Profile::Isolated1).await;
    let task = suite.function(basic::slow_idempotent::function);
    let mut handles = Vec::new();
    for index in 0..20 {
        handles.push(
            task.send(basic::slow_idempotent::Input {
                token: format!("ledger_{index}"),
                duration_ms: 50,
            })
            .await
            .expect("send"),
        );
    }
    for handle in &handles {
        let _value = succeeded(handle, Duration::from_secs(60)).await;
    }
    assert_eq!(suite.markers.count("idempotent-ledger_"), 20);
}

/// Python: `test_layer2_cluster.py::test_softcap_owner_transition_after_worker_crash`.
#[tokio::test]
#[serial]
async fn test_softcap_owner_transition_after_worker_crash() {
    let suite = Suite::start(BIN, Instance::SoftCap).await;
    let client = suite.client().await;
    let mut first = suite.worker(Profile::Isolated1).await;
    let blocker = suite
        .function(basic::slow::function)
        .send(basic::slow::Input { duration_ms: 6_000 })
        .await
        .expect("send the blocker");
    rows::wait_for_task_status(&client, blocker.task_id(), "RUNNING", RESULT_WAIT).await;
    let token = "softcap_owner_transition";
    let handle = suite
        .function(basic::slow_idempotent::function)
        .send(basic::slow_idempotent::Input {
            token: token.to_owned(),
            duration_ms: 100,
        })
        .await
        .expect("send");
    let first_owner = poll::poll_until(RESULT_WAIT, "the prefetched claim", async || {
        let row = rows::read_task(&client, handle.task_id()).await?;
        (row.status == "CLAIMED").then_some(row.worker_id).flatten()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    first.kill_group();
    let _exit = first.wait_exit(Duration::from_secs(5)).await;
    let _second = suite.worker(Profile::Isolated1).await;
    assert_eq!(
        succeeded(&handle, RESULT_WAIT).await,
        format!("executed:{token}")
    );
    let row = rows::read_task(&client, handle.task_id())
        .await
        .expect("a row");
    assert_ne!(row.worker_id, Some(first_owner));
    assert_eq!(suite.markers.count("idempotent-"), 1);
}
