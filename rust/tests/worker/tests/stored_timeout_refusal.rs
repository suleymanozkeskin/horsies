//! A stored timeout that a worker cannot use fails its task in one claim
//! (Python `tests/integration/test_stored_timeout_refusal.py`).
//!
//! A queued row can hold a `timeout_ms` above the bound that a worker runs:
//! an older version accepted any value of at least 1000. The claim sends the
//! row to a child with a refusal in its run frame; the child sets the row
//! RUNNING and fails it with `WORKER_SERIALIZATION_ERROR` before it resolves
//! the task. Each test stores the timeout before its worker starts, so the
//! first claim of the row reads it.

use std::time::Duration;

use horsies::{
    OutputlessResults, TaskInfoParts, TaskNode, TaskOutcome, TaskStatus, WorkflowSpecBuilder,
};
use horsies_test_support::rows::{self, TaskRow};
use horsies_test_support::tokio_postgres::Client;
use horsies_test_worker::harness::{outcome, succeeded, Suite};
use horsies_test_worker::instance::{Instance, Profile};
use horsies_test_worker::tasks::{basic, workflow};
use serial_test::serial;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for the end of a task or a workflow that a running
/// worker runs.
const RESULT_WAIT: Duration = Duration::from_secs(30);

/// Every part of the task info.
const ALL_PARTS: TaskInfoParts = TaskInfoParts {
    result: true,
    failed_reason: true,
    attempts: true,
};

/// The largest stored `timeout_ms` that a worker runs (core
/// `worker::retry::MAX_TASK_TIMEOUT_MS`, Python `MAX_TASK_TIMEOUT_MS`).
const TIMEOUT_BOUND_MS: u64 = 4_294_967_295;

/// The code of a run that the worker refuses.
const REFUSAL_CODE: &str = "WORKER_SERIALIZATION_ERROR";

/// The message of the refusal of a stored timeout above
/// [`TIMEOUT_BOUND_MS`] (core `worker::retry::timeout_refusal_message`).
fn above_bound_message() -> String {
    format!("stored timeout_ms is above the limit of {TIMEOUT_BOUND_MS} ms (about 49.7 days)")
}

/// Write `timeout_ms` into the stored options of task `task_id`, as an older
/// version could store it (Python `_store_timeout`). The task must be a
/// PENDING live row: no worker has claimed it.
async fn store_timeout(client: &Client, task_id: Uuid, timeout_ms: u64) {
    let stored: Option<String> = client
        .query_one(
            "SELECT task_options FROM horsies_tasks WHERE id = $1 AND status = 'PENDING'",
            &[&task_id],
        )
        .await
        .unwrap_or_else(|error| panic!("read the options of the PENDING task {task_id}: {error}"))
        .get(0);
    let mut options = match stored {
        Some(text) => match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(serde_json::Value::Object(options)) => options,
            Ok(other) => panic!("the options of task {task_id} are not an object: {other}"),
            Err(error) => panic!("the options of task {task_id} do not parse: {error}"),
        },
        None => serde_json::Map::new(),
    };
    let _previous = options.insert("timeout_ms".to_owned(), serde_json::Value::from(timeout_ms));
    let text = serde_json::Value::Object(options).to_string();
    let changed = client
        .execute(
            "UPDATE horsies_tasks SET task_options = $2 WHERE id = $1 AND status = 'PENDING'",
            &[&task_id, &text],
        )
        .await
        .unwrap_or_else(|error| panic!("store the timeout of task {task_id}: {error}"));
    assert_eq!(changed, 1, "task {task_id} is a PENDING live row");
}

/// Check that task `task_id` ended `FAILED` with the refusal in one claim:
/// one attempt, which does not retry. Returns its ended row.
async fn assert_refused_in_one_claim(suite: &Suite, client: &Client, task_id: Uuid) -> TaskRow {
    let info = suite
        .app
        .get_task_info(task_id, ALL_PARTS)
        .await
        .unwrap_or_else(|error| panic!("read the info of task {task_id}: {error}"));
    assert_eq!((info.status, info.retry_count), (TaskStatus::Failed, 0));
    let attempts = info.attempts.expect("attempts were asked for");
    let [attempt] = attempts.as_slice() else {
        panic!("one attempt, not {attempts:?}");
    };
    assert_eq!(attempt.attempt, 1);
    assert_eq!(attempt.outcome.to_string(), "FAILED");
    assert!(!attempt.will_retry);
    assert_eq!(attempt.error_code.as_deref(), Some(REFUSAL_CODE));
    let row = rows::read_task(client, task_id)
        .await
        .unwrap_or_else(|| panic!("task {task_id} has a row"));
    assert_eq!(row.status, "FAILED");
    assert_eq!(row.error_code.as_deref(), Some(REFUSAL_CODE));
    assert_eq!(row.retry_count, 0);
    row
}

/// Run a later task on the worker, so that the worker makes claim passes
/// after the refusal ended, then check that task `task_id` was not claimed
/// again: it has no live row, its ended row is `ended`, and it still has one
/// attempt.
async fn assert_not_claimed_again(suite: &Suite, client: &Client, task_id: Uuid, ended: &TaskRow) {
    let later = suite
        .function(basic::simple::handle)
        .send(basic::simple::Input { x: 1 })
        .await
        .expect("send the later task");
    assert_eq!(succeeded(&later, RESULT_WAIT).await, 2);
    let live = rows::count(
        client,
        "SELECT count(*) FROM horsies_tasks WHERE id = $1",
        &[&task_id],
    )
    .await;
    assert_eq!(live, 0, "task {task_id} has no live row to claim");
    assert_eq!(
        rows::read_task(client, task_id).await.as_ref(),
        Some(ended),
        "task {task_id} did not change after its end"
    );
    let attempts = suite
        .app
        .get_task_info(task_id, ALL_PARTS)
        .await
        .unwrap_or_else(|error| panic!("read the info of task {task_id}: {error}"))
        .attempts
        .expect("attempts were asked for");
    assert_eq!(
        attempts.len(),
        1,
        "task {task_id} has one attempt: {attempts:?}"
    );
}

/// A step node `id` that returns `completed_<id>`.
fn step(suite: &Suite, id: &str) -> TaskNode<String, workflow::step::Input> {
    suite
        .function(workflow::step::handle)
        .node()
        .node_id(id)
        .set(workflow::step::params::step(), id.to_owned())
        .expect("a step input")
}

/// Python: `test_stored_timeout_refusal.py::test_a_refused_timeout_fails_in_one_claim_and_is_not_claimed_again`.
/// The first claim fails the task with `WORKER_SERIALIZATION_ERROR` and the
/// message of the timeout refusal; its body does not run, and no later
/// claim takes it.
#[tokio::test]
#[serial]
async fn a_refused_timeout_fails_in_one_claim_and_is_not_claimed_again() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let client = suite.client().await;
    let token = Uuid::new_v4().simple().to_string();
    let handle = suite
        .function(basic::idempotent::handle)
        .send(basic::idempotent::Input {
            token: token.clone(),
        })
        .await
        .expect("send e2e_idempotent");
    store_timeout(&client, handle.task_id(), TIMEOUT_BOUND_MS + 1).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    match outcome(&handle, RESULT_WAIT).await {
        TaskOutcome::Failed(error) => {
            assert_eq!(error.code().text(), REFUSAL_CODE);
            assert_eq!(error.message(), Some(above_bound_message().as_str()));
        }
        TaskOutcome::Succeeded(value) => panic!("the task succeeded with {value:?}"),
    }
    assert!(
        suite.markers.read(&format!("idempotent-{token}")).is_none(),
        "the body of the refused task did not run"
    );
    let ended = assert_refused_in_one_claim(&suite, &client, handle.task_id()).await;
    assert!(!ended.is_workflow_task);
    assert_not_claimed_again(&suite, &client, handle.task_id(), &ended).await;
}

/// Python: `test_stored_timeout_refusal.py::test_a_refused_timeout_fails_its_workflow_node`.
/// The refused root node fails in one claim, its dependent is skipped and
/// the workflow fails; no later claim takes the node's task.
#[tokio::test]
#[serial]
async fn a_refused_timeout_fails_its_workflow_node() {
    let suite = Suite::start(BIN, Instance::Default).await;
    let client = suite.client().await;
    let mut spec = WorkflowSpecBuilder::new("stored_timeout_flow");
    let _key = spec.definition_key("tests.e2e.stored_timeout_flow.v1");
    let a = spec.task(step(&suite, "a"));
    let _b = spec.task(step(&suite, "b").waits_for(a));
    let spec = spec
        .build()
        .unwrap_or_else(|refused| panic!("build the spec: {refused}"));
    let handle = suite
        .app
        .start::<OutputlessResults>(&spec)
        .await
        .unwrap_or_else(|refused| panic!("start the workflow: {refused}"));
    let workflow_id = handle.workflow_id();
    let task_id = rows::workflow_node(&client, workflow_id, 0)
        .await
        .task_id
        .expect("the start mints the task of the root node");
    store_timeout(&client, task_id, TIMEOUT_BOUND_MS + 1).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let ended_status = rows::wait_for_workflow_end(&client, workflow_id, RESULT_WAIT).await;
    assert_eq!(ended_status, "FAILED", "the workflow status");
    let nodes = rows::workflow_nodes(&client, workflow_id).await;
    let statuses: Vec<&str> = nodes.iter().map(|node| node.status.as_str()).collect();
    assert_eq!(statuses, vec!["FAILED", "SKIPPED"]);
    assert_eq!(nodes[0].task_id, Some(task_id));
    assert!(
        nodes[0]
            .result
            .as_deref()
            .is_some_and(|result| result.contains(REFUSAL_CODE)),
        "node a stores the refusal: {:?}",
        nodes[0].result
    );
    let ended = assert_refused_in_one_claim(&suite, &client, task_id).await;
    assert!(ended.is_workflow_task);
    assert_not_claimed_again(&suite, &client, task_id, &ended).await;
    assert_eq!(
        rows::workflow_nodes(&client, workflow_id).await,
        nodes,
        "the nodes did not change after the end"
    );
}
