//! The polling fallback of `WorkflowHandle::get` (Python
//! `TestListenerFallbackContract`, `clients/python/tests/integration/test_workflow_handle.py`):
//! the reading app's session URL refuses each connect, so the wait cannot
//! subscribe on `workflow_done` and reads the workflow row again every
//! second.
//!
//! Each test runs on a database of its own, a copy of the core's test
//! template (`core/src/test_support/database.rs`): the
//! installed schema, Rust family. A worker broker of the test covers the
//! history partitions and ends the workflow's node as a worker does
//! (Python `finish_task`, `clients/python/tests/integration/conftest.py`).

#![cfg(feature = "tokio-postgres")]

use std::convert::Infallible;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use horsies::{
    task, AppConfig, Horsies, SchemaMode, TaskError, TaskOutcome, WaitLimit, WorkflowHandle,
    WorkflowReadError, WorkflowStatus,
};
use horsies_core::core::lifecycle::{OwnedClaim, WorkflowSuccessOutcome};
use horsies_core::{
    on_workflow_task_complete, ChildSpecFuture, ChildSpecRequest, ChildSpecSource,
    ChildSpecUnavailable, CoverageOutcome, CoverageRequest, CoverageSerialization,
    FinishWorkflowSuccess, PostgresBroker, TaskResult, WorkflowTaskCompletion,
};
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
    let broker = PostgresBroker::connect(url, horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

/// The value that the workflow's one node ends with.
const ANSWER: u32 = 100;

/// The worker that the node's task runs on, and the process facts of the
/// start of its run: the attempt row of phase 1 needs them.
const WORKER: &str = "workflow-get-polling-worker";
const WORKER_PID: i32 = 4242;
const WORKER_HOSTNAME: &str = "workflow-get-polling-host";
const WORKER_PROCESS_NAME: &str = "workflow-get-polling-process";

/// The history leaves that the coverage pass creates ahead, in days and in
/// hours.
const COVERAGE_HISTORY_DAYS: u32 = 2;
const COVERAGE_HEARTBEAT_HOURS: u32 = 2;

/// The wait limit of a `get()` that the end of the workflow answers. The
/// end comes long before it.
const POLLED_WAIT_LIMIT: Duration = Duration::from_secs(10);

/// When the test ends the workflow after its `get()` started: after the
/// first poll (1 s, `WORKFLOW_RESULT_REPOLL_POLLING`), so the wait has read
/// the RUNNING row again by then.
const END_DURING_THE_WAIT_AFTER: Duration = Duration::from_millis(1_500);

/// A wait limit that passes while the wait polls a RUNNING workflow.
const SHORT_WAIT_LIMIT: Duration = Duration::from_millis(1_500);

/// The longest the test waits for the answer of a `get()`: more than each
/// wait limit here, so a wait that does not end fails the test.
const WAIT_ANSWER_BOUND: Duration = Duration::from_secs(15);

#[task("workflow_get_polling_answer")]
async fn answer() -> Result<u32, TaskError<Infallible>> {
    Ok(ANSWER)
}

/// A test database, and a worker broker on it. The broker ran one coverage
/// pass: it adds the default retention class (`standard_30d`, which a start
/// writes on each task) and the history leaves that phase 1 moves a task
/// into.
async fn covered_database() -> (TestDatabase, PostgresBroker) {
    let database = TestDatabase::create().await;
    let worker = PostgresBroker::connect(database.url(), horsies_core::ClientFamily::Rust)
        .await
        .expect("connect the worker broker");
    let request = CoverageRequest::new(COVERAGE_HISTORY_DAYS, COVERAGE_HEARTBEAT_HOURS, &[])
        .expect("a coverage request in bounds");
    let coverage = worker
        .ensure_partition_coverage(&request, CoverageSerialization::Gated)
        .await
        .expect("cover the history partitions");
    assert!(
        matches!(coverage, CoverageOutcome::Ensured(_)),
        "partition coverage of the test database: {coverage:?}"
    );
    (database, worker)
}

/// Starts a workflow of one node, the task `answer`, which is its output,
/// on an app of `url`. No worker runs the task: the workflow stays RUNNING.
async fn start_one_node_workflow(url: &str, name: &str) -> Uuid {
    let mut writer = Horsies::new(AppConfig::for_database_url(url)).expect("the writing app");
    let answer_function = answer::register(&mut writer).expect("register the task");
    let mut builder = writer.workflow_builder(name);
    builder.definition_key(format!("tests.workflow_get_polling.{name}.v1"));
    let output = builder
        .task(answer_function.node())
        .expect("the writing app registers answer");
    builder.output(output);
    let spec = builder.build().expect("the spec builds");
    let started: WorkflowHandle<u32> = writer.start(&spec).await.expect("the workflow starts");
    started.workflow_id()
}

/// The handle of `workflow_id` on an app that reads the database of `url`
/// and whose listener cannot connect. Its session URL is on a port where no
/// server listens, so the listener's connect is refused; the URL names a
/// database of its own, so the listener of the process for it is new. The
/// app runs no schema install (`ObserveOnly`): the install runs on the
/// session URL.
///
/// Checks that the workflow is RUNNING: that read opens the connection of
/// the app's core pool.
async fn listener_refused_handle(url: &str, workflow_id: Uuid) -> WorkflowHandle<u32> {
    let mut config = AppConfig::for_database_url(url);
    config.broker.session_database = horsies::SessionDatabase::Separate {
        url: format!(
            "postgresql://postgres:none@127.0.0.1:1/listener_refused_{}",
            Uuid::new_v4().simple()
        ),
    };
    let reader =
        Horsies::with_schema_mode(config, SchemaMode::ObserveOnly).expect("the reading app");
    let handle = reader.workflow_handle::<u32>(workflow_id);
    let status = handle.status().await;
    assert!(
        matches!(status, Ok(WorkflowStatus::Running)),
        "the status of workflow {workflow_id}: {status:?}"
    );
    handle
}

/// The child spec source of the phase 2 of these tests: their workflows have
/// no sub-workflow node, so no step asks it for a child.
struct NoChildren;

impl ChildSpecSource for NoChildren {
    fn child_spec(&self, request: ChildSpecRequest) -> ChildSpecFuture<'_> {
        Box::pin(async move {
            Err(ChildSpecUnavailable::Raised {
                subject: format!(
                    "build of the child of {}:{}",
                    request.parent_workflow_name, request.task_index
                ),
                cause: "the workflows of these tests have no sub-workflow node".to_owned(),
            })
        })
    }
}

/// Ends the task of node 0 of `workflow_id` with `ANSWER` as a worker does:
/// the task goes RUNNING under `WORKER` with the process facts of its start,
/// phase 1 moves it to history (`PostgresBroker::finish_workflow_success`),
/// and phase 2 finishes the node and completes the workflow
/// (`on_workflow_task_complete`).
async fn end_the_node(database: &TestDatabase, worker: &PostgresBroker, workflow_id: Uuid) {
    let client = database.client();
    let task_id: Option<Uuid> = client
        .query_one(
            "SELECT task_id FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 0",
            &[&workflow_id],
        )
        .await
        .expect("read node 0 of the workflow")
        .get(0);
    let task_id = task_id.expect("node 0 of the workflow has a task");
    let claimed_at: DateTime<Utc> = client
        .query_one(
            "UPDATE horsies_tasks
             SET status = 'RUNNING', attempt_claims = 1, first_claimed_at = NOW(), claimed_at = NOW(), claimed_by_worker_id = $2,
                 claim_expires_at = NULL, started_at = NOW(), worker_pid = $3,
                 worker_hostname = $4, worker_process_name = $5, updated_at = NOW()
             WHERE id = $1 AND status IN ('PENDING', 'CLAIMED')
             RETURNING claimed_at",
            &[
                &task_id,
                &WORKER,
                &WORKER_PID,
                &WORKER_HOSTNAME,
                &WORKER_PROCESS_NAME,
            ],
        )
        .await
        .expect("run the task of node 0")
        .get(0);
    let outcome = TaskResult::<serde_json::Value>::Ok(serde_json::json!(ANSWER));
    let result_json = serde_json::to_string(&outcome).expect("encode the result of the task");

    let phase_one = worker
        .finish_workflow_success(&FinishWorkflowSuccess {
            task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at,
            },
            result_json,
            placement: horsies_core::core::lifecycle::AttemptPlacement::NotObserved,
        })
        .await
        .expect("phase 1 of the task");
    assert!(
        matches!(phase_one, WorkflowSuccessOutcome::SuccessApplied { .. }),
        "phase 1 of task {task_id}: {phase_one:?}"
    );
    let phase_two = on_workflow_task_complete(worker, task_id, &NoChildren)
        .await
        .expect("phase 2 of the task");
    assert_eq!(phase_two, WorkflowTaskCompletion::Applied);
}

/// A wait on a RUNNING workflow whose listener connect is refused polls the
/// workflow row. The workflow ends during the wait, and a poll answers its
/// result.
#[tokio::test]
async fn get_polls_to_the_end_when_the_listener_connect_is_refused() {
    let (database, worker) = covered_database().await;
    let workflow_id = start_one_node_workflow(database.url(), "listen_refused_poll").await;
    let handle = listener_refused_handle(database.url(), workflow_id).await;
    let limit = WaitLimit::Within(POLLED_WAIT_LIMIT);

    let wait = handle.get(limit);
    tokio::pin!(wait);
    tokio::select! {
        early = &mut wait => panic!("the wait answered before the workflow ended: {early:?}"),
        () = tokio::time::sleep(END_DURING_THE_WAIT_AFTER) => {}
    }
    end_the_node(&database, &worker, workflow_id).await;
    let answered = tokio::time::timeout(WAIT_ANSWER_BOUND, wait)
        .await
        .expect("the wait answers within its bound");

    match answered {
        Ok(TaskOutcome::Succeeded(value)) => assert_eq!(value, ANSWER),
        other => panic!("expected the result {ANSWER}, got {other:?}"),
    }
    worker.close().await;
    database.drop().await;
}

/// A wait on a RUNNING workflow whose listener connect is refused polls the
/// workflow row, and the workflow does not end. At the wait limit `get()`
/// answers `WaitTimedOut`: it waited for the whole limit, and it did not
/// hang.
#[tokio::test]
async fn get_answers_wait_timed_out_when_the_limit_passes_while_polling() {
    let (database, worker) = covered_database().await;
    let workflow_id = start_one_node_workflow(database.url(), "listen_refused_timeout").await;
    let handle = listener_refused_handle(database.url(), workflow_id).await;
    let limit = WaitLimit::Within(SHORT_WAIT_LIMIT);

    let started = Instant::now();
    let answered = tokio::time::timeout(WAIT_ANSWER_BOUND, handle.get(limit))
        .await
        .expect("the wait answers within its bound");
    let waited = started.elapsed();

    match answered {
        Err(WorkflowReadError::WaitTimedOut {
            workflow_id: timed_out,
        }) => assert_eq!(timed_out, workflow_id),
        other => panic!("expected WaitTimedOut, got {other:?}"),
    }
    assert!(
        waited >= SHORT_WAIT_LIMIT,
        "the wait answered after {waited:?}, before its limit of {SHORT_WAIT_LIMIT:?}"
    );
    worker.close().await;
    database.drop().await;
}
