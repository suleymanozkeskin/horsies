//! The blocking views of the Rust client on PostgreSQL: each view method
//! runs its async method on the core's process runtime and returns its
//! answer (Python's sync calls: `send` and `schedule`, `retry_send`,
//! `TaskHandle.get` and `info`, the app's reads by task id and health
//! reads, `check(live=True)`, the workflow starts and `retry_start`, and the
//! sync methods of `WorkflowHandle`). One test per outcome of the database.
//!
//! The blocking calls run on an OS thread of their own that runs no tokio
//! runtime, as in a program without one ([`off_the_runtime`]). That thread
//! builds its own app on the test database, makes the calls and checks
//! their answers; only ids go back to the test.
//!
//! Each test runs on a database of its own, a copy of the core's test
//! template (`core/src/test_support/database.rs`): the
//! installed schema, Rust family. A worker broker of the test covers the
//! history partitions, and ends a task or a workflow node as a worker does
//! (Python `finish_task`, `clients/python/tests/integration/conftest.py`).

#![cfg(feature = "tokio-postgres")]

use std::convert::Infallible;
use std::fmt::Debug;
use std::time::Duration;

use chrono::{DateTime, Utc};
use horsies::{
    task, AppConfig, HandleOperationError, HandleResult, Horsies, HorsiesError, NodeKey,
    NodeResult, PingTarget, ReadError, TaskError, TaskFunction, TaskInfoParts, TaskOutcome,
    TaskSendErrorCode, TaskSendOptions, TaskStatus, WaitLimit, WholeSeconds, WorkerHistoryLimit,
    WorkflowDefConfig, WorkflowDefinition, WorkflowReadError, WorkflowSpec, WorkflowSpecBuilder,
    WorkflowStartErrorCode, WorkflowStatus,
};
use horsies_core::core::lifecycle::{OwnedClaim, TerminalizationOutcome, WorkflowSuccessOutcome};
use horsies_core::{
    on_workflow_task_complete, CancelAnswer, ChildSpecFuture, ChildSpecRequest, ChildSpecSource,
    ChildSpecUnavailable, CoverageOutcome, CoverageRequest, CoverageSerialization,
    FinishWorkflowSuccess, PlainSuccess, PostgresBroker, StreamCoverageState, SuccessBatch,
    TaskResult, WorkflowTaskCompletion,
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

/// The input of every send of the task `echo`, and the value that the
/// worker of the test stores as its result.
const SENT_VALUE: u32 = 7;

/// The stored input text of a send of `echo` with [`SENT_VALUE`].
const SENT_INPUT: &str = r#"{"value":7}"#;

/// The value that the one node of every workflow here ends with.
const ANSWER: u32 = 100;

/// The node id of the one node of every workflow here.
const ANSWER_NODE: &str = "answer";

/// The registered names of the two tasks.
const ECHO_TASK: &str = "blocking_views_echo";
const ANSWER_TASK: &str = "blocking_views_answer";

/// The tasks of the app of each thread.
const REGISTERED_TASKS: usize = 2;

/// The worker that runs a task of the test, and the process facts of the
/// start of its run: the attempt row of a finish needs them.
const WORKER: &str = "blocking-views-worker";
const WORKER_PID: i32 = 4343;
const WORKER_HOSTNAME: &str = "blocking-views-host";
const WORKER_PROCESS_NAME: &str = "blocking-views-process";

/// The history leaves that the coverage pass creates ahead, in days and in
/// hours.
const COVERAGE_HISTORY_DAYS: u32 = 2;
const COVERAGE_HEARTBEAT_HOURS: u32 = 2;

/// The wait limit of a read that times out: no task or workflow here ends
/// during it.
const SHORT_WAIT: Duration = Duration::from_millis(300);

/// The wait limit of a read of a task or a workflow that already ended, or
/// is PAUSED: the read answers at once, and the limit only bounds a read
/// that would not.
const ENDED_WAIT: Duration = Duration::from_secs(10);

/// The window of a worker ping with no worker on the database.
const PING_WINDOW: Duration = Duration::from_millis(300);

/// The delay of a delayed send (`TaskSendOptions::delay`).
const SCHEDULE_DELAY_SECS: u32 = 3_600;

/// A worker id that never reported a state.
const ABSENT_WORKER: &str = "blocking-views-absent-worker";

#[task("blocking_views_echo")]
async fn echo(value: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(value)
}

#[task("blocking_views_answer")]
async fn answer() -> Result<u32, TaskError<Infallible>> {
    Ok(ANSWER)
}

/// The params of each start of a workflow template here. The build does
/// not read them.
const TEMPLATE_PARAMS: u32 = 1;

/// One node, the task `answer` with the node id [`ANSWER_NODE`], which is
/// the output. The app of the builder registers `answer` before any build.
fn answer_output(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
    let output = builder.task(answer::node().node_id(ANSWER_NODE))?;
    Ok(WorkflowDefConfig::new().output(output))
}

/// A definition without params, for the views of a workflow function.
struct Nightly;

impl WorkflowDefinition for Nightly {
    type Output = u32;
    type Params = ();

    fn name() -> &'static str {
        "blocking_views_nightly"
    }

    fn definition_key() -> &'static str {
        "tests.blocking_views.nightly.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        answer_output(builder)
    }
}

/// A definition with params, for the views of a workflow template.
struct Regional;

impl WorkflowDefinition for Regional {
    type Output = u32;
    type Params = u32;

    fn name() -> &'static str {
        "blocking_views_regional"
    }

    fn definition_key() -> &'static str {
        "tests.blocking_views.regional.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        answer_output(builder)
    }
}

/// A definition without params for the start by type on the app.
struct ByTypeNightly;

impl WorkflowDefinition for ByTypeNightly {
    type Output = u32;
    type Params = ();

    fn name() -> &'static str {
        "blocking_views_by_type_nightly"
    }

    fn definition_key() -> &'static str {
        "tests.blocking_views.by_type_nightly.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        answer_output(builder)
    }
}

/// A definition with params for the start by type on the app.
struct ByTypeRegional;

impl WorkflowDefinition for ByTypeRegional {
    type Output = u32;
    type Params = u32;

    fn name() -> &'static str {
        "blocking_views_by_type_regional"
    }

    fn definition_key() -> &'static str {
        "tests.blocking_views.by_type_regional.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        answer_output(builder)
    }
}

/// Runs `call` on a new OS thread that runs no tokio runtime, as a program
/// without a runtime calls the blocking API, and returns its value. The
/// test's runtime keeps running while it waits. A panic of `call` fails the
/// test with its own payload.
async fn off_the_runtime<R, F>(call: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let joined = tokio::task::spawn_blocking(move || std::thread::spawn(call).join())
        .await
        .expect("the thread of the blocking calls is joined");
    match joined {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// An app on the database of `url` with the tasks `echo` and `answer`, and
/// the function of `echo`. No I/O.
fn app_on(url: &str) -> (Horsies, TaskFunction<echo::Input, u32, Infallible>) {
    let mut app = Horsies::new(AppConfig::for_database_url(url)).expect("the app");
    let echo_function = echo::register(&mut app).expect("register echo");
    let _answer_function = answer::register(&mut app).expect("register answer");
    (app, echo_function)
}

/// The input of every send of `echo`.
fn echo_input() -> echo::Input {
    echo::Input { value: SENT_VALUE }
}

/// A wait limit of `limit`.
fn wait_of(limit: Duration) -> WaitLimit {
    WaitLimit::Within(limit)
}

/// A thread runtime for a call that must run on a runtime thread: the
/// `ASYNC_CONTEXT` answer of a blocking call.
fn thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime of this thread")
}

/// The spec of a workflow `name` of one node, the task `answer`, which is
/// its output, built on a builder of `app`, which registers `answer`.
fn one_node_spec(app: &Horsies, name: &str) -> WorkflowSpec {
    one_node_spec_with_node_id(app, name, ANSWER_NODE)
}

/// [`one_node_spec`] whose node has the id `node_id`: with another id, a
/// start command of the same name that differs from [`one_node_spec`].
fn one_node_spec_with_node_id(app: &Horsies, name: &str, node_id: &str) -> WorkflowSpec {
    let mut builder = app.workflow_builder(name);
    builder.definition_key(format!("tests.blocking_views.{name}.v1"));
    let output = builder
        .task(answer::node().node_id(node_id))
        .expect("the app registers answer");
    builder.output(output);
    builder.build().expect("the spec builds")
}

/// Sends `echo` with a blocking send on an app of `url`. Returns the task
/// id.
fn send_echo(url: &str) -> Uuid {
    let (_app, echo_function) = app_on(url);
    echo_function
        .blocking()
        .send(echo_input())
        .expect("the blocking send")
        .task_id()
}

/// Starts a workflow of one node with a blocking start on an app of `url`.
/// No worker runs the node: the workflow stays RUNNING. Returns its id.
fn start_one_node_workflow(url: &str, name: &str) -> Uuid {
    let (app, _echo_function) = app_on(url);
    app.blocking()
        .start::<u32>(&one_node_spec(&app, name))
        .expect("the blocking start")
        .workflow_id()
}

/// A test database, and a worker broker on it. The broker ran one coverage
/// pass: it adds the default retention class (`standard_30d`, which a send
/// and a start write on each task) and the history leaves that a finish
/// moves a task into.
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

/// The task `task_id` goes RUNNING under [`WORKER`] with the process facts
/// of its start, as a worker starts it. Returns its claim generation.
async fn run_the_task(database: &TestDatabase, task_id: Uuid) -> DateTime<Utc> {
    database
        .client()
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
        .expect("run the task")
        .get(0)
}

/// The text of a stored success with `value`.
fn success_text(value: u32) -> String {
    serde_json::to_string(&TaskResult::<serde_json::Value>::Ok(serde_json::json!(
        value
    )))
    .expect("encode the result of the task")
}

/// Ends the plain task `task_id` with [`SENT_VALUE`] as a worker does: it
/// goes RUNNING, then the plain-success write moves it to history as
/// COMPLETED with its result.
async fn complete_the_task(database: &TestDatabase, worker: &PostgresBroker, task_id: Uuid) {
    let claimed_at = run_the_task(database, task_id).await;
    let batch = SuccessBatch::new(vec![PlainSuccess {
        task_id,
        claimed_at,
        result_json: success_text(SENT_VALUE),
        placement: horsies_core::core::lifecycle::AttemptPlacement::NotObserved,
    }])
    .expect("a batch of one row");
    let outcomes = worker
        .complete_successes(WORKER, &batch)
        .await
        .expect("the success write answers")
        .into_outcomes();
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { task_id: applied, .. }] if *applied == task_id
        ),
        "the success of task {task_id}: {outcomes:?}"
    );
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

/// Ends the task of node 0 of `workflow_id` with [`ANSWER`] as a worker
/// does: the task goes RUNNING, phase 1 moves it to history
/// (`PostgresBroker::finish_workflow_success`), and phase 2 finishes the
/// node and completes the workflow (`on_workflow_task_complete`).
async fn end_the_node(database: &TestDatabase, worker: &PostgresBroker, workflow_id: Uuid) {
    let task_id: Option<Uuid> = database
        .client()
        .query_one(
            "SELECT task_id FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 0",
            &[&workflow_id],
        )
        .await
        .expect("read node 0 of the workflow")
        .get(0);
    let task_id = task_id.expect("node 0 of the workflow has a task");
    let claimed_at = run_the_task(database, task_id).await;
    let phase_one = worker
        .finish_workflow_success(&FinishWorkflowSuccess {
            task_id,
            fence: OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at,
            },
            result_json: success_text(ANSWER),
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

/// The node ids of the workflow `workflow_id`, in node index order.
async fn node_ids(database: &TestDatabase, workflow_id: Uuid) -> Vec<String> {
    database
        .client()
        .query(
            "SELECT node_id FROM horsies_workflow_tasks WHERE workflow_id = $1 ORDER BY task_index",
            &[&workflow_id],
        )
        .await
        .expect("read the node ids")
        .iter()
        .map(|row| -> String { row.get(0) })
        .collect()
}

/// The id, name and status of each workflow of the database, by name.
async fn workflow_rows(database: &TestDatabase) -> Vec<(Uuid, String, String)> {
    database
        .client()
        .query(
            "SELECT id, name, status FROM horsies_workflows ORDER BY name",
            &[],
        )
        .await
        .expect("read the workflows")
        .iter()
        .map(|row| -> (Uuid, String, String) { (row.get(0), row.get(1), row.get(2)) })
        .collect()
}

/// `read` is `TASK_NOT_FOUND` of `task_id`.
fn assert_task_not_found<V: Debug>(read: Result<V, ReadError>, task_id: Uuid) {
    match read {
        Err(ReadError::TaskNotFound {
            task_id: missing, ..
        }) => assert_eq!(missing, task_id),
        other => panic!("expected TASK_NOT_FOUND of task {task_id}, got {other:?}"),
    }
}

/// `answer` is `WORKFLOW_NOT_FOUND` of `workflow_id`.
fn assert_workflow_not_found<V: Debug>(answer: HandleResult<V>, workflow_id: Uuid) {
    match answer {
        Err(HandleOperationError::WorkflowNotFound {
            workflow_id: missing,
            ..
        }) => assert_eq!(missing, workflow_id),
        other => panic!("expected WORKFLOW_NOT_FOUND of workflow {workflow_id}, got {other:?}"),
    }
}

/// A send now and a send with a delay write PENDING tasks with the sent
/// input; the delayed one is due after its delay. `retry_send` of a send
/// refused on a runtime thread writes the task id of the refused send.
#[tokio::test]
async fn blocking_sends_write_their_tasks() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    let (sent, delayed, retried) = off_the_runtime(move || {
        let (_app, echo_function) = app_on(&url);
        let sent = echo_function
            .blocking()
            .send(echo_input())
            .expect("the blocking send")
            .task_id();
        let delayed = TaskSendOptions::new().delay(WholeSeconds::from_secs(SCHEDULE_DELAY_SECS));
        let delayed_id = echo_function
            .blocking()
            .with_options(delayed)
            .send(echo_input())
            .expect("the blocking send with a delay")
            .task_id();
        let refused = thread_runtime()
            .block_on(async { echo_function.blocking().send(echo_input()) })
            .expect_err("a blocking send on a runtime thread is refused");
        assert_eq!(refused.code(), TaskSendErrorCode::AsyncContext, "{refused}");
        let refused_id = refused
            .task_id()
            .expect("the refused send keeps its task id");
        let retried = echo_function
            .blocking()
            .retry_send(&refused)
            .expect("the blocking retry of the refused send")
            .task_id();
        assert_eq!(retried, refused_id);
        (sent, delayed_id, retried)
    })
    .await;

    for (task_id, due_later) in [(sent, false), (delayed, true), (retried, false)] {
        let row = database
            .client()
            .query_one(
                "SELECT status, task_name, input, enqueued_at > NOW() FROM horsies_tasks
                 WHERE id = $1",
                &[&task_id],
            )
            .await
            .expect("read the sent task");
        let status: String = row.get(0);
        let task_name: String = row.get(1);
        let input: String = row.get(2);
        let due_after_now: bool = row.get(3);
        assert_eq!(status, "PENDING", "task {task_id}");
        assert_eq!(task_name, ECHO_TASK, "task {task_id}");
        assert_eq!(input, SENT_INPUT, "task {task_id}");
        assert_eq!(due_after_now, due_later, "task {task_id}");
    }
    worker.close().await;
    database.drop().await;
}

/// A COMPLETED task: the handle's `get`, the app's `get_result`,
/// `get_raw_result` and both info reads answer its stored result.
#[tokio::test]
async fn blocking_reads_of_a_completed_task_answer_its_result() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    let task_id = off_the_runtime({
        let url = url.clone();
        move || send_echo(&url)
    })
    .await;
    complete_the_task(&database, &worker, task_id).await;

    off_the_runtime(move || {
        let (app, _echo_function) = app_on(&url);
        let handle = app.task_handle_of::<u32, Infallible>(task_id);
        let with_result = TaskInfoParts {
            result: true,
            ..TaskInfoParts::default()
        };

        match handle.blocking().get(WaitLimit::NoLimit) {
            Ok(TaskOutcome::Succeeded(value)) => assert_eq!(value, SENT_VALUE),
            other => panic!("expected the result {SENT_VALUE}, got {other:?}"),
        }
        match app
            .blocking()
            .get_result::<u32, Infallible>(task_id, WaitLimit::NoLimit)
        {
            Ok(TaskOutcome::Succeeded(value)) => assert_eq!(value, SENT_VALUE),
            other => panic!("expected the result {SENT_VALUE}, got {other:?}"),
        }
        let stored = app
            .blocking()
            .get_raw_result(task_id, WaitLimit::NoLimit)
            .expect("the stored result");
        assert_eq!(stored.task_id, task_id);
        assert_eq!(stored.task_name, ECHO_TASK);
        assert_eq!(stored.status, TaskStatus::Completed);
        let envelope: serde_json::Value =
            serde_json::from_str(&stored.envelope_json).expect("the envelope is JSON");
        assert_eq!(envelope["ok"], serde_json::json!(SENT_VALUE), "{envelope}");

        for info in [
            handle.blocking().info(with_result),
            app.blocking().get_task_info(task_id, with_result),
        ] {
            let info = info.expect("the info of the completed task");
            assert_eq!(info.task_id, task_id);
            assert_eq!(info.status, TaskStatus::Completed);
            match &info.result {
                Some(TaskResult::Ok(text)) => assert_eq!(text.get(), SENT_VALUE.to_string()),
                other => panic!("expected the stored value {SENT_VALUE}, got {other:?}"),
            }
        }
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// A PENDING task: a bounded `get`, `get_result` and `get_raw_result` time
/// out with the status PENDING; both info reads answer PENDING; the class
/// reads see its stream, which no live worker serves.
#[tokio::test]
async fn blocking_reads_of_a_pending_task_time_out_and_report_it_pending() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    off_the_runtime(move || {
        let (app, echo_function) = app_on(&url);
        let handle = echo_function
            .blocking()
            .send(echo_input())
            .expect("the blocking send");
        let task_id = handle.task_id();
        let timed_out = |read: Result<(), ReadError>| match read {
            Err(ReadError::WaitTimedOut {
                task_id: waited,
                status: TaskStatus::Pending,
            }) => assert_eq!(waited, task_id),
            other => panic!("expected WAIT_TIMEOUT while PENDING, got {other:?}"),
        };

        timed_out(handle.blocking().get(wait_of(SHORT_WAIT)).map(|_| ()));
        timed_out(
            app.blocking()
                .get_result::<u32, Infallible>(task_id, wait_of(SHORT_WAIT))
                .map(|_| ()),
        );
        timed_out(
            app.blocking()
                .get_raw_result(task_id, wait_of(SHORT_WAIT))
                .map(|_| ()),
        );
        let info = handle
            .blocking()
            .info(TaskInfoParts::default())
            .expect("the info of the pending task");
        assert_eq!(info.status, TaskStatus::Pending);
        let app_info = app
            .blocking()
            .get_task_info(task_id, TaskInfoParts::default())
            .expect("the info of the pending task");
        assert_eq!(app_info.task_id, task_id);
        assert_eq!(app_info.status, TaskStatus::Pending);

        let coverage = app
            .blocking()
            .get_class_coverage()
            .expect("the class coverage");
        assert_eq!(coverage.live_workers, 0);
        match coverage.streams.as_slice() {
            [stream] => {
                assert_eq!(stream.queue_name, info.queue_name);
                assert_eq!(stream.state, StreamCoverageState::Unserved);
            }
            other => panic!("expected the one pending stream, got {other:?}"),
        }
        let report = app.blocking().get_class_report().expect("the class report");
        assert!(report.workers.is_empty(), "{report:?}");
        match report.streams.as_slice() {
            [stream] => {
                assert_eq!(stream.queue_name, info.queue_name);
                assert_eq!(stream.state, StreamCoverageState::Unserved);
            }
            other => panic!("expected the one pending stream, got {other:?}"),
        }
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// A task id that no row holds: every read is `TASK_NOT_FOUND`.
#[tokio::test]
async fn blocking_reads_of_an_unknown_task_are_task_not_found() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    off_the_runtime(move || {
        let (app, _echo_function) = app_on(&url);
        let task_id = Uuid::new_v4();
        let handle = app.task_handle_of::<u32, Infallible>(task_id);

        assert_task_not_found(handle.blocking().get(wait_of(SHORT_WAIT)), task_id);
        assert_task_not_found(handle.blocking().info(TaskInfoParts::default()), task_id);
        assert_task_not_found(
            app.blocking()
                .get_result::<u32, Infallible>(task_id, wait_of(SHORT_WAIT)),
            task_id,
        );
        assert_task_not_found(
            app.blocking().get_raw_result(task_id, wait_of(SHORT_WAIT)),
            task_id,
        );
        assert_task_not_found(
            app.blocking()
                .get_task_info(task_id, TaskInfoParts::default()),
            task_id,
        );
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// A task cancelled before it ran: `get` answers `TASK_CANCELLED`, the raw
/// read `RESULT_NOT_AVAILABLE` (the cancel stores no result), and the info
/// CANCELLED with its error code.
#[tokio::test]
async fn blocking_reads_of_a_cancelled_task_answer_task_cancelled() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    let task_id = off_the_runtime({
        let url = url.clone();
        move || send_echo(&url)
    })
    .await;
    let cancelled = worker
        .cancel_task(task_id, false)
        .await
        .expect("the cancel answers");
    assert!(
        matches!(
            cancelled,
            CancelAnswer::Cancelled {
                was_status: TaskStatus::Pending
            }
        ),
        "{cancelled:?}"
    );

    off_the_runtime(move || {
        let (app, _echo_function) = app_on(&url);
        match app
            .task_handle_of::<u32, Infallible>(task_id)
            .blocking()
            .get(wait_of(ENDED_WAIT))
        {
            Ok(TaskOutcome::Failed(error)) => assert_eq!(error.code().text(), "TASK_CANCELLED"),
            other => panic!("expected TASK_CANCELLED, got {other:?}"),
        }
        match app.blocking().get_raw_result(task_id, wait_of(ENDED_WAIT)) {
            Err(ReadError::ResultNotAvailable {
                task_id: read,
                status: TaskStatus::Cancelled,
            }) => assert_eq!(read, task_id),
            other => panic!("expected RESULT_NOT_AVAILABLE while CANCELLED, got {other:?}"),
        }
        let info = app
            .blocking()
            .get_task_info(task_id, TaskInfoParts::default())
            .expect("the info of the cancelled task");
        assert_eq!(info.status, TaskStatus::Cancelled);
        assert_eq!(info.error_code.as_deref(), Some("TASK_CANCELLED"));
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// The health reads on a database with no worker: the ping answers, no
/// worker state is held, no worker answers a ping, and the live check
/// passes.
#[tokio::test]
async fn blocking_health_reads_and_the_live_check_answer_from_the_database() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    off_the_runtime(move || {
        let (app, _echo_function) = app_on(&url);
        let ping = app.blocking().ping_database().expect("the database ping");
        assert!(ping.latency_ms >= 0.0, "{ping:?}");
        let states = app
            .blocking()
            .list_worker_states()
            .expect("the worker states");
        assert!(states.is_empty(), "{states:?}");
        let state = app
            .blocking()
            .get_worker_state(ABSENT_WORKER)
            .expect("the state of a worker");
        assert!(state.is_none(), "{state:?}");
        let history = app
            .blocking()
            .get_worker_state_history(ABSENT_WORKER, WorkerHistoryLimit::AllRetained)
            .expect("the state history of a worker");
        assert!(history.is_empty(), "{history:?}");
        let pongs = app
            .blocking()
            .ping_workers(PingTarget::EveryWorker, PING_WINDOW, None)
            .expect("the worker ping");
        assert!(pongs.is_empty(), "{pongs:?}");
        let passed = app.blocking().check_live().expect("the live check passes");
        assert_eq!(passed.task_count(), REGISTERED_TASKS);
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// `Horsies::start` and `start_with_id`: a start writes a RUNNING workflow;
/// a start again with its id and its start command answers the same
/// workflow and writes nothing; a start with its id and another name is
/// `WORKFLOW_ID_CONFLICT`, and one with its id, its name and another start
/// command is `WORKFLOW_COMMAND_CONFLICT`; neither writes.
#[tokio::test]
async fn blocking_app_starts_write_find_or_refuse_by_the_workflow_id() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    let started = off_the_runtime(move || {
        let (app, _echo_function) = app_on(&url);
        let spec = one_node_spec(&app, "blocking_views_app_start");
        let started = app
            .blocking()
            .start::<u32>(&spec)
            .expect("the blocking start")
            .workflow_id();
        let found = app
            .blocking()
            .start_with_id::<u32>(&spec, started)
            .expect("a start again with the same id and name")
            .workflow_id();
        assert_eq!(found, started);
        let other_name = one_node_spec(&app, "blocking_views_other_name");
        match app.blocking().start_with_id::<u32>(&other_name, started) {
            Err(refused) => {
                assert_eq!(
                    refused.code(),
                    WorkflowStartErrorCode::WorkflowIdConflict,
                    "{refused}"
                );
                assert_eq!(refused.workflow_id(), started);
            }
            Ok(handle) => panic!(
                "the start with a taken id answered workflow {}",
                handle.workflow_id()
            ),
        }
        let other_command =
            one_node_spec_with_node_id(&app, "blocking_views_app_start", "other_node");
        match app.blocking().start_with_id::<u32>(&other_command, started) {
            Err(refused) => {
                assert_eq!(
                    refused.code(),
                    WorkflowStartErrorCode::WorkflowCommandConflict,
                    "{refused}"
                );
                assert_eq!(refused.workflow_id(), started);
                assert!(!refused.retryable());
            }
            Ok(handle) => panic!(
                "the start with another command answered workflow {}",
                handle.workflow_id()
            ),
        }
        started
    })
    .await;

    assert_eq!(
        workflow_rows(&database).await,
        vec![(
            started,
            "blocking_views_app_start".to_owned(),
            "RUNNING".to_owned()
        )]
    );
    assert_eq!(
        node_ids(&database, started).await,
        vec![ANSWER_NODE.to_owned()]
    );
    worker.close().await;
    database.drop().await;
}

/// Two concurrent starts with one id, one name and two start commands: one
/// start writes the workflow, and the other waits for its commit on the id
/// and is `WORKFLOW_COMMAND_CONFLICT`. The workflow holds the node of the
/// winner only.
#[tokio::test]
async fn two_concurrent_starts_with_one_id_and_two_commands_write_one_workflow() {
    let (database, worker) = covered_database().await;
    let (app, _echo_function) = app_on(database.url());
    let workflow_id = Uuid::new_v4();
    let first = one_node_spec(&app, "start_once_race");
    let second = one_node_spec_with_node_id(&app, "start_once_race", "other_node");

    let (first_start, second_start) = tokio::join!(
        app.start_with_id::<u32>(&first, workflow_id),
        app.start_with_id::<u32>(&second, workflow_id),
    );

    let winner_node = match (first_start, second_start) {
        (Ok(_), Err(refused)) | (Err(refused), Ok(_))
            if refused.code() == WorkflowStartErrorCode::WorkflowCommandConflict =>
        {
            assert_eq!(refused.workflow_id(), workflow_id);
            node_ids(&database, workflow_id).await
        }
        (first_start, second_start) => panic!(
            "one start wins and the other is a command conflict, got {:?} and {:?}",
            first_start.map(|handle| handle.workflow_id()),
            second_start.map(|handle| handle.workflow_id()),
        ),
    };
    assert!(
        winner_node == vec![ANSWER_NODE.to_owned()] || winner_node == vec!["other_node".to_owned()],
        "{winner_node:?}"
    );
    assert_eq!(workflow_rows(&database).await.len(), 1);
    worker.close().await;
    database.drop().await;
}

/// A top-level run with no stored start command (an earlier horsies version
/// started it; here a direct insert) is returned to a start with its id and
/// its name, with no compare, and nothing is written.
#[tokio::test]
async fn a_run_with_no_stored_start_command_is_returned_to_a_start_with_its_id() {
    let (database, worker) = covered_database().await;
    let (app, _echo_function) = app_on(database.url());
    let workflow_id = Uuid::new_v4();
    database
        .client()
        .execute(
            "INSERT INTO horsies_workflows (
                 id, name, definition_key, status, on_error, depth, root_workflow_id, sent_at,
                 created_at, started_at, updated_at
             ) VALUES ($1, 'start_once_unrecorded', NULL, 'RUNNING', 'fail', 0, $1, NOW(), NOW(),
                       NOW(), NOW())",
            &[&workflow_id],
        )
        .await
        .expect("insert a run with no stored start command");

    let found = app
        .start_with_id::<u32>(&one_node_spec(&app, "start_once_unrecorded"), workflow_id)
        .await
        .expect("the start returns the run")
        .workflow_id();

    assert_eq!(found, workflow_id);
    assert!(node_ids(&database, workflow_id).await.is_empty());
    worker.close().await;
    database.drop().await;
}

/// The starts of a workflow function and of a workflow template: `start`
/// writes a RUNNING workflow; the function's `start_with_id` again with
/// its id answers the same workflow; the template's `start_with_id` writes
/// the workflow with the given id.
#[tokio::test]
async fn blocking_function_and_template_starts_write_their_workflows() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    let chosen = Uuid::new_v4();
    let (nightly, regional) = off_the_runtime(move || {
        let (mut app, _echo_function) = app_on(&url);
        let function = app
            .register_workflow_definition::<Nightly>()
            .expect("register the definition");
        let nightly = function
            .blocking()
            .start()
            .expect("the function start")
            .workflow_id();
        let found = function
            .blocking()
            .start_with_id(nightly)
            .expect("a start again with the same id")
            .workflow_id();
        assert_eq!(found, nightly);
        let template = app
            .workflow_template::<Regional>()
            .expect("register the template");
        let regional = template
            .blocking()
            .start(TEMPLATE_PARAMS)
            .expect("the template start")
            .workflow_id();
        let with_id = template
            .blocking()
            .start_with_id(TEMPLATE_PARAMS, chosen)
            .expect("the template start with an id")
            .workflow_id();
        assert_eq!(with_id, chosen);
        (nightly, regional)
    })
    .await;

    let mut expected = vec![
        (nightly, Nightly::name().to_owned(), "RUNNING".to_owned()),
        (regional, Regional::name().to_owned(), "RUNNING".to_owned()),
        (chosen, Regional::name().to_owned(), "RUNNING".to_owned()),
    ];
    let mut rows = workflow_rows(&database).await;
    expected.sort();
    rows.sort();
    assert_eq!(rows, expected);
    worker.close().await;
    database.drop().await;
}

/// The blocking `start_workflow` and `start_workflow_with` of the app view:
/// each starts its definition through its registration on the app, on the
/// database of that app.
#[tokio::test]
async fn blocking_starts_by_type_write_their_workflows() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    let (nightly, regional) = off_the_runtime(move || {
        let (mut app, _echo_function) = app_on(&url);
        let _function = app
            .register_workflow_definition::<ByTypeNightly>()
            .expect("register the definition");
        let _template = app
            .workflow_template::<ByTypeRegional>()
            .expect("register the template");
        let nightly = app
            .blocking()
            .start_workflow::<ByTypeNightly>()
            .expect("the start by type")
            .workflow_id();
        let regional = app
            .blocking()
            .start_workflow_with::<ByTypeRegional>(TEMPLATE_PARAMS)
            .expect("the start by type with params")
            .workflow_id();
        (nightly, regional)
    })
    .await;

    let mut expected = vec![
        (
            nightly,
            ByTypeNightly::name().to_owned(),
            "RUNNING".to_owned(),
        ),
        (
            regional,
            ByTypeRegional::name().to_owned(),
            "RUNNING".to_owned(),
        ),
    ];
    let mut rows = workflow_rows(&database).await;
    expected.sort();
    rows.sort();
    assert_eq!(rows, expected);
    worker.close().await;
    database.drop().await;
}

/// `retry_start`: a start on a runtime thread answers `ASYNC_CONTEXT` with
/// its workflow id and writes nothing; the retry of that error on a thread
/// without a runtime writes the workflow with that id.
#[tokio::test]
async fn a_blocking_retry_start_writes_the_refused_start_with_its_workflow_id() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    let refused_id = off_the_runtime(move || {
        let (mut app, _echo_function) = app_on(&url);
        let function = app
            .register_workflow_definition::<Nightly>()
            .expect("register the definition");
        let refused = thread_runtime()
            .block_on(async { function.blocking().start() })
            .expect_err("a blocking start on a runtime thread is refused");
        assert_eq!(
            refused.code(),
            WorkflowStartErrorCode::AsyncContext,
            "{refused}"
        );
        let started = function
            .blocking()
            .retry_start(&refused)
            .expect("the blocking retry of the refused start")
            .workflow_id();
        assert_eq!(started, refused.workflow_id());
        started
    })
    .await;

    assert_eq!(
        workflow_rows(&database).await,
        vec![(refused_id, Nightly::name().to_owned(), "RUNNING".to_owned())]
    );
    worker.close().await;
    database.drop().await;
}

/// The handle of a RUNNING workflow whose node did not run: `status` is
/// RUNNING, `tasks` holds the node without a result, `results` is empty,
/// `result_for` is `RESULT_NOT_READY`, and a bounded `get` times out.
#[tokio::test]
async fn blocking_handle_reads_of_a_running_workflow() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    off_the_runtime(move || {
        let (app, _echo_function) = app_on(&url);
        let handle = app
            .blocking()
            .start::<u32>(&one_node_spec(&app, "blocking_views_running"))
            .expect("the blocking start");
        let workflow_id = handle.workflow_id();
        let view = handle.blocking();

        let status = view.status();
        assert!(matches!(status, Ok(WorkflowStatus::Running)), "{status:?}");
        let nodes = view.tasks().expect("the nodes of the workflow");
        match nodes.as_slice() {
            [node] => {
                assert_eq!(node.node_id.as_deref(), Some(ANSWER_NODE));
                assert_eq!(node.index, 0);
                assert_eq!(node.name, ANSWER_TASK);
                assert!(node.result.is_none(), "{node:?}");
            }
            other => panic!("expected the one node, got {other:?}"),
        }
        let results = view.results().expect("the node results");
        assert!(results.is_empty(), "{results:?}");
        match view.result_for::<u32, Infallible>(&NodeKey::new(ANSWER_NODE)) {
            Err(WorkflowReadError::ResultNotReady {
                workflow_id: read,
                node_id,
            }) => {
                assert_eq!(read, workflow_id);
                assert_eq!(node_id, ANSWER_NODE);
            }
            other => panic!("expected RESULT_NOT_READY, got {other:?}"),
        }
        match view.get(wait_of(SHORT_WAIT)) {
            Err(WorkflowReadError::WaitTimedOut {
                workflow_id: waited,
            }) => assert_eq!(waited, workflow_id),
            other => panic!("expected WAIT_TIMEOUT, got {other:?}"),
        }
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// `pause` and `resume`: a pause of a RUNNING workflow answers `true` and
/// a second pause `false`; `get` of the PAUSED workflow is
/// `WORKFLOW_PAUSED`; a resume answers `true`, the workflow is RUNNING
/// again, and a second resume answers `false`.
#[tokio::test]
async fn blocking_pause_and_resume_move_the_workflow_between_running_and_paused() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    off_the_runtime(move || {
        let workflow_id = start_one_node_workflow(&url, "blocking_views_paused");
        let (app, _echo_function) = app_on(&url);
        let handle = app.workflow_handle::<u32>(workflow_id);
        let view = handle.blocking();

        let paused = view.pause();
        assert!(matches!(paused, Ok(true)), "{paused:?}");
        let status = view.status();
        assert!(matches!(status, Ok(WorkflowStatus::Paused)), "{status:?}");
        let paused_again = view.pause();
        assert!(matches!(paused_again, Ok(false)), "{paused_again:?}");
        match view.get(wait_of(ENDED_WAIT)) {
            Ok(TaskOutcome::Failed(error)) => assert_eq!(error.code().text(), "WORKFLOW_PAUSED"),
            other => panic!("expected WORKFLOW_PAUSED, got {other:?}"),
        }
        let resumed = view.resume();
        assert!(matches!(resumed, Ok(true)), "{resumed:?}");
        let status = view.status();
        assert!(matches!(status, Ok(WorkflowStatus::Running)), "{status:?}");
        let resumed_again = view.resume();
        assert!(matches!(resumed_again, Ok(false)), "{resumed_again:?}");
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// `cancel`: the workflow is CANCELLED and `get` is `WORKFLOW_CANCELLED`; a
/// pause of it answers `false`, and a cancel again leaves it as it is.
#[tokio::test]
async fn a_blocking_cancel_ends_the_workflow_as_cancelled() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    off_the_runtime(move || {
        let workflow_id = start_one_node_workflow(&url, "blocking_views_cancelled");
        let (app, _echo_function) = app_on(&url);
        let handle = app.workflow_handle::<u32>(workflow_id);
        let view = handle.blocking();

        view.cancel().expect("the blocking cancel");
        let status = view.status();
        assert!(
            matches!(status, Ok(WorkflowStatus::Cancelled)),
            "{status:?}"
        );
        match view.get(wait_of(ENDED_WAIT)) {
            Ok(TaskOutcome::Failed(error)) => {
                assert_eq!(error.code().text(), "WORKFLOW_CANCELLED");
            }
            other => panic!("expected WORKFLOW_CANCELLED, got {other:?}"),
        }
        let paused = view.pause();
        assert!(matches!(paused, Ok(false)), "{paused:?}");
        view.cancel().expect("a cancel of an ended workflow");
        let status = view.status();
        assert!(
            matches!(status, Ok(WorkflowStatus::Cancelled)),
            "{status:?}"
        );
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// A COMPLETED workflow: `status` is COMPLETED, `get` and `result_for`
/// answer the node's value, and `results` holds its stored text.
#[tokio::test]
async fn blocking_handle_reads_of_a_completed_workflow_answer_its_result() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    let workflow_id = off_the_runtime({
        let url = url.clone();
        move || start_one_node_workflow(&url, "blocking_views_completed")
    })
    .await;
    end_the_node(&database, &worker, workflow_id).await;

    off_the_runtime(move || {
        let (app, _echo_function) = app_on(&url);
        let handle = app.workflow_handle::<u32>(workflow_id);
        let view = handle.blocking();

        let status = view.status();
        assert!(
            matches!(status, Ok(WorkflowStatus::Completed)),
            "{status:?}"
        );
        match view.get(WaitLimit::NoLimit) {
            Ok(TaskOutcome::Succeeded(value)) => assert_eq!(value, ANSWER),
            other => panic!("expected the result {ANSWER}, got {other:?}"),
        }
        match view.result_for::<u32, Infallible>(&NodeKey::new(ANSWER_NODE)) {
            Ok(TaskOutcome::Succeeded(value)) => assert_eq!(value, ANSWER),
            other => panic!("expected the node result {ANSWER}, got {other:?}"),
        }
        let results = view.results().expect("the node results");
        assert_eq!(results.len(), 1, "{results:?}");
        match results.get(ANSWER_NODE) {
            Some(NodeResult::Outcome(TaskOutcome::Succeeded(text))) => {
                assert_eq!(text.get(), ANSWER.to_string());
            }
            other => panic!("expected the stored value {ANSWER}, got {other:?}"),
        }
    })
    .await;
    worker.close().await;
    database.drop().await;
}

/// A workflow id that no row holds: every handle method is
/// `WORKFLOW_NOT_FOUND`, and `get` too.
#[tokio::test]
async fn blocking_handle_methods_of_an_unknown_workflow_are_workflow_not_found() {
    let (database, worker) = covered_database().await;
    let url = database.url().to_owned();
    off_the_runtime(move || {
        let (app, _echo_function) = app_on(&url);
        let workflow_id = Uuid::new_v4();
        let handle = app.workflow_handle::<u32>(workflow_id);
        let view = handle.blocking();

        assert_workflow_not_found(view.status(), workflow_id);
        assert_workflow_not_found(view.tasks(), workflow_id);
        assert_workflow_not_found(view.results(), workflow_id);
        assert_workflow_not_found(view.pause(), workflow_id);
        assert_workflow_not_found(view.resume(), workflow_id);
        assert_workflow_not_found(view.cancel(), workflow_id);
        match view.get(wait_of(SHORT_WAIT)) {
            Err(WorkflowReadError::WorkflowNotFound {
                workflow_id: missing,
            }) => assert_eq!(missing, workflow_id),
            other => panic!("expected WORKFLOW_NOT_FOUND, got {other:?}"),
        }
    })
    .await;
    worker.close().await;
    database.drop().await;
}
