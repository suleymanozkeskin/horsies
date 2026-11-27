//! Two apps in one process, each on a database of its own. Each send, each
//! workflow start of a spec and each start of a definition by its type goes
//! to the database of the app that it names. A send, a node or a start by
//! type of a task or a definition that the app does not register is refused
//! and writes nothing, also when another app of the process registers it.
//! No send, node or start reads a process-wide registration.
//!
//! Each database is a copy of the core's test template
//! (`core/src/test_support/database.rs`), so the tests need the test
//! PostgreSQL server of that module. A worker broker of each test adds the
//! default retention class that a send and a start write on each task.

#![cfg(feature = "tokio-postgres")]

use std::convert::Infallible;

use horsies::{
    task, AppConfig, Horsies, HorsiesError, HorsiesErrorCode, TaskError, TaskSendError,
    TaskSendOptions, WholeSeconds, WorkflowDefConfig, WorkflowDefinition, WorkflowSpec,
    WorkflowSpecBuilder, WorkflowStartError,
};
use horsies_core::{
    CoverageOutcome, CoverageRequest, CoverageSerialization, FailureCause, PostgresBroker,
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

/// The history leaves that the coverage pass creates ahead, in days and in
/// hours.
const COVERAGE_HISTORY_DAYS: u32 = 2;
const COVERAGE_HEARTBEAT_HOURS: u32 = 2;

/// The delay of the delayed send: longer than the test.
const DELAY_SECS: u32 = 3_600;

/// The node id of the one node of each workflow.
const ANSWER_NODE: &str = "answer";

/// The value that `answer` returns.
const ANSWER: u32 = 100;

#[task("two_apps_echo")]
async fn echo(value: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(value)
}

#[task("two_apps_answer")]
async fn answer() -> Result<u32, TaskError<Infallible>> {
    Ok(ANSWER)
}

/// The one node of each definition: `answer`, as the output.
fn answer_output(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
    let output = builder.task(answer::node().node_id(ANSWER_NODE))?;
    Ok(WorkflowDefConfig::new().output(output))
}

/// A definition without params, started by its type.
struct Nightly;

impl WorkflowDefinition for Nightly {
    type Output = u32;
    type Params = ();

    fn name() -> &'static str {
        "two_apps_nightly"
    }

    fn definition_key() -> &'static str {
        "tests.two_apps.nightly.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        answer_output(builder)
    }
}

/// A definition with params, started by its type.
struct Regional;

impl WorkflowDefinition for Regional {
    type Output = u32;
    type Params = u32;

    fn name() -> &'static str {
        "two_apps_regional"
    }

    fn definition_key() -> &'static str {
        "tests.two_apps.regional.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        answer_output(builder)
    }
}

/// The params of each start of [`Regional`]; the build does not read them.
const REGIONAL_PARAMS: u32 = 1;

/// A test database, and a worker broker on it that ran one coverage pass.
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

/// An app on the database of `url` that registers `echo` and `answer`,
/// then [`Nightly`] and [`Regional`]. No I/O.
fn app_on(url: &str) -> Horsies {
    let mut app = Horsies::new(AppConfig::for_database_url(url)).expect("the app");
    let _echo = echo::register(&mut app).expect("register echo");
    let _answer = answer::register(&mut app).expect("register answer");
    let _nightly = app
        .register_workflow_definition::<Nightly>()
        .expect("register the definition");
    let _regional = app
        .workflow_template::<Regional>()
        .expect("register the template");
    app
}

/// `refused` is the refusal of a start whose definition the app does not
/// register: `VALIDATION_FAILED` with the cause `PreconditionFailed`.
fn assert_start_not_registered(refused: &WorkflowStartError) {
    assert!(
        matches!(refused, WorkflowStartError::ValidationFailed { .. }),
        "{refused}"
    );
    assert_eq!(
        refused.failure_cause(),
        FailureCause::PreconditionFailed,
        "{refused}"
    );
}

/// The spec of a workflow `name` of one node, the task `answer`, which is
/// its output, on a workflow builder of `app`.
fn one_node_spec(app: &Horsies, name: &str) -> WorkflowSpec {
    let mut builder = app.workflow_builder(name);
    builder.definition_key(format!("tests.two_apps.{name}.v1"));
    let output = builder
        .task(answer::node().node_id(ANSWER_NODE))
        .expect("the app registers answer");
    builder.output(output);
    builder.build().expect("the spec builds")
}

/// A table that holds the rows of the sends or of the starts.
#[derive(Debug, Clone, Copy)]
enum Table {
    Tasks,
    Workflows,
}

impl Table {
    /// The count of the rows of the table with one id.
    fn count_by_id(self) -> &'static str {
        match self {
            Self::Tasks => "SELECT count(*) FROM horsies_tasks WHERE id = $1",
            Self::Workflows => "SELECT count(*) FROM horsies_workflows WHERE id = $1",
        }
    }
}

/// Whether a database holds a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Presence {
    Present,
    Absent,
}

impl Presence {
    /// The count of the rows with the id of the row.
    fn count(self) -> i64 {
        match self {
            Self::Present => 1,
            Self::Absent => 0,
        }
    }
}

/// The row of `table` with the id `id` is in `database`, or is not.
async fn assert_row(database: &TestDatabase, table: Table, id: Uuid, presence: Presence) {
    let count: i64 = database
        .client()
        .query_one(table.count_by_id(), &[&id])
        .await
        .expect("count the rows")
        .get(0);
    assert_eq!(
        count,
        presence.count(),
        "{table:?} row {id} in database {}",
        database.name()
    );
}

/// Whether the task `task_id` of `database` is due after now.
async fn due_after_now(database: &TestDatabase, task_id: Uuid) -> bool {
    database
        .client()
        .query_one(
            "SELECT enqueued_at > NOW() FROM horsies_tasks WHERE id = $1",
            &[&task_id],
        )
        .await
        .expect("read the due time of the task")
        .get(0)
}

/// Each app of one process sends and starts on its own database: the rows
/// of the first app are in the first database only, the rows of the
/// second app in the second database only. A start of a spec and a start
/// of a definition by its type each go through the app that they name. The
/// delayed send of the second app is due after its delay.
#[tokio::test]
async fn each_send_and_each_start_goes_to_the_database_of_its_app() {
    let (first_database, first_worker) = covered_database().await;
    let (second_database, second_worker) = covered_database().await;
    let first = app_on(first_database.url());
    let second = app_on(second_database.url());

    let first_task = echo::send(&first, 1)
        .await
        .expect("the send through the first app")
        .task_id();
    let delayed = TaskSendOptions::new().delay(WholeSeconds::from_secs(DELAY_SECS));
    let second_task = echo::with_options(delayed)
        .send(&second, 2)
        .await
        .expect("the delayed send through the second app")
        .task_id();
    let first_workflow = first
        .start::<u32>(&one_node_spec(&first, "first"))
        .await
        .expect("the start through the first app")
        .workflow_id();
    let second_workflow = second
        .start::<u32>(&one_node_spec(&second, "second"))
        .await
        .expect("the start through the second app")
        .workflow_id();
    let first_by_type = first
        .start_workflow::<Nightly>()
        .await
        .expect("the start by type through the first app")
        .workflow_id();
    let second_by_type = second
        .start_workflow_with::<Regional>(REGIONAL_PARAMS)
        .await
        .expect("the start by type with params through the second app")
        .workflow_id();

    let first_rows = [
        (Table::Tasks, first_task),
        (Table::Workflows, first_workflow),
        (Table::Workflows, first_by_type),
    ];
    let second_rows = [
        (Table::Tasks, second_task),
        (Table::Workflows, second_workflow),
        (Table::Workflows, second_by_type),
    ];
    for (database, own, other) in [
        (&first_database, &first_rows, &second_rows),
        (&second_database, &second_rows, &first_rows),
    ] {
        for &(table, id) in own {
            assert_row(database, table, id, Presence::Present).await;
        }
        for &(table, id) in other {
            assert_row(database, table, id, Presence::Absent).await;
        }
    }
    assert!(!due_after_now(&first_database, first_task).await);
    assert!(due_after_now(&second_database, second_task).await);
    first_worker.close().await;
    second_worker.close().await;
    first_database.drop().await;
    second_database.drop().await;
}

/// An app that registers no task and no definition, on a database where
/// another app of the process registers them: its send and its starts by
/// type are `VALIDATION_FAILED` with the cause `PreconditionFailed`, its
/// workflow builder refuses the node with `HRS-302`, and the database holds
/// no task and no workflow.
#[tokio::test]
async fn a_send_and_a_node_of_a_task_that_the_app_does_not_register_write_nothing() {
    let (database, worker) = covered_database().await;
    let _registered = app_on(database.url());
    let app = Horsies::new(AppConfig::for_database_url(database.url())).expect("the app");

    let refused = match echo::send(&app, 1).await {
        Err(refused) => refused,
        Ok(handle) => panic!("the send wrote task {}", handle.task_id()),
    };
    match &refused {
        TaskSendError::ValidationFailed { cause, .. } => {
            assert_eq!(*cause, FailureCause::PreconditionFailed, "{refused}");
        }
        other => panic!("expected VALIDATION_FAILED, got {other}"),
    }
    let mut builder = app.workflow_builder("unregistered");
    let refused = match builder.task(answer::node()) {
        Err(refused) => refused,
        Ok(node) => panic!("the builder added the node at index {}", node.index()),
    };
    assert_eq!(
        refused.code,
        Some(HorsiesErrorCode::WorkflowUnregisteredTask),
        "{refused}"
    );
    let refused = match app.start_workflow::<Nightly>().await {
        Err(refused) => refused,
        Ok(handle) => panic!("the start wrote workflow {}", handle.workflow_id()),
    };
    assert_start_not_registered(&refused);
    let refused = match app.start_workflow_with::<Regional>(REGIONAL_PARAMS).await {
        Err(refused) => refused,
        Ok(handle) => panic!("the start wrote workflow {}", handle.workflow_id()),
    };
    assert_start_not_registered(&refused);

    for table in ["horsies_tasks", "horsies_workflows"] {
        let count_sql = format!("SELECT count(*) FROM {table}");
        let rows: i64 = database
            .client()
            .query_one(count_sql.as_str(), &[])
            .await
            .expect("count the rows")
            .get(0);
        assert_eq!(rows, 0, "{table}");
    }
    worker.close().await;
    database.drop().await;
}
