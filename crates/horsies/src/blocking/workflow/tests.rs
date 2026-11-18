//! The blocking workflow views on a thread that runs a tokio runtime: each
//! answers `ASYNC_CONTEXT` and runs nothing. The app's address has no
//! database; no test connects.

use std::convert::Infallible;

use serde::{Deserialize, Serialize};

use super::*;
use crate::workflow::tests_support::unit_task_node;
use crate::{AppConfig, Horsies, HorsiesError, WorkflowDefConfig, WorkflowSpecBuilder};
use horsies_core::WorkflowStartErrorCode;

fn app() -> Horsies {
    Horsies::new(AppConfig::for_database_url(
        "postgresql://127.0.0.1:1/nothing",
    ))
    .unwrap()
}

/// One task node as the output.
fn one_task(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
    let tick = builder.task(unit_task_node("tick"));
    Ok(WorkflowDefConfig::new().output(tick))
}

/// A definition without params, for the views of a workflow function.
struct Nightly;

impl WorkflowDefinition for Nightly {
    type Output = u32;
    type Params = ();

    fn name() -> &'static str {
        "blocking_nightly"
    }

    fn definition_key() -> &'static str {
        "tests.blocking.nightly.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        one_task(builder)
    }
}

/// A definition without params, for the process-wide start. Its own type:
/// the process keeps the first registration of each type.
struct Weekly;

impl WorkflowDefinition for Weekly {
    type Output = u32;
    type Params = ();

    fn name() -> &'static str {
        "blocking_weekly"
    }

    fn definition_key() -> &'static str {
        "tests.blocking.weekly.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        one_task(builder)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Region {
    region: String,
}

/// A definition with params whose build always refuses: on a runtime
/// thread the build does not run, so the answer is `ASYNC_CONTEXT`.
struct Regional;

impl WorkflowDefinition for Regional {
    type Output = u32;
    type Params = Region;

    fn name() -> &'static str {
        "blocking_regional"
    }

    fn definition_key() -> &'static str {
        "tests.blocking.regional.v1"
    }

    fn build_with(params: Region) -> Result<WorkflowSpec, HorsiesError> {
        Err(HorsiesError::new(format!(
            "region {} has no build",
            params.region
        )))
    }
}

fn region() -> Region {
    Region {
        region: "eu".to_owned(),
    }
}

/// Python `WorkflowStartAsyncContext`: nothing written, not retryable, with
/// the workflow name and the id of the start.
fn assert_async_context(refused: &WorkflowStartError, workflow_name: &str) {
    assert_eq!(
        refused.code(),
        WorkflowStartErrorCode::AsyncContext,
        "{refused}"
    );
    assert!(!refused.retryable(), "{refused}");
    assert_eq!(refused.workflow_name(), workflow_name);
    assert!(refused.workflow_id().is_some(), "{refused}");
}

#[tokio::test]
async fn a_blocking_start_on_a_runtime_thread_writes_nothing_and_keeps_its_id() {
    let mut app = app();
    let function = app.register_workflow_definition::<Nightly>().unwrap();
    let refused = function.blocking().start().unwrap_err();
    assert_async_context(&refused, "blocking_nightly");
    assert!(
        refused.message().contains("`function.start().await`"),
        "{}",
        refused.message()
    );
    let workflow_id = Uuid::from_u128(7);
    let with_id = function.blocking().start_with_id(workflow_id).unwrap_err();
    assert_async_context(&with_id, "blocking_nightly");
    assert_eq!(with_id.workflow_id(), Some(workflow_id));
}

/// Python's order: the error is checked first, on any thread; a valid retry
/// on a runtime thread answers `ASYNC_CONTEXT` with the id of the error.
#[tokio::test]
async fn a_blocking_retry_checks_its_error_before_the_thread() {
    let mut app = app();
    let function = app.register_workflow_definition::<Nightly>().unwrap();
    let workflow_id = Uuid::from_u128(11);
    let failed = |workflow_name: &str| WorkflowStartError::EnqueueFailed {
        message: "the commit reply was lost".to_owned(),
        retryable: true,
        workflow_name: workflow_name.to_owned(),
        workflow_id,
    };
    let other = function
        .blocking()
        .retry_start(&failed("another"))
        .unwrap_err();
    assert_eq!(other.code(), WorkflowStartErrorCode::ValidationFailed);
    let retried = function
        .blocking()
        .retry_start(&failed("blocking_nightly"))
        .unwrap_err();
    assert_async_context(&retried, "blocking_nightly");
    assert_eq!(retried.workflow_id(), Some(workflow_id));
}

/// Python's `_validate_start_retry` takes an `ASYNC_CONTEXT` error: the
/// retry passes the error check and, on a runtime thread, answers
/// `ASYNC_CONTEXT` with the id of the error, not `VALIDATION_FAILED`.
#[tokio::test]
async fn a_blocking_retry_takes_an_async_context_error_of_its_workflow() {
    let mut app = app();
    let function = app.register_workflow_definition::<Nightly>().unwrap();
    let workflow_id = Uuid::from_u128(12);
    let earlier = function.blocking().start_with_id(workflow_id).unwrap_err();
    assert_async_context(&earlier, "blocking_nightly");
    let retried = function.blocking().retry_start(&earlier).unwrap_err();
    assert_async_context(&retried, "blocking_nightly");
    assert_eq!(retried.workflow_id(), Some(workflow_id));
}

#[tokio::test]
async fn a_blocking_template_start_on_a_runtime_thread_runs_no_build() {
    let mut app = app();
    let template = app.workflow_template::<Regional>().unwrap();
    assert_async_context(
        &template.blocking().start(region()).unwrap_err(),
        "blocking_regional",
    );
    let workflow_id = Uuid::from_u128(13);
    let with_id = template
        .blocking()
        .start_with_id(region(), workflow_id)
        .unwrap_err();
    assert_async_context(&with_id, "blocking_regional");
    assert_eq!(with_id.workflow_id(), Some(workflow_id));
}

#[tokio::test]
async fn a_blocking_process_wide_start_on_a_runtime_thread_writes_nothing() {
    let mut app = app();
    let _function = app.register_workflow_definition::<Weekly>().unwrap();
    assert_async_context(&start_workflow::<Weekly>().unwrap_err(), "blocking_weekly");
    let _template = app.workflow_template::<Regional>().unwrap();
    assert_async_context(
        &start_workflow_with::<Regional>(region()).unwrap_err(),
        "blocking_regional",
    );
}

#[tokio::test]
async fn a_blocking_spec_start_on_a_runtime_thread_writes_nothing() {
    let mut builder = WorkflowSpecBuilder::new("blocking_adhoc");
    builder.definition_key("tests.blocking.adhoc.v1");
    let tick = builder.task(unit_task_node("tick"));
    builder.output(tick);
    let spec = builder.build().unwrap();
    let app = app();
    assert_async_context(
        &app.blocking().start::<u32>(&spec).unwrap_err(),
        "blocking_adhoc",
    );
    let workflow_id = Uuid::from_u128(17);
    let with_id = app
        .blocking()
        .start_with_id::<u32>(&spec, workflow_id)
        .unwrap_err();
    assert_eq!(with_id.workflow_id(), Some(workflow_id));
}

/// Python `HandleAsyncContext`: each handle method answers
/// `ASYNC_CONTEXT`, not retryable, with the workflow id, and names its
/// async form.
#[tokio::test]
async fn a_blocking_handle_method_on_a_runtime_thread_is_async_context() {
    let workflow_id = Uuid::from_u128(19);
    let handle = app().workflow_handle::<u32>(workflow_id);
    let view = handle.blocking();
    let refusals = [
        view.status().map(|_status| ()).unwrap_err(),
        view.results().map(|_results| ()).unwrap_err(),
        view.tasks().map(|_tasks| ()).unwrap_err(),
        view.cancel().unwrap_err(),
        view.pause().map(|_paused| ()).unwrap_err(),
        view.resume().map(|_resumed| ()).unwrap_err(),
    ];
    for refused in &refusals {
        assert_eq!(refused.code(), HandleErrorCode::AsyncContext, "{refused}");
        assert!(!refused.retryable(), "{refused}");
        assert_eq!(refused.workflow_id(), workflow_id);
    }
    assert!(
        refusals[0].message().contains("`handle.status().await`"),
        "{}",
        refusals[0].message()
    );
}

/// Python's sync `get` and `result_for` answer `RetrievalCode.ASYNC_CONTEXT`.
#[tokio::test]
async fn a_blocking_outcome_read_on_a_runtime_thread_is_async_context() {
    let handle = app().workflow_handle::<u32>(Uuid::from_u128(23));
    let read = handle.blocking().get(WaitLimit::NoLimit).unwrap_err();
    assert!(
        matches!(read, WorkflowReadError::AsyncContext { .. }),
        "{read}"
    );
    assert_eq!(read.code().value(), "ASYNC_CONTEXT");
    assert!(
        read.to_string().contains("`handle.get(limit).await`"),
        "{read}"
    );
    let node = handle
        .blocking()
        .result_for::<u32, Infallible>(&NodeKey::new("tick"))
        .unwrap_err();
    assert!(
        matches!(node, WorkflowReadError::AsyncContext { .. }),
        "{node}"
    );
}
