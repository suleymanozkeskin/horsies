//! Registration and the start refusals before any I/O. Each test has its
//! own definition types: the process-wide starts keep the first
//! registration of a type.

use serde::{Deserialize, Serialize};

use super::*;
use crate::workflow::node::tests::task_node;
use crate::workflow::{OutputlessResults, WorkflowDefConfig, WorkflowSpecBuilder};
use crate::AppConfig;
use horsies_core::WorkflowStartErrorCode;

fn app() -> Horsies {
    Horsies::new(AppConfig::for_database_url(
        "postgresql://localhost/horsies",
    ))
    .unwrap()
}

fn one_task(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
    let tick = builder.task(task_node::<u32, ()>("tick"));
    Ok(WorkflowDefConfig::new().output(tick))
}

struct Daily;

impl WorkflowDefinition for Daily {
    type Output = u32;
    type Params = ();

    fn name() -> &'static str {
        "daily"
    }

    fn definition_key() -> &'static str {
        "tests.function.daily.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        one_task(builder)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Region {
    region: String,
}

struct Regional;

impl WorkflowDefinition for Regional {
    type Output = u32;
    type Params = Region;

    fn name() -> &'static str {
        "regional"
    }

    fn definition_key() -> &'static str {
        "tests.function.regional.v1"
    }

    fn build_with(params: Region) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = WorkflowSpecBuilder::new(format!("regional_{}", params.region));
        builder.definition_key(Self::definition_key());
        let tick = builder.task(task_node::<u32, ()>("tick"));
        builder.output(tick);
        builder.build()
    }
}

/// Its build refuses every region.
struct RefusingRegional;

impl WorkflowDefinition for RefusingRegional {
    type Output = u32;
    type Params = Region;

    fn name() -> &'static str {
        "refusing_regional"
    }

    fn definition_key() -> &'static str {
        "tests.function.refusing_regional.v1"
    }

    fn build_with(params: Region) -> Result<WorkflowSpec, HorsiesError> {
        Err(HorsiesError::new(format!("bad region: {}", params.region)))
    }
}

/// Never registered: its process-wide start is refused.
struct Unregistered;

impl WorkflowDefinition for Unregistered {
    type Output = OutputlessResults;
    type Params = ();

    fn name() -> &'static str {
        "unregistered"
    }

    fn definition_key() -> &'static str {
        "tests.function.unregistered.v1"
    }
}

#[test]
fn a_definition_registers_and_gives_its_function() {
    let mut app = app();
    let function = app.register_workflow_definition::<Daily>().unwrap();
    assert_eq!(function.name(), "daily");
    assert_eq!(function.definition_key(), Some("tests.function.daily.v1"));
    assert_eq!(function.spec().node_count(), 1);
    assert!(app
        .workflows()
        .definition("tests.function.daily.v1")
        .is_some());
    let again = app.register_workflow_definition::<Daily>().unwrap_err();
    assert_eq!(again.code, Some(ErrorCode::WorkflowDuplicateDefinitionKey));
}

#[test]
fn a_template_builds_from_params_and_makes_its_node() {
    let mut app = app();
    let template = app.workflow_template::<Regional>().unwrap();
    assert_eq!(template.name(), "regional");
    let spec = template
        .build(Region {
            region: "eu".to_owned(),
        })
        .unwrap();
    assert_eq!(spec.name(), "regional_eu");
    let node = template.node(SubWorkflowParams::NotGiven).unwrap();
    assert_eq!(node.definition_key(), "tests.function.regional.v1");
}

#[test]
fn a_builder_without_a_case_is_refused() {
    let app = app();
    let refused = app
        .check_workflow_builder("empty", |_case: &u32| Daily::build())
        .register()
        .unwrap_err();
    assert_eq!(refused.code, Some(ErrorCode::WorkflowCheckCasesRequired));
}

#[test]
fn the_check_builds_each_case() {
    let app = app();
    app.check_workflow_builder("by_region", |case: &u32| match case {
        0 => Daily::build(),
        _ => Err(HorsiesError::new("no region")),
    })
    .cases([0, 1])
    .register()
    .unwrap();
    let findings = app.workflow_check_findings();
    assert_eq!(findings.len(), 1, "{findings:?}");
    assert_eq!(findings[0].code, Some(ErrorCode::WorkflowCheckCaseInvalid));
}

/// Python `_check_workflow_builders`: the check runs each case with the
/// sends of the process suppressed, and leaves the suppression as it was.
#[test]
fn the_check_runs_each_case_with_the_sends_suppressed() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let app = app();
    let suppression = app.send_suppression();
    let seen = Arc::new(AtomicBool::new(false));
    let seen_in_case = Arc::clone(&seen);
    app.check_workflow_builder("suppressed", move |_case: &u32| {
        seen_in_case.store(suppression.is_active(), Ordering::Relaxed);
        Daily::build()
    })
    .case(0)
    .register()
    .unwrap();
    assert!(!app.are_sends_suppressed());
    let findings = app.workflow_check_findings();
    assert!(findings.is_empty(), "{findings:?}");
    assert!(seen.load(Ordering::Relaxed));
    assert!(!app.are_sends_suppressed());
}

#[tokio::test]
async fn a_spec_without_a_definition_key_is_refused_before_any_io() {
    let app = app();
    let mut builder = WorkflowSpecBuilder::new("keyless");
    let tick = builder.task(task_node::<u32, ()>("tick"));
    builder.output(tick);
    let spec = builder.build().unwrap();
    let refused = app.start::<u32>(&spec).await.unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert!(!refused.retryable());
    assert_eq!(refused.workflow_name(), "keyless");
}

#[tokio::test]
async fn an_output_type_that_is_not_the_handle_s_is_refused_before_any_io() {
    let app = app();
    let spec = Daily::build().unwrap();
    let refused = app.start::<String>(&spec).await.unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert!(refused.message().contains("HRS-025"), "{refused}");
}

#[tokio::test]
async fn a_spec_with_an_unregistered_child_is_refused_before_any_io() {
    let app = app();
    let mut builder = WorkflowSpecBuilder::new("parent");
    builder.definition_key("tests.function.parent.v1");
    builder.sub_workflow(Daily::node(SubWorkflowParams::NotGiven).unwrap());
    let spec = builder.build().unwrap();
    let refused = app.start::<OutputlessResults>(&spec).await.unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
}

#[tokio::test]
async fn a_retry_of_an_error_that_is_not_enqueue_failed_is_refused() {
    let mut app = app();
    let function = app.register_workflow_definition::<Daily>().unwrap();
    let error = WorkflowStartError::ValidationFailed {
        message: "earlier".to_owned(),
        workflow_name: "daily".to_owned(),
        workflow_id: Uuid::nil(),
        cause: horsies_core::StartRefusalCause::Check,
    };
    let refused = function.retry_start(&error).await.unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
}

#[tokio::test]
async fn a_process_wide_start_before_registration_is_refused() {
    let refused = start_workflow::<Unregistered>().await.unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert!(refused.message().contains("register_workflow_definition"));
    assert_eq!(
        refused.failure_cause(),
        horsies_core::FailureCause::PreconditionFailed
    );
}

/// Python mints the id of a start before its checks: the refusal before the
/// registration carries the id of its start.
#[tokio::test]
async fn a_process_wide_start_before_registration_names_the_id_of_its_start() {
    let workflow_id = Uuid::from_u128(21);
    let refused = start_workflow_as::<Unregistered>(workflow_id)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert_eq!(refused.workflow_id(), workflow_id);
}

#[test]
fn the_first_registration_serves_the_process_wide_start() {
    let mut first = app();
    let mut second = app();
    let function = first.register_workflow_definition::<Daily>().unwrap();
    let _later = second.register_workflow_definition::<Daily>().unwrap();
    let kept = global::<Daily, WorkflowFunction<u32>>().unwrap();
    assert_eq!(kept.name(), function.name());
}

/// A template start whose build refuses is `VALIDATION_FAILED`, not
/// retryable, with the name of the workflow and the text of the build's
/// error, before any I/O.
#[tokio::test]
async fn a_template_start_whose_build_refuses_is_validation_failed_before_any_io() {
    let mut app = app();
    let template = app.workflow_template::<RefusingRegional>().unwrap();
    let refused = template
        .start(Region {
            region: "antarctica".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert_eq!(refused.workflow_name(), "refusing_regional");
    assert!(!refused.retryable());
    assert!(
        refused.message().contains("bad region: antarctica"),
        "{}",
        refused.message()
    );
}
