//! Registration, the starts of a definition by its type on one app, and the
//! start refusals before any I/O.

use serde::{Deserialize, Serialize};

use super::*;
use crate::workflow::node::tests::{add, task_node, test_builder};
use crate::workflow::{OutputlessResults, WorkflowDefConfig, WorkflowSpecBuilder};
use crate::AppConfig;
use horsies_core::WorkflowStartErrorCode;

fn app() -> Horsies {
    Horsies::new(AppConfig::for_database_url(
        "postgresql://localhost/horsies",
    ))
    .unwrap()
}

/// An app whose address has no database: a start that a defect lets past
/// its lookup fails to connect, and writes to no local database.
fn app_without_database() -> Horsies {
    Horsies::new(AppConfig::for_database_url(
        "postgresql://127.0.0.1:1/nothing",
    ))
    .unwrap()
}

fn one_task(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
    let tick = add(builder, task_node::<u32, ()>("tick"));
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

    fn build_with(
        mut builder: WorkflowSpecBuilder,
        params: Region,
    ) -> Result<WorkflowSpec, HorsiesError> {
        builder.definition_key(Self::definition_key());
        let tick = add(
            &mut builder,
            task_node::<u32, ()>("tick").node_id(params.region),
        );
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

    fn build_with(
        _builder: WorkflowSpecBuilder,
        params: Region,
    ) -> Result<WorkflowSpec, HorsiesError> {
        Err(HorsiesError::new(format!("bad region: {}", params.region)))
    }
}

/// The key of [`Daily`] on another type: a start by type does not take one
/// for the other.
struct DailyTwin;

impl WorkflowDefinition for DailyTwin {
    type Output = u32;
    type Params = ();

    fn name() -> &'static str {
        "daily_twin"
    }

    fn definition_key() -> &'static str {
        Daily::definition_key()
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        one_task(builder)
    }
}

/// A definition without params whose one node is a child that no app
/// registers: its start passes the lookup and is refused by the check of
/// the spec, before any I/O.
struct ParentOfUnregistered;

impl WorkflowDefinition for ParentOfUnregistered {
    type Output = OutputlessResults;
    type Params = ();

    fn name() -> &'static str {
        "parent_of_unregistered"
    }

    fn definition_key() -> &'static str {
        "tests.function.parent_of_unregistered.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        builder.sub_workflow(Unregistered::node(SubWorkflowParams::NotGiven)?);
        Ok(WorkflowDefConfig::new())
    }
}

/// Never registered: its start is refused.
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
    assert_eq!(again.code, Some(HorsiesErrorCode::WorkflowDuplicateDefinitionKey));
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
    assert_eq!(spec.name(), "regional");
    assert_eq!(spec.node_ids().collect::<Vec<_>>(), vec!["eu"]);
    let node = template.node(SubWorkflowParams::NotGiven).unwrap();
    assert_eq!(node.definition_key(), "tests.function.regional.v1");
}

#[test]
fn a_builder_without_a_case_is_refused() {
    let app = app();
    let refused = app
        .check_workflow_builder("empty", |builder, _case: &u32| Daily::build(builder))
        .register()
        .unwrap_err();
    assert_eq!(refused.code, Some(HorsiesErrorCode::WorkflowCheckCasesRequired));
}

#[test]
fn the_check_builds_each_case() {
    let app = app();
    app.check_workflow_builder("by_region", |builder, case: &u32| match case {
        0 => Daily::build(builder),
        _ => Err(HorsiesError::new("no region")),
    })
    .cases([0, 1])
    .register()
    .unwrap();
    let findings = app.workflow_check_findings();
    assert_eq!(findings.len(), 1, "{findings:?}");
    assert_eq!(findings[0].code, Some(HorsiesErrorCode::WorkflowCheckCaseInvalid));
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
    app.check_workflow_builder("suppressed", move |builder, _case: &u32| {
        seen_in_case.store(suppression.is_active(), Ordering::Relaxed);
        Daily::build(builder)
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
    let mut builder = test_builder("keyless");
    let tick = add(&mut builder, task_node::<u32, ()>("tick"));
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
    let spec = Daily::build(app.workflow_builder(Daily::name())).unwrap();
    let refused = app.start::<String>(&spec).await.unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert!(refused.message().contains("HRS-025"), "{refused}");
}

#[tokio::test]
async fn a_spec_with_an_unregistered_child_is_refused_before_any_io() {
    let app = app();
    let mut builder = test_builder("parent");
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

/// `refused` is the refusal of a start of a definition that the app does
/// not register in the form of `method`: `VALIDATION_FAILED` with the cause
/// `PreconditionFailed`.
fn assert_not_registered(refused: &WorkflowStartError, method: &str) {
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert!(
        refused.message().contains("is not registered on this app"),
        "{refused}"
    );
    assert!(refused.message().contains(method), "{refused}");
    assert_eq!(
        refused.failure_cause(),
        horsies_core::FailureCause::PreconditionFailed
    );
}

/// A start of a definition that the app does not register is refused
/// before any I/O, also when another app of the process registers it.
#[tokio::test]
async fn a_start_of_a_definition_that_the_app_does_not_register_is_refused() {
    let without = app_without_database();
    let refused = without.start_workflow::<Unregistered>().await.unwrap_err();
    assert_not_registered(&refused, "register_workflow_definition");

    let mut registered = app_without_database();
    let _function = registered.register_workflow_definition::<Daily>().unwrap();
    let _template = registered.workflow_template::<RefusingRegional>().unwrap();
    let refused = without.start_workflow::<Daily>().await.unwrap_err();
    assert_not_registered(&refused, "register_workflow_definition");
    let refused = without
        .start_workflow_with::<RefusingRegional>(Region {
            region: "eu".to_owned(),
        })
        .await
        .unwrap_err();
    assert_not_registered(&refused, "workflow_template");
}

/// Python mints the id of a start before its checks: the refusal carries
/// the id of its start.
#[tokio::test]
async fn a_refused_start_names_the_id_of_its_start() {
    let workflow_id = Uuid::from_u128(21);
    let refused = app_without_database()
        .start_workflow_as::<Unregistered>(workflow_id)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert_eq!(refused.workflow_id(), workflow_id);
}

/// A start by type takes the definition only in the form of its
/// registration, and only of its own type: a definition without params is
/// not a template, and another type with the same key is not `Daily`.
#[tokio::test]
async fn a_start_takes_the_registered_definition_of_its_type_and_form_only() {
    let mut app = app_without_database();
    let _function = app.register_workflow_definition::<Daily>().unwrap();
    let refused = app.start_workflow_with::<Daily>(()).await.unwrap_err();
    assert_not_registered(&refused, "workflow_template");
    let refused = app.start_workflow::<DailyTwin>().await.unwrap_err();
    assert_not_registered(&refused, "register_workflow_definition");
}

/// A start of a registered definition passes the lookup and runs the
/// checks of its spec: the refusal of an unregistered child, before any
/// I/O, in place of the lookup refusal.
#[tokio::test]
async fn a_start_of_a_registered_definition_runs_the_checks_of_its_spec() {
    let mut app = app_without_database();
    let _function = app
        .register_workflow_definition::<ParentOfUnregistered>()
        .unwrap();
    let refused = app
        .start_workflow::<ParentOfUnregistered>()
        .await
        .unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert!(
        refused.message().contains("references child workflow"),
        "{refused}"
    );
    assert!(
        !refused.message().contains("is not registered on this app"),
        "{refused}"
    );
}

/// A template start through the app runs the build of its registration:
/// the build's refusal, not the lookup refusal.
#[tokio::test]
async fn a_template_start_through_the_app_runs_its_build() {
    let mut app = app_without_database();
    let _template = app.workflow_template::<RefusingRegional>().unwrap();
    let refused = app
        .start_workflow_with::<RefusingRegional>(Region {
            region: "antarctica".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
    assert!(
        refused.message().contains("bad region: antarctica"),
        "{refused}"
    );
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
