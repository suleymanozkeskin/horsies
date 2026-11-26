//! The parse of spec data, the workflow start, and the resume.
//!
//! - `prepareClientSpec` is pure: the core's parse of a spec data text
//!   (`parse_workflow_spec_data`). A refusal is a value with the field path
//!   and the cause, not a throw.
//! - `startWorkflow` is the core's start of a client spec
//!   (`PostgresBroker::start_client_workflow`) with the handle's child spec
//!   source, as the Rust client's start: the schema install of the handle
//!   first, then the start. The outcome map is the Rust client's: a start
//!   error is a value of the workflow start catalog.
//! - `resumeWorkflow` is the core's resume of a client workflow
//!   (`PostgresBroker::resume_client_workflow`) with the same source.
//! - A start and a resume need the source: a handle with no source is
//!   `CLIENT_DEFECT`, because the package registers it before the first
//!   start or resume. A boundary value that breaks a rule is `CLIENT_DEFECT`
//!   too. In both cases nothing starts.
//! - Writes: a stop of the core in flight leaves the outcome unknown. A
//!   start again with the same id and the same spec data finds the run.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::telemetry::trace_carrier::SendTrace;
use horsies_core::{
    parse_workflow_spec_data, ChildSpecSource, ClientSpec, ClientStartError, ClientStartOutcome,
    ExistingRun, HandleOperation, StartRefusalCause, WorkflowError, WorkflowStartError,
};
use napi::bindgen_prelude::{BigInt, Object};
use napi::Env;
use napi_derive::napi;
use uuid::Uuid;

use crate::call::guard::{sync_call, CallKind};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::CoreHandle;
use crate::process_core::runtime::pure_call_refusal;
use crate::sends::producer_handle::ProducerHandle;
use crate::values::boundary::instant_of_epoch_micros;
use crate::values::catalog_values::RawCatalogError;
use crate::values::trace_carrier::TraceArgument;
use crate::workflows::handle_ops::{
    broker_for, change_of, spawn_checked, workflow_id_of, HandleChange,
};

/// The subject of a failed start: the core operation, as the Python
/// bridge names it.
const START_SUBJECT: &str = "start_workflow";

/// The rule that a start or a resume on a handle with no child spec source
/// breaks.
const SOURCE_RULE: &str = "the workflows area registers the child spec source of a handle \
                           (registerChildSpecSource) before its first start or resume";

/// A spec that the core read from spec data. JavaScript gets it from
/// `prepareClientSpec`; it has no constructor.
#[napi]
#[derive(Clone, Debug)]
pub struct NativeClientSpec {
    spec: Arc<ClientSpec>,
}

#[napi]
impl NativeClientSpec {
    /// The workflow name of the spec. Pure.
    #[napi(ts_return_type = "NativeEnvelope<string>")]
    pub fn workflow_name(&self) -> Envelope<String> {
        sync_call("workflow_name", || {
            Envelope::Value(self.spec.name().to_owned())
        })
    }
}

/// The outcome of the core's parse of a spec data text.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug)]
pub enum ClientSpecPrepare {
    Prepared {
        spec: NativeClientSpec,
    },
    /// The field path of the text (`nodes[2].join.min_success`, `.` for the
    /// document) and the cause; never a value of the text.
    Refused {
        field_path: String,
        cause: String,
    },
}

/// Parse `specDataText` into a spec, or give the refusal. Starts nothing;
/// the fork and stop rules apply, as for every core call.
#[napi(ts_return_type = "NativeEnvelope<ClientSpecPrepare>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn prepare_client_spec(spec_data_text: String) -> Envelope<ClientSpecPrepare> {
    sync_call("prepare_client_spec", || {
        if let Some(refusal) = pure_call_refusal() {
            return Envelope::Thrown(refusal.thrown());
        }
        Envelope::Value(prepared_spec(&spec_data_text))
    })
}

/// The spec of a spec data text, or the core's refusal. Pure.
fn prepared_spec(spec_data_text: &str) -> ClientSpecPrepare {
    match parse_workflow_spec_data(spec_data_text) {
        Ok(spec) => ClientSpecPrepare::Prepared {
            spec: NativeClientSpec {
                spec: Arc::new(spec),
            },
        },
        Err(refused) => ClientSpecPrepare::Refused {
            field_path: refused.field_path,
            cause: refused.cause,
        },
    }
}

/// Why a start returns the run that holds its id.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExistingRunName {
    /// The run holds the start command of this start.
    SameCommand,
    /// A run of this name with no stored start command.
    FingerprintNotStored,
}

impl ExistingRunName {
    /// The name of the core's reason. Pure.
    fn of(existing: ExistingRun) -> Self {
        match existing {
            ExistingRun::SameCommand => Self::SameCommand,
            ExistingRun::FingerprintNotStored => Self::FingerprintNotStored,
        }
    }
}

/// The outcome of one workflow start: the tree was written, a run with the
/// id exists and the start returns it (nothing written), or a value of the
/// workflow start catalog.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowStartOutcome {
    Started,
    Exists { existing: ExistingRunName },
    Failed { error: RawCatalogError },
}

/// One checked start: the spec, the id, the start call time, the trace,
/// the resend rule, and the handle's child spec source.
struct StartRequest {
    spec: Arc<ClientSpec>,
    workflow_id: Uuid,
    sent_at: DateTime<Utc>,
    trace: SendTrace,
    resend_on_transient_err: bool,
    source: Arc<dyn ChildSpecSource>,
}

/// Start `spec` as the workflow `workflowId` on `handle`. A boundary value
/// that breaks a rule, or a handle with no child spec source, resolves with
/// `CLIENT_DEFECT` at once.
#[napi(ts_return_type = "Promise<NativeEnvelope<WorkflowStartOutcome>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn start_workflow<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    spec: &NativeClientSpec,
    workflow_id: String,
    sent_at_micros: BigInt,
    trace: TraceArgument,
    resend_on_transient_err: bool,
) -> napi::Result<Object<'env>> {
    let start = StartCall {
        spec,
        workflow_id,
        sent_at_micros,
        trace,
        resend_on_transient_err,
    };
    start_workflow_on(env, ProducerHandle::of_process(handle), handle, start)
}

/// The values of one `startWorkflow` call.
pub struct StartCall<'call> {
    pub spec: &'call NativeClientSpec,
    pub workflow_id: String,
    pub sent_at_micros: BigInt,
    pub trace: TraceArgument,
    pub resend_on_transient_err: bool,
}

/// `startWorkflow` through `producer`, with the child spec source of
/// `handle`.
pub fn start_workflow_on<'env>(
    env: &'env Env,
    producer: ProducerHandle,
    handle: &CoreHandle,
    call: StartCall<'_>,
) -> napi::Result<Object<'env>> {
    let StartCall {
        spec,
        workflow_id,
        sent_at_micros,
        trace,
        resend_on_transient_err,
    } = call;
    spawn_checked(
        env,
        START_SUBJECT,
        CallKind::Write,
        producer,
        || -> Result<StartRequest, NativeThrown> {
            Ok(StartRequest {
                spec: Arc::clone(&spec.spec),
                workflow_id: workflow_id_of(&workflow_id)?,
                sent_at: instant_of_epoch_micros("sentAtMicros", &sent_at_micros)?,
                trace: trace.send_trace()?,
                resend_on_transient_err,
                source: registered_source(handle, "startWorkflow")?,
            })
        },
        start_operation,
    )
}

/// The child spec source of `handle`, or `CLIENT_DEFECT` of `subject` when
/// none is registered.
fn registered_source(
    handle: &CoreHandle,
    subject: &str,
) -> Result<Arc<dyn ChildSpecSource>, NativeThrown> {
    handle
        .child_spec_source()
        .ok_or_else(|| NativeThrown::ClientDefect {
            subject: subject.to_owned(),
            rule: SOURCE_RULE.to_owned(),
        })
}

/// One start through the handle of this process (in a task child, the child
/// host's producer handle), with the child spec source of the given handle:
/// the schema install of the handle, then the core's start.
async fn start_operation(producer: ProducerHandle, request: StartRequest) -> WorkflowStartOutcome {
    let StartRequest {
        spec,
        workflow_id,
        sent_at,
        trace,
        resend_on_transient_err,
        source,
    } = request;
    let workflow_name = spec.name();
    let broker = match producer.lazy_handle().initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return failed(&WorkflowStartError::start_failed(
                workflow_name,
                workflow_id,
                ClientStartError::SchemaInstall(error),
            ))
        }
    };
    let answer = broker
        .start_client_workflow(
            &spec,
            workflow_id,
            sent_at,
            trace,
            resend_on_transient_err,
            &*source,
        )
        .await;
    start_outcome(workflow_name, workflow_id, answer)
}

/// The outcome of the core's answer to the start `workflow_id` of
/// `workflow_name` (the Rust client's map). Pure.
///
/// Panics: an error that a start never gives (`WorkflowNotFound`,
/// `InvalidStatus`), a defect of the core; the operation guard gives
/// `CORE_PANIC`.
fn start_outcome(
    workflow_name: &str,
    workflow_id: Uuid,
    answer: Result<ClientStartOutcome, ClientStartError>,
) -> WorkflowStartOutcome {
    match answer {
        Ok(ClientStartOutcome::Started) => WorkflowStartOutcome::Started,
        Ok(ClientStartOutcome::Exists(existing)) => WorkflowStartOutcome::Exists {
            existing: ExistingRunName::of(existing),
        },
        Ok(ClientStartOutcome::IdHeld(holder)) => failed(&WorkflowStartError::id_held(
            workflow_name,
            workflow_id,
            holder,
        )),
        Err(error) => failed(&start_error(error, workflow_name, workflow_id)),
    }
}

/// The start error of the failed start `workflow_id` of `workflow_name`: a
/// spec or a child spec that the core refuses is `VALIDATION_FAILED`; the
/// schema install, the database, a step contract and a child build that
/// gave no answer are `ENQUEUE_FAILED`, with the retry rule of the cause.
/// Pure.
///
/// Panics: an error that a start never gives (a defect of the core).
fn start_error(
    error: ClientStartError,
    workflow_name: &str,
    workflow_id: Uuid,
) -> WorkflowStartError {
    match error {
        ClientStartError::Start(
            refused @ (WorkflowError::Serialization(_) | WorkflowError::Validation(_)),
        ) => WorkflowStartError::ValidationFailed {
            message: refused.to_string(),
            workflow_name: workflow_name.to_owned(),
            workflow_id,
            cause: StartRefusalCause::Error(Box::new(refused)),
        },
        start_failure @ (ClientStartError::SchemaInstall(_)
        | ClientStartError::Start(
            WorkflowError::Broker(_)
            | WorkflowError::StepContract(_)
            | WorkflowError::ChildSpecSource(_),
        )) => WorkflowStartError::start_failed(workflow_name, workflow_id, start_failure),
        ClientStartError::Start(
            ref unexpected @ (WorkflowError::WorkflowNotFound { .. }
            | WorkflowError::InvalidStatus(_)),
        ) => panic!(
            "the start of workflow {workflow_id} returned an error outside the start catalog \
             (a defect of the core): {unexpected}"
        ),
    }
}

/// The outcome of a start error. Pure.
fn failed(error: &WorkflowStartError) -> WorkflowStartOutcome {
    WorkflowStartOutcome::Failed {
        error: RawCatalogError::of(START_SUBJECT, error),
    }
}

/// One checked resume: the workflow id and the handle's child spec source.
struct ResumeRequest {
    workflow_id: Uuid,
    source: Arc<dyn ChildSpecSource>,
}

/// Resume the PAUSED workflow `workflowId`: `changed: true` when it
/// resumed, `false` when it is not PAUSED. The children that the resume owes
/// build through the handle's child spec source. A workflow id that breaks
/// a boundary rule, or a handle with no child spec source, resolves with
/// `CLIENT_DEFECT` at once.
#[napi(ts_return_type = "Promise<NativeEnvelope<HandleChange>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn resume_workflow<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Resume.subject(),
        CallKind::Write,
        ProducerHandle::of_process(handle),
        || -> Result<ResumeRequest, NativeThrown> {
            Ok(ResumeRequest {
                workflow_id: workflow_id_of(&workflow_id)?,
                source: registered_source(handle, "resumeWorkflow")?,
            })
        },
        resume_operation,
    )
}

/// One resume: the schema install of the handle, then the core's resume.
async fn resume_operation(producer: ProducerHandle, request: ResumeRequest) -> HandleChange {
    let operation = HandleOperation::Resume;
    let resumed = match broker_for(&producer, &operation, request.workflow_id).await {
        Ok(broker) => {
            broker
                .resume_client_workflow(request.workflow_id, &*request.source)
                .await
        }
        Err(error) => Err(error),
    };
    change_of(operation.subject(), resumed)
}

#[cfg(test)]
mod tests {
    use horsies_core::error_catalog::{ErrorClassValues, SlotData};
    use horsies_core::{BrokerError, ChildSpecUnavailable, IdHolder};

    use super::*;

    /// The workflow name of the starts of these tests.
    const NAME: &str = "nightly";

    fn id() -> Uuid {
        Uuid::from_u128(5)
    }

    /// The vectors and the refused texts of the core's spec data vectors.
    fn spec_data_vectors() -> serde_json::Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../core/tests/fixtures/stored_forms/workflow_spec_data.json");
        let file = std::fs::read_to_string(&path).expect("the spec data vectors are readable");
        serde_json::from_str(&file).expect("the spec data vectors are JSON")
    }

    /// The class of a failed start, or the kind of another outcome.
    fn class_of(outcome: &WorkflowStartOutcome) -> &str {
        match outcome {
            WorkflowStartOutcome::Failed { error } => error.class.as_str(),
            WorkflowStartOutcome::Started => "started",
            WorkflowStartOutcome::Exists { .. } => "exists",
        }
    }

    /// The `retryable` flag of a start error.
    fn retryable(error: &WorkflowStartError) -> Option<bool> {
        error
            .slot_values()
            .into_iter()
            .find(|slot| slot.name == "retryable")
            .and_then(|slot| match slot.value {
                SlotData::Bool(flag) => Some(flag),
                _other => None,
            })
    }

    #[test]
    fn a_vector_text_is_prepared_with_its_workflow_name() {
        let vectors = spec_data_vectors();
        let vector = &vectors["vectors"][0];
        let text = vector["text"].as_str().expect("a vector has a text");
        let ClientSpecPrepare::Prepared { spec } = prepared_spec(text) else {
            panic!("the core reads the vector text");
        };
        assert_eq!(
            spec.workflow_name(),
            Envelope::Value(
                vector["document"]["name"]
                    .as_str()
                    .expect("a vector names its workflow")
                    .to_owned()
            )
        );
    }

    #[test]
    fn a_refused_text_gives_its_field_path_and_a_cause() {
        let vectors = spec_data_vectors();
        let refused = &vectors["refused_texts"][0];
        let text = refused["text"].as_str().expect("a refused text");
        let ClientSpecPrepare::Refused { field_path, cause } = prepared_spec(text) else {
            panic!("the core refuses the text");
        };
        assert_eq!(
            field_path,
            refused["core_field_path"]
                .as_str()
                .expect("a refused text names its field path")
        );
        assert!(!cause.is_empty());
    }

    #[test]
    fn a_written_tree_is_started_and_an_existing_run_names_its_reason() {
        assert_eq!(
            start_outcome(NAME, id(), Ok(ClientStartOutcome::Started)),
            WorkflowStartOutcome::Started
        );
        for (existing, name) in [
            (ExistingRun::SameCommand, ExistingRunName::SameCommand),
            (
                ExistingRun::FingerprintNotStored,
                ExistingRunName::FingerprintNotStored,
            ),
        ] {
            assert_eq!(
                start_outcome(NAME, id(), Ok(ClientStartOutcome::Exists(existing))),
                WorkflowStartOutcome::Exists { existing: name }
            );
        }
    }

    #[test]
    fn an_id_of_another_name_is_an_id_conflict_and_another_command_a_command_conflict() {
        for (holder, class) in [
            (
                IdHolder::OtherWorkflowName {
                    existing_name: "other".to_owned(),
                },
                "WorkflowStartIdConflict",
            ),
            (IdHolder::OtherCommand, "WorkflowStartCommandConflict"),
            (IdHolder::SubWorkflow, "WorkflowStartCommandConflict"),
        ] {
            let outcome = start_outcome(NAME, id(), Ok(ClientStartOutcome::IdHeld(holder)));
            assert_eq!(class_of(&outcome), class);
        }
    }

    #[test]
    fn a_refused_spec_is_validation_failed() {
        let undecodable = serde_json::from_str::<serde_json::Value>("{")
            .expect_err("an open object does not parse");
        for refused in [
            WorkflowError::Validation("no queue class".to_owned()),
            WorkflowError::Serialization(undecodable),
        ] {
            let outcome = start_outcome(NAME, id(), Err(ClientStartError::Start(refused)));
            assert_eq!(class_of(&outcome), "WorkflowStartValidationFailed");
        }
    }

    #[test]
    fn an_install_the_database_and_a_child_build_are_enqueue_failed_with_the_core_rule() {
        let not_run = ChildSpecUnavailable::NotRun {
            subject: "build of the child of nightly:0".to_owned(),
            cause: "the JavaScript environment is closing".to_owned(),
        };
        let raised = ChildSpecUnavailable::Raised {
            subject: "build of the child of nightly:0".to_owned(),
            cause: "the build wrapper threw".to_owned(),
        };
        for (error, retry) in [
            (
                ClientStartError::SchemaInstall(BrokerError::ListenerClosed),
                true,
            ),
            (
                ClientStartError::Start(WorkflowError::Broker(BrokerError::ListenerClosed)),
                true,
            ),
            (
                ClientStartError::Start(WorkflowError::ChildSpecSource(not_run)),
                true,
            ),
            (
                ClientStartError::Start(WorkflowError::ChildSpecSource(raised)),
                false,
            ),
        ] {
            let mapped = start_error(error, NAME, id());
            assert_eq!(retryable(&mapped), Some(retry), "{mapped}");
            assert_eq!(
                class_of(&failed(&mapped)),
                "WorkflowStartEnqueueFailed",
                "{mapped}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "outside the start catalog")]
    fn an_error_that_a_start_never_gives_is_a_defect() {
        let _never = start_error(
            ClientStartError::Start(WorkflowError::WorkflowNotFound { workflow_id: id() }),
            NAME,
            id(),
        );
    }
}
