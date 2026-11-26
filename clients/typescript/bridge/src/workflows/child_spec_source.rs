//! The child spec source of a core handle: the core asks it for the spec of
//! a sub-workflow child inside a workflow step, and it runs the package's
//! build function on the JavaScript thread through the callback primitive
//! (`crate::call::callback`).
//!
//! One build, in order (the Rust client's `child_spec_answer`):
//! 1. A node with no definition key, or with a key that the app does not
//!    list (the keys are given at the registration), fails with
//!    `SUBWORKFLOW_LOAD_FAILED`; no input is read and no call goes into
//!    JavaScript. Both clients look the definition up before the input.
//! 2. The core merges the node input with its `args_from` envelopes
//!    (`merged_child_input`). A node failure of the merge fails the node; a
//!    refusal of the merge is `Raised`.
//! 3. The merged text passes the core's strict JSON check
//!    (`check_json_text`), because the package's reader trusts only checked
//!    text. A refused text fails the node with `WORKFLOW_ENQUEUE_FAILED`.
//! 4. The call into JavaScript in the take form: the wait for the
//!    JavaScript thread to take the call is at most [`CHILD_SPEC_WAIT_BOUND`].
//!    Past the bound the answer is `NotRun` (retryable): the step rolls back
//!    and runs again through its retry or the workflow recovery. The drop of
//!    the call at the bound removes its entries, so a late take finds no call
//!    and the wrapper does not run the build. Once JavaScript takes the call,
//!    the wait for the answer has no bound of its own, as in the Rust and
//!    Python clients: a build that blocks the JavaScript thread is the
//!    child's loop watch's case (`LOOP_BLOCK_LIMIT`, `loop_blocked`).
//! 5. The answer: a spec that the core parses is the child; every other
//!    answer fails the node with the core's error of its case.
//!
//! The package's wrapper never throws. A throw, an answer that does not
//! read, a refused Node-API call, a call on the JavaScript thread, and spec
//! data that the core refuses are defects of the package: each is `Raised`
//! (not retryable) with the subject and the cause, and one ERROR record. The
//! step rolls back, and a start reports `ENQUEUE_FAILED`.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use horsies_core::catalog::CodeEnum;
use horsies_core::core::codec::check_json_text;
use horsies_core::frame_protocol::LOOP_BLOCK_LIMIT;
use horsies_core::{
    build_failed_error, merged_child_input, node_failure_result, params_decode_failed_error,
    params_refused_error, parse_workflow_spec_data, subworkflow_load_failed_error, ChildBuildInput,
    ChildSpecError, ChildSpecFuture, ChildSpecRequest, ChildSpecSource, ChildSpecUnavailable,
    ClientChildSpec, ErrorCode, HorsiesError, NodeInput, RefusedMember, TaskError,
};
use napi::bindgen_prelude::{BigInt, FnArgs, Function};
use napi_derive::napi;

use crate::call::callback::{CallbackFailure, JsCallback, NotRunCause};
use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::{ChildSpecSourceSet, CoreHandle};

/// Most build calls of one source that wait for the JavaScript thread. A
/// call past it is `NotRun` (retryable). It is the queue bound of the
/// Python bridge's callback thread.
pub const CHILD_SPEC_QUEUE_BOUND: usize = 1_024;

/// The longest wait for the JavaScript thread to take the call of one build.
/// A JavaScript loop that does not take the call within it is blocked by the
/// core's rule. The run of the build after the take has no bound here: the
/// child's loop watch bounds a blocked loop (`LOOP_BLOCK_LIMIT`,
/// `loop_blocked`).
pub const CHILD_SPEC_WAIT_BOUND: Duration = LOOP_BLOCK_LIMIT;

/// The name of the build function of a TypeScript workflow, as the node
/// error of a refused `args_from` member names it.
const BUILD_HOOK: &str = "build";

/// The facts of one child build that the core asks for. `inputText` passed
/// the core's strict JSON check.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildSpecCall {
    /// The definition key of the sub-workflow node.
    pub definition_key: String,
    /// The node's params with its `args_from` envelopes merged.
    pub input_text: String,
}

/// The code of a build refusal: a catalog code (`HRS-nnn`), or none.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildRefusalCode {
    Catalog { code: String },
    NoCode,
}

/// The answer of the package's build function to one call.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChildSpecAnswer {
    /// The spec data text of the child.
    Built { spec_data: String },
    /// The app lists no workflow with the definition key.
    NotListed,
    /// A member of the input does not decode, or no param takes it.
    MemberRefused { key: String, cause: String },
    /// The input does not decode as the params as a whole.
    ParamsUndecodable { cause: String },
    /// The build threw, returned a thenable, or its spec failed a check.
    BuildFailed {
        code: BuildRefusalCode,
        message: String,
    },
}

/// How a registration ended.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceRegistration {
    Registered,
    /// The handle holds a source; the new one is dropped.
    AlreadySet,
}

/// The callback of the package's build function.
type ChildSpecCallback = JsCallback<ChildSpecCall, ChildSpecAnswer, CHILD_SPEC_QUEUE_BOUND>;

/// The definition keys of the workflows that the app lists, fixed at the
/// registration of the source.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListedKeys(HashSet<String>);

impl ListedKeys {
    /// The set of `keys`. Pure.
    pub fn of(keys: Vec<String>) -> Self {
        Self(keys.into_iter().collect())
    }

    /// Whether the app lists a workflow with `key`. Pure.
    fn holds(&self, key: &str) -> bool {
        self.0.contains(key)
    }
}

/// The registration of the package's build function: the child spec source
/// of one core handle.
pub struct JavaScriptChildSpecSource {
    callback: ChildSpecCallback,
    listed: ListedKeys,
}

/// Set `source` as the child spec source of `handle`, once, with the
/// definition keys of the workflows that the app lists. On the JavaScript
/// thread; starts nothing. The registration is weak: it does not keep the
/// event loop alive.
#[napi(ts_return_type = "NativeEnvelope<SourceRegistration>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn register_child_spec_source(
    handle: &CoreHandle,
    source: Function<FnArgs<(BigInt, ChildSpecCall)>, ()>,
    definition_keys: Vec<String>,
) -> Envelope<SourceRegistration> {
    sync_call("register_child_spec_source", || {
        let callback = match ChildSpecCallback::register(&source) {
            Ok(callback) => callback,
            Err(error) => {
                return Envelope::Thrown(NativeThrown::ClientDefect {
                    subject: "registerChildSpecSource".to_owned(),
                    rule: format!(
                        "Node-API refused the threadsafe function of the child spec source: \
                         {error}"
                    ),
                })
            }
        };
        let registered = Arc::new(JavaScriptChildSpecSource {
            callback,
            listed: ListedKeys::of(definition_keys),
        });
        Envelope::Value(match handle.set_child_spec_source(registered) {
            Ok(()) => SourceRegistration::Registered,
            Err(ChildSpecSourceSet) => SourceRegistration::AlreadySet,
        })
    })
}

impl ChildSpecSource for JavaScriptChildSpecSource {
    fn child_spec(&self, request: ChildSpecRequest) -> ChildSpecFuture<'_> {
        Box::pin(self.answer(request))
    }
}

impl JavaScriptChildSpecSource {
    /// The answer to `request`: steps 1 to 3 here, then the call into
    /// JavaScript, taken within [`CHILD_SPEC_WAIT_BOUND`], then its answer.
    async fn answer(
        &self,
        request: ChildSpecRequest,
    ) -> Result<ClientChildSpec, ChildSpecUnavailable> {
        let (call, input) = match build_step(&request, &self.listed) {
            BuildStep::Answered(answer) => return answer,
            BuildStep::Call { call, input } => (call, input),
        };
        let end = match self.callback.start_call(call) {
            Ok((taken, answer)) => {
                call_end(taken.wait(), answer.wait(), CHILD_SPEC_WAIT_BOUND).await
            }
            Err(failure) => CallEnd::Failed(failure),
        };
        spec_of_call(&request, &input, end)
    }
}

/// How a call of the take form ended: `NotTaken` when the JavaScript thread
/// did not take it within `take_bound`; else its answer or its failure, with
/// no bound on the wait for the answer. The drop of `answer` at the bound
/// ends the call.
async fn call_end(
    taken: impl Future<Output = ()>,
    answer: impl Future<Output = Result<ChildSpecAnswer, CallbackFailure>>,
    take_bound: Duration,
) -> CallEnd {
    match tokio::time::timeout(take_bound, taken).await {
        Err(_not_taken) => CallEnd::NotTaken,
        Ok(()) => match answer.await {
            Ok(answer) => CallEnd::Answered(answer),
            Err(failure) => CallEnd::Failed(failure),
        },
    }
}

/// What a build does before any call into JavaScript.
enum BuildStep {
    /// The node fails, or the build cannot run: the answer, with no call.
    Answered(Result<ClientChildSpec, ChildSpecUnavailable>),
    /// The call, and the merged input that the answer map reads.
    Call {
        call: ChildSpecCall,
        input: ChildBuildInput,
    },
}

/// How the wait for the answer of one call ended.
enum CallEnd {
    Answered(ChildSpecAnswer),
    Failed(CallbackFailure),
    /// The JavaScript thread did not take the call within
    /// [`CHILD_SPEC_WAIT_BOUND`].
    NotTaken,
}

/// Steps 1 to 3 of a build: the definition key and its listing in
/// `listed`, the merge, and the strict check of the merged text. Pure.
fn build_step(request: &ChildSpecRequest, listed: &ListedKeys) -> BuildStep {
    let Some(definition_key) = request.definition_key.as_deref() else {
        return BuildStep::Answered(node_fails(
            request,
            subworkflow_load_failed_error(&request.parent_workflow_name, request.task_index, None),
        ));
    };
    if !listed.holds(definition_key) {
        return BuildStep::Answered(node_fails(
            request,
            subworkflow_load_failed_error(
                &request.parent_workflow_name,
                request.task_index,
                Some(definition_key),
            ),
        ));
    }
    let input = match merged_child_input(request) {
        Ok(NodeInput::Input(input)) => input,
        Ok(NodeInput::NodeFails(error)) => return BuildStep::Answered(node_fails(request, error)),
        Err(refused) => {
            return BuildStep::Answered(Err(build_raised(request, refused.to_string())))
        }
    };
    if let Err(error) = strict_input(request, input.text()) {
        return BuildStep::Answered(node_fails(request, error));
    }
    BuildStep::Call {
        call: ChildSpecCall {
            definition_key: definition_key.to_owned(),
            input_text: input.text().to_owned(),
        },
        input,
    }
}

/// `Ok` for a merged input text that passes the core's strict JSON check;
/// else the node error `WORKFLOW_ENQUEUE_FAILED` with the refusal. Pure.
fn strict_input(request: &ChildSpecRequest, text: &str) -> Result<(), TaskError> {
    check_json_text(text).map_err(|refused| {
        params_decode_failed_error(
            request.parent_workflow_id,
            &request.parent_workflow_name,
            request.task_index,
            &format!("the merged input does not pass the strict JSON read: {refused}"),
        )
    })
}

/// The answer of a build from the end of its call. Pure apart from the
/// ERROR record of a defect.
fn spec_of_call(
    request: &ChildSpecRequest,
    input: &ChildBuildInput,
    end: CallEnd,
) -> Result<ClientChildSpec, ChildSpecUnavailable> {
    match end {
        CallEnd::Answered(answer) => spec_of_answer(request, input, answer),
        CallEnd::Failed(failure) => Err(unavailable_of(request, failure)),
        CallEnd::NotTaken => Err(ChildSpecUnavailable::NotRun {
            subject: subject_of(request),
            cause: format!(
                "the JavaScript thread did not take the build call within {} ms",
                CHILD_SPEC_WAIT_BOUND.as_millis()
            ),
        }),
    }
}

/// Why a call into JavaScript gave no answer, as the core names it: a call
/// that did not run is `NotRun` (retryable); every other failure is a
/// defect of the package. Pure apart from the ERROR record of a defect.
fn unavailable_of(request: &ChildSpecRequest, failure: CallbackFailure) -> ChildSpecUnavailable {
    match failure {
        CallbackFailure::NotRun { cause } => ChildSpecUnavailable::NotRun {
            subject: subject_of(request),
            cause: not_run_text(cause).to_owned(),
        },
        CallbackFailure::Threw {
            call_id,
            name,
            message,
        } => package_defect(
            request,
            format!("the build wrapper of call {call_id} threw {name}: {message}"),
        ),
        CallbackFailure::Panicked { cause } => {
            package_defect(request, format!("the read of the answer panicked: {cause}"))
        }
        CallbackFailure::AnswerRefused { cause } => package_defect(
            request,
            format!("the answer does not read as a child spec answer: {cause}"),
        ),
        CallbackFailure::NodeApiRefused { status } => {
            package_defect(request, format!("Node-API refused the call with {status}"))
        }
        CallbackFailure::OnJavaScriptThread => package_defect(
            request,
            "the core asked for the build on the JavaScript thread, which must run it".to_owned(),
        ),
    }
}

/// The text of a call that did not run. Pure.
fn not_run_text(cause: NotRunCause) -> &'static str {
    match cause {
        NotRunCause::QueueFull => "the queue of build calls into JavaScript is full",
        NotRunCause::EnvClosing => "the JavaScript environment is closing",
    }
}

/// The answer of a build from the package's answer (step 5). Pure apart
/// from the ERROR record of a defect.
fn spec_of_answer(
    request: &ChildSpecRequest,
    input: &ChildBuildInput,
    answer: ChildSpecAnswer,
) -> Result<ClientChildSpec, ChildSpecUnavailable> {
    let workflow_id = request.parent_workflow_id;
    let workflow_name = request.parent_workflow_name.as_str();
    let task_index = request.task_index;
    let error = match answer {
        ChildSpecAnswer::Built { spec_data } => {
            return match parse_workflow_spec_data(&spec_data) {
                Ok(spec) => Ok(ClientChildSpec::Built(spec)),
                Err(refused) => Err(package_defect(
                    request,
                    format!("the core refuses the built spec data: {refused}"),
                )),
            }
        }
        ChildSpecAnswer::NotListed => subworkflow_load_failed_error(
            workflow_name,
            task_index,
            request.definition_key.as_deref(),
        ),
        ChildSpecAnswer::MemberRefused { key, cause } => params_refused_error(
            workflow_id,
            task_index,
            RefusedMember {
                key: &key,
                cause: &cause,
            },
            input,
            BUILD_HOOK,
        ),
        ChildSpecAnswer::ParamsUndecodable { cause } => {
            params_decode_failed_error(workflow_id, workflow_name, task_index, &cause)
        }
        ChildSpecAnswer::BuildFailed { code, message } => match build_error_of(code, message) {
            Ok(error) => build_failed_error(
                workflow_id,
                workflow_name,
                task_index,
                &ChildSpecError::of_builder(&error),
            ),
            Err(unknown) => {
                return Err(package_defect(
                    request,
                    format!(
                        "the build failed with the code {:?}, which is not a code of the core's \
                         catalog",
                        unknown.text
                    ),
                ))
            }
        },
    };
    node_fails(request, error)
}

/// A code text that names no code of the core's catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
struct UnknownErrorCode {
    text: String,
}

/// The core error of a failed build: the message with the catalog code of
/// `code`, or with no code. Pure.
fn build_error_of(
    code: BuildRefusalCode,
    message: String,
) -> Result<HorsiesError, UnknownErrorCode> {
    match code {
        BuildRefusalCode::NoCode => Ok(HorsiesError::new(message)),
        BuildRefusalCode::Catalog { code } => {
            match <ErrorCode as CodeEnum>::VARIANTS
                .iter()
                .find(|known| known.wire_value() == code)
            {
                Some(known) => Ok(HorsiesError::new(message).with_code(*known)),
                None => Err(UnknownErrorCode { text: code }),
            }
        }
    }
}

/// The answer that fails the node of `request` with `error`, stored in its
/// result envelope (the Rust client's `node_fails`). An envelope that does
/// not encode is `Raised`. Pure.
fn node_fails(
    request: &ChildSpecRequest,
    error: TaskError,
) -> Result<ClientChildSpec, ChildSpecUnavailable> {
    let message = error.to_string();
    match node_failure_result(error) {
        Ok(failure_result) => Ok(ClientChildSpec::NodeFails {
            message,
            failure_result,
        }),
        Err(refused) => Err(build_raised(request, refused.to_string())),
    }
}

/// A build of `request` that cannot answer, for `cause`: not retryable.
/// Pure.
fn build_raised(request: &ChildSpecRequest, cause: String) -> ChildSpecUnavailable {
    ChildSpecUnavailable::Raised {
        subject: subject_of(request),
        cause,
    }
}

/// A defect of the package in the build of `request`: `Raised` with the
/// cause, and one ERROR record.
fn package_defect(request: &ChildSpecRequest, cause: String) -> ChildSpecUnavailable {
    let raised = build_raised(request, format!("{cause} (a defect of the package)"));
    tracing::error!("{raised}");
    raised
}

/// The subject of the build of `request`, as the failures name it. Pure.
fn subject_of(request: &ChildSpecRequest) -> String {
    format!(
        "build of the child of {}:{}",
        request.parent_workflow_name, request.task_index
    )
}

#[cfg(test)]
mod tests {
    use horsies_core::{ChildStartSource, SubWorkflowParams};
    use uuid::Uuid;

    use super::*;

    /// The keys of an app that lists only `tests.report.v1`.
    fn listed() -> ListedKeys {
        ListedKeys::of(vec!["tests.report.v1".to_owned()])
    }

    /// The request of the node 2 of `nightly` with the key `definition_key`
    /// and the stored input `node_input`, with no `args_from`.
    fn request(definition_key: Option<&str>, node_input: &str) -> ChildSpecRequest {
        ChildSpecRequest {
            parent_workflow_id: Uuid::from_u128(9),
            parent_workflow_name: "nightly".to_owned(),
            task_index: 2,
            child_depth: 1,
            definition_key: definition_key.map(str::to_owned),
            params: SubWorkflowParams::Given,
            node_input: node_input.to_owned(),
            args_from: None,
            sources: Vec::new(),
        }
    }

    /// A request with a key and the input `{"day":"2026-11-25"}`, and its
    /// merged input.
    fn called() -> (ChildSpecRequest, ChildBuildInput) {
        let request = request(Some("tests.report.v1"), r#"{"day":"2026-11-25"}"#);
        match build_step(&request, &listed()) {
            BuildStep::Call { call, input } => {
                assert_eq!(
                    call,
                    ChildSpecCall {
                        definition_key: "tests.report.v1".to_owned(),
                        input_text: r#"{"day":"2026-11-25"}"#.to_owned(),
                    }
                );
                (request, input)
            }
            BuildStep::Answered(answer) => panic!("the build calls JavaScript, got {answer:?}"),
        }
    }

    /// The message and the stored envelope of a node that fails.
    fn failed_node(answer: Result<ClientChildSpec, ChildSpecUnavailable>) -> (String, String) {
        match answer {
            Ok(ClientChildSpec::NodeFails {
                message,
                failure_result,
            }) => (message, failure_result),
            Ok(ClientChildSpec::Built(spec)) => {
                panic!("the node fails, got the spec {}", spec.name())
            }
            Err(unavailable) => panic!("the node fails, got {unavailable}"),
        }
    }

    /// The cause of a build that raised.
    fn raised_cause(answer: Result<ClientChildSpec, ChildSpecUnavailable>) -> String {
        match answer {
            Err(ChildSpecUnavailable::Raised { subject, cause }) => {
                assert_eq!(subject, "build of the child of nightly:2");
                cause
            }
            Err(other) => panic!("the build raised, got {other}"),
            Ok(answer) => panic!("the build raised, got {answer:?}"),
        }
    }

    /// The text of the first vector of the core's spec data vectors.
    fn vector_spec_data() -> (String, String) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../core/tests/fixtures/stored_forms/workflow_spec_data.json");
        let file = std::fs::read_to_string(&path).expect("the spec data vectors are readable");
        let vectors: serde_json::Value =
            serde_json::from_str(&file).expect("the spec data vectors are JSON");
        let text = vectors["vectors"][0]["text"]
            .as_str()
            .expect("a vector text");
        let refused = vectors["refused_texts"][0]["text"]
            .as_str()
            .expect("a refused text");
        (text.to_owned(), refused.to_owned())
    }

    fn answered(answer: ChildSpecAnswer) -> Result<ClientChildSpec, ChildSpecUnavailable> {
        let (request, input) = called();
        spec_of_call(&request, &input, CallEnd::Answered(answer))
    }

    #[test]
    fn a_node_with_no_definition_key_fails_to_load_with_no_call() {
        let BuildStep::Answered(answer) = build_step(&request(None, "{}"), &listed()) else {
            panic!("a node with no key calls no JavaScript");
        };
        let (message, failure_result) = failed_node(answer);
        assert_eq!(
            message,
            "[SUBWORKFLOW_LOAD_FAILED] Failed to load subworkflow definition for nightly:2"
        );
        assert!(
            failure_result.contains("SUBWORKFLOW_LOAD_FAILED"),
            "{failure_result}"
        );
    }

    #[test]
    fn a_key_that_the_app_does_not_list_fails_to_load_before_the_input_is_read() {
        // A stored input that the merge refuses, and an `args_from` entry
        // that does not encode: the lookup comes first, as in both clients.
        let mut unlisted = request(Some("tests.other.v1"), "");
        unlisted.args_from = Some("not json".to_owned());
        let BuildStep::Answered(answer) = build_step(&unlisted, &listed()) else {
            panic!("a key that the app does not list calls no JavaScript");
        };
        let (message, failure_result) = failed_node(answer);
        assert!(
            message.starts_with("[SUBWORKFLOW_LOAD_FAILED]"),
            "{message}"
        );
        assert!(
            failure_result.contains("tests.other.v1"),
            "{failure_result}"
        );
    }

    #[test]
    fn a_dependency_result_that_repeats_a_key_fails_the_node_with_the_strict_read() {
        let mut merged = request(Some("tests.report.v1"), "{}");
        merged.args_from = Some(r#"{"rows": 0}"#.to_owned());
        merged.sources = vec![ChildStartSource {
            task_index: 0,
            node_id: Some("fetch".to_owned()),
            status: "COMPLETED".to_owned(),
            task_name: "fetch_rows".to_owned(),
            definition_key: None,
            result: Some(r#"{"__h_task_result__":true,"ok":{"a":1,"a":2},"err":null}"#.to_owned()),
        }];
        let BuildStep::Answered(answer) = build_step(&merged, &listed()) else {
            panic!("a merged input that the strict read refuses calls no JavaScript");
        };
        let (message, _envelope) = failed_node(answer);
        assert!(
            message.starts_with("[WORKFLOW_ENQUEUE_FAILED] Subworkflow build failed: "),
            "{message}"
        );
        assert!(message.contains("repeats the key"), "{message}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_that_javascript_does_not_take_ends_at_the_wait_bound() {
        let started = tokio::time::Instant::now();
        let end = call_end(
            std::future::pending(),
            std::future::pending::<Result<ChildSpecAnswer, CallbackFailure>>(),
            CHILD_SPEC_WAIT_BOUND,
        )
        .await;
        assert!(matches!(end, CallEnd::NotTaken));
        // The paused clock jumps to the timer, which rounds up to the next
        // millisecond.
        let waited = started.elapsed();
        assert!(waited >= CHILD_SPEC_WAIT_BOUND, "{waited:?}");
        assert!(
            waited < CHILD_SPEC_WAIT_BOUND + Duration::from_millis(2),
            "{waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_taken_call_waits_for_its_answer_past_the_wait_bound() {
        // The build runs three bounds long after the take: the bound covers
        // only the wait for the take.
        let started = tokio::time::Instant::now();
        let end = call_end(
            async {},
            async {
                tokio::time::sleep(CHILD_SPEC_WAIT_BOUND * 3).await;
                Ok(ChildSpecAnswer::NotListed)
            },
            CHILD_SPEC_WAIT_BOUND,
        )
        .await;
        assert!(matches!(end, CallEnd::Answered(ChildSpecAnswer::NotListed)));
        assert!(started.elapsed() >= CHILD_SPEC_WAIT_BOUND * 3);
    }

    #[test]
    fn a_stored_input_that_the_merge_refuses_fails_the_node() {
        let BuildStep::Answered(answer) =
            build_step(&request(Some("tests.report.v1"), ""), &listed())
        else {
            panic!("an empty stored input calls no JavaScript");
        };
        let (message, _envelope) = failed_node(answer);
        assert_eq!(
            message,
            "[WORKER_SERIALIZATION_ERROR] Corrupt kwargs JSON for subworkflow task 2"
        );
    }

    #[test]
    fn a_merged_input_that_the_strict_read_refuses_fails_the_node() {
        let request = request(Some("tests.report.v1"), "{}");
        let error =
            strict_input(&request, r#"{"day":1,"day":2}"#).expect_err("a repeated key is refused");
        let (message, _envelope) = failed_node(node_fails(&request, error));
        assert!(
            message.starts_with("[WORKFLOW_ENQUEUE_FAILED] Subworkflow build failed: "),
            "{message}"
        );
        assert!(message.contains("repeats the key"), "{message}");
        assert_eq!(
            strict_input(&request, r#"{"day":1}"#).map_err(|error| error.to_string()),
            Ok(())
        );
    }

    #[test]
    fn a_built_spec_that_the_core_reads_is_the_child() {
        let (text, _refused) = vector_spec_data();
        match answered(ChildSpecAnswer::Built { spec_data: text }) {
            Ok(ClientChildSpec::Built(spec)) => assert_eq!(spec.name(), "checkout"),
            Ok(other) => panic!("the child is built, got {other:?}"),
            Err(unavailable) => panic!("the child is built, got {unavailable}"),
        }
    }

    #[test]
    fn built_spec_data_that_the_core_refuses_is_a_defect_of_the_package() {
        let (_text, refused) = vector_spec_data();
        let cause = raised_cause(answered(ChildSpecAnswer::Built { spec_data: refused }));
        assert!(cause.contains("nodes[0].queue"), "{cause}");
        assert!(cause.ends_with("(a defect of the package)"), "{cause}");
    }

    #[test]
    fn a_workflow_that_the_app_does_not_list_fails_to_load() {
        let (message, failure_result) = failed_node(answered(ChildSpecAnswer::NotListed));
        assert!(
            message.starts_with("[SUBWORKFLOW_LOAD_FAILED]"),
            "{message}"
        );
        assert!(
            failure_result.contains("tests.report.v1"),
            "{failure_result}"
        );
    }

    #[test]
    fn a_refused_static_member_is_a_serialization_error_of_the_node() {
        let (message, _envelope) = failed_node(answered(ChildSpecAnswer::MemberRefused {
            key: "day".to_owned(),
            cause: "expected a date text".to_owned(),
        }));
        assert_eq!(
            message,
            "[WORKER_SERIALIZATION_ERROR] Typed decode of subworkflow kwargs failed for task 2: \
             expected a date text"
        );
    }

    #[test]
    fn undecodable_params_are_an_enqueue_failure_of_the_node() {
        let (message, _envelope) = failed_node(answered(ChildSpecAnswer::ParamsUndecodable {
            cause: "the param limit has no member".to_owned(),
        }));
        assert_eq!(
            message,
            "[WORKFLOW_ENQUEUE_FAILED] Subworkflow build failed: the param limit has no member \
             (workflow nightly:2)"
        );
    }

    #[test]
    fn a_failed_build_with_a_catalog_code_names_the_class_of_its_category() {
        let (message, _envelope) = failed_node(answered(ChildSpecAnswer::BuildFailed {
            code: BuildRefusalCode::Catalog {
                code: "HRS-001".to_owned(),
            },
            message: "the workflow has no name".to_owned(),
        }));
        assert_eq!(
            message,
            "[WORKFLOW_ENQUEUE_FAILED] Subworkflow build failed: WorkflowValidationError: \
             error[HRS-001]: the workflow has no name (workflow nightly:2)"
        );
    }

    #[test]
    fn a_failed_build_with_no_code_names_the_base_class() {
        let (message, _envelope) = failed_node(answered(ChildSpecAnswer::BuildFailed {
            code: BuildRefusalCode::NoCode,
            message: "TypeError: x is not a function".to_owned(),
        }));
        assert_eq!(
            message,
            "[WORKFLOW_ENQUEUE_FAILED] Subworkflow build failed: HorsiesError: error: TypeError: \
             x is not a function (workflow nightly:2)"
        );
    }

    #[test]
    fn a_code_outside_the_catalog_is_a_defect_of_the_package() {
        let cause = raised_cause(answered(ChildSpecAnswer::BuildFailed {
            code: BuildRefusalCode::Catalog {
                code: "HRS-999".to_owned(),
            },
            message: "unknown".to_owned(),
        }));
        assert!(cause.contains("\"HRS-999\""), "{cause}");
    }

    #[test]
    fn a_call_that_did_not_run_or_was_not_taken_is_not_run_and_retryable() {
        let (request, input) = called();
        for end in [
            CallEnd::Failed(CallbackFailure::NotRun {
                cause: NotRunCause::QueueFull,
            }),
            CallEnd::Failed(CallbackFailure::NotRun {
                cause: NotRunCause::EnvClosing,
            }),
            CallEnd::NotTaken,
        ] {
            match spec_of_call(&request, &input, end) {
                Err(unavailable @ ChildSpecUnavailable::NotRun { .. }) => {
                    assert!(unavailable.is_retryable());
                }
                Err(other) => panic!("the call did not run, got {other}"),
                Ok(answer) => panic!("the call did not run, got {answer:?}"),
            }
        }
    }

    #[test]
    fn every_other_call_failure_is_a_defect_of_the_package() {
        let (request, input) = called();
        for failure in [
            CallbackFailure::Threw {
                call_id: 4,
                name: "TypeError".to_owned(),
                message: "boom".to_owned(),
            },
            CallbackFailure::Panicked {
                cause: "boom".to_owned(),
            },
            CallbackFailure::AnswerRefused {
                cause: "no kind".to_owned(),
            },
            CallbackFailure::NodeApiRefused {
                status: napi::Status::GenericFailure,
            },
            CallbackFailure::OnJavaScriptThread,
        ] {
            let answer = spec_of_call(&request, &input, CallEnd::Failed(failure));
            let cause = raised_cause(answer);
            assert!(cause.ends_with("(a defect of the package)"), "{cause}");
        }
    }
}
