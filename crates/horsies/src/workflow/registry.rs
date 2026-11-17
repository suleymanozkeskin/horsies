//! The workflow registry of an app: the definitions that sub-workflow nodes
//! and starts name, by definition key, and the child builds that the core's
//! workflow steps ask of it.
//!
//! A definition has params or has none. One without params has one spec,
//! built and checked when it registers. One with params has a build: decode
//! the params text with the params type, run `build_with`, check the output
//! type. The core calls the build for each sub-workflow node of the
//! definition (with the node's params, its `args_from` members merged), and
//! a start with params calls it.
//!
//! The registry is the child spec source of the core's workflow steps
//! ([`ChildSpecSource`]): for a sub-workflow node, it loads the definition
//! by its key, takes the builder input that the core merges
//! ([`merged_child_input`]), runs the build, and answers the child's spec
//! data, or the node's failure with Python's error envelope. The core
//! refuses no recursive definition: the depth limit of 32 stops a nesting at
//! run time, and [`WorkflowRegistry::check_findings`] reports a nesting that
//! reaches it for certain (Python's nesting check).

use std::any::TypeId;
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use horsies_core::{
    build_failed_error, merged_child_input, node_failure_result, params_decode_failed_error,
    params_refused_error, subworkflow_load_failed_error, ChildSpecError, ChildSpecFuture,
    ChildSpecRequest, ChildSpecSource, ChildSpecUnavailable, ClientChildSpec, ClientSpec,
    NodeInput, RefusedMember, TaskError,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::definition::WorkflowDefinition;
use super::node::unread_node_member;
use super::spec::{SpecBody, SpecNode, WorkflowSpec};
use super::spec_data::client_spec;
use super::sub_workflow::NodeParams;
use crate::task::body::{decode_checked, DecodeFailure};
use crate::{AppConfig, ErrorCode, HorsiesError, ValidationReport};

/// The hook that builds a child from params, as the error texts of a child
/// start name it: the definition's `build_with`.
const BUILD_WITH: &str = "build_with";

/// The deepest child workflow (Python `MAX_SUBWORKFLOW_DEPTH`); the core
/// fails a node whose child would be deeper.
const MAX_SUBWORKFLOW_DEPTH: usize = 32;

/// The params text of a sub-workflow node that gives no params.
const NO_PARAMS_TEXT: &str = "{}";

/// The text from which `()` params decode: a params text with no member.
const UNIT_PARAMS_TEXT: &str = "null";

/// The build of a definition with params: its spec from the params text
/// (first argument) and the names of the members of that text that hold
/// `args_from` results (second argument).
pub(crate) type ParamsBuild = Arc<
    dyn Fn(&str, &[String]) -> Result<WorkflowSpec, ParamsBuildRefused> + Send + Sync + 'static,
>;

/// A check case of a workflow builder: one spec.
pub(crate) type CaseBuild =
    Arc<dyn Fn() -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static>;

/// Why the build of a definition with params gave no spec.
#[derive(Debug, Clone)]
pub(crate) enum ParamsBuildRefused {
    /// The params member `key` does not decode to the field of that name, or
    /// no field takes it; `build_with` did not run. The node stores
    /// `WORKER_SERIALIZATION_ERROR`.
    Member { key: String, cause: String },
    /// The params do not decode as a whole (a field with no member, for
    /// one); `build_with` did not run. `text` is the decode's text. The node
    /// stores `WORKFLOW_ENQUEUE_FAILED` with the text after Python's prefix
    /// `Subworkflow build failed: `.
    Undecodable { text: String },
    /// `build_with` refused, or the spec it built has another output type.
    /// The node stores `WORKFLOW_ENQUEUE_FAILED`.
    Failed(HorsiesError),
}

/// What a definition builds.
#[derive(Clone)]
pub(crate) enum DefinitionKind {
    /// No params: the one spec.
    WithoutParams(Arc<WorkflowSpec>),
    /// Params: the build.
    WithParams(ParamsBuild),
}

/// A registered definition.
#[derive(Clone)]
pub(crate) struct Definition {
    pub(crate) name: String,
    pub(crate) definition_key: String,
    pub(crate) kind: DefinitionKind,
}

impl Definition {
    /// The definition `D` without params: its spec, built and checked now.
    ///
    /// Errors: the refusals of `D::build`.
    pub(crate) fn without_params<D: WorkflowDefinition>() -> Result<Self, HorsiesError> {
        Ok(Self {
            name: D::name().to_owned(),
            definition_key: D::definition_key().to_owned(),
            kind: DefinitionKind::WithoutParams(Arc::new(D::build()?)),
        })
    }

    /// The definition `D` with params: `D::build_with` over the params
    /// text, with the output type checked.
    pub(crate) fn with_params<D: WorkflowDefinition>() -> Self {
        let build: ParamsBuild = Arc::new(
            |text: &str, result_members: &[String]| -> Result<WorkflowSpec, ParamsBuildRefused> {
                let params = decode_params::<D::Params>(text, result_members)?;
                let spec =
                    run_user_build(|| D::build_with(params)).map_err(ParamsBuildRefused::Failed)?;
                spec.check_output_type::<D::Output>()
                    .map_err(ParamsBuildRefused::Failed)?;
                Ok(spec)
            },
        );
        Self {
            name: D::name().to_owned(),
            definition_key: D::definition_key().to_owned(),
            kind: DefinitionKind::WithParams(build),
        }
    }
}

/// A check case: the builder name, and the build of its spec.
#[derive(Clone)]
pub(crate) struct CheckCase {
    pub(crate) builder_name: String,
    pub(crate) build: CaseBuild,
}

/// The registered definitions and check cases.
struct Registered {
    /// The definitions by definition key.
    definitions: BTreeMap<String, Definition>,
    check_cases: Vec<CheckCase>,
}

/// The workflow registry of one app. Shared by the app, its workflow
/// functions and its task runtime, so a function registered early sees a
/// definition registered later.
pub(crate) struct WorkflowRegistry {
    config: AppConfig,
    registered: RwLock<Registered>,
}

impl std::fmt::Debug for WorkflowRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowRegistry")
            .field("definitions", &self.read().definitions.len())
            .finish_non_exhaustive()
    }
}

impl WorkflowRegistry {
    /// An empty registry for the app of `config`.
    pub(crate) fn new(config: AppConfig) -> Self {
        Self {
            config,
            registered: RwLock::new(Registered {
                definitions: BTreeMap::new(),
                check_cases: Vec::new(),
            }),
        }
    }

    /// The config of the app.
    pub(crate) fn config(&self) -> &AppConfig {
        &self.config
    }

    /// Register `definition`.
    ///
    /// Errors, and nothing is registered: an empty name (`HRS-001`) or
    /// definition key (`HRS-016`); a name or a definition key that another
    /// definition has (`HRS-017`); for a definition without params, a task
    /// node on a queue that the app does not admit (`HRS-014`).
    pub(crate) fn register(&self, definition: Definition) -> Result<(), HorsiesError> {
        if definition.name.trim().is_empty() {
            return Err(
                HorsiesError::new("workflow definition name must not be empty")
                    .with_code(ErrorCode::WorkflowNoName),
            );
        }
        if definition.definition_key.trim().is_empty() {
            return Err(HorsiesError::new(format!(
                "workflow definition '{}' has an empty definition key",
                definition.name
            ))
            .with_code(ErrorCode::WorkflowNoDefinitionKey));
        }
        // A spec without params converts now, so a task queue that the app
        // does not admit is refused at its registration.
        match &definition.kind {
            DefinitionKind::WithoutParams(spec) => {
                let _converted = client_spec(spec, &self.config)?;
            }
            DefinitionKind::WithParams(_) => {}
        }
        let mut registered = self.write();
        if let Some(existing) = registered.definitions.get(&definition.definition_key) {
            return Err(duplicate(
                &definition,
                format!(
                    "definition key '{}' is registered for workflow '{}'",
                    definition.definition_key, existing.name
                ),
            ));
        }
        if registered
            .definitions
            .values()
            .any(|existing| existing.name == definition.name)
        {
            return Err(duplicate(
                &definition,
                format!("workflow name '{}' is registered", definition.name),
            ));
        }
        let _previous = registered
            .definitions
            .insert(definition.definition_key.clone(), definition);
        Ok(())
    }

    /// Add the check cases `cases` of a workflow builder, which
    /// [`Self::check_findings`] builds and walks.
    pub(crate) fn add_check_cases(&self, cases: Vec<CheckCase>) {
        self.write().check_cases.extend(cases);
    }

    /// The definition registered under `definition_key`.
    pub(crate) fn definition(&self, definition_key: &str) -> Option<Definition> {
        self.read().definitions.get(definition_key).cloned()
    }

    /// `spec` checked against the registered definitions and converted to
    /// the core's spec data.
    ///
    /// Errors: every refusal of [`Self::spec_findings`] in one error; then
    /// the refusals of the conversion.
    pub(crate) fn prepare(&self, spec: &WorkflowSpec) -> Result<ClientSpec, HorsiesError> {
        let mut report = ValidationReport::new("workflow_start");
        for finding in self.spec_findings(&self.read(), spec) {
            report.add(finding);
        }
        report.into_result()?;
        client_spec(spec, &self.config)
    }

    /// Python's check of the registered workflows: each definition without
    /// params and each check case is built, checked against the registry and
    /// the app's queues, and walked into its sub-workflows down to the depth
    /// limit. A nesting is reported only when it fails for certain.
    pub(crate) fn check_findings(&self) -> Vec<HorsiesError> {
        let registered = self.read();
        let mut findings = Vec::new();
        let roots = registered
            .definitions
            .values()
            .filter_map(|definition| match &definition.kind {
                DefinitionKind::WithoutParams(spec) => Some((**spec).clone()),
                DefinitionKind::WithParams(_) => None,
            })
            .collect::<Vec<WorkflowSpec>>();
        for case in &registered.check_cases {
            match std::panic::catch_unwind(AssertUnwindSafe(|| (case.build)())) {
                Ok(Ok(spec)) => self.check_root(&registered, &spec, &mut findings),
                Ok(Err(refused)) => findings.push(
                    HorsiesError::new(format!(
                        "workflow builder '{}' refused a check case",
                        case.builder_name
                    ))
                    .with_code(ErrorCode::WorkflowCheckCaseInvalid)
                    .with_note(refused.to_string()),
                ),
                Err(panic) => findings.push(
                    HorsiesError::new(format!(
                        "workflow builder '{}' panicked on a check case",
                        case.builder_name
                    ))
                    .with_code(ErrorCode::WorkflowCheckBuilderException)
                    .with_note(panic_text(panic.as_ref()))
                    .with_help("workflow builders must return a spec or a HorsiesError"),
                ),
            }
        }
        for spec in &roots {
            self.check_root(&registered, spec, &mut findings);
        }
        findings
    }

    fn check_root(
        &self,
        registered: &Registered,
        spec: &WorkflowSpec,
        findings: &mut Vec<HorsiesError>,
    ) {
        findings.extend(self.spec_findings(registered, spec));
        if let Err(refused) = client_spec(spec, &self.config) {
            findings.push(refused);
        }
        self.walk(registered, spec, &[], findings);
    }

    /// The refusals of the sub-workflow nodes of `spec`: a child definition
    /// that is not registered (`HRS-020`), and params given to a child
    /// definition without params (`HRS-022`, Python
    /// `_collect_subworkflow_default_build_with_param_errors`).
    fn spec_findings(&self, registered: &Registered, spec: &WorkflowSpec) -> Vec<HorsiesError> {
        spec.nodes
            .iter()
            .filter_map(|node| match &node.body {
                SpecBody::SubWorkflow {
                    definition_key,
                    params,
                } => match registered.definitions.get(definition_key) {
                    None => Some(unregistered_child(spec, node, definition_key)),
                    Some(definition) => {
                        params_without_build_with(spec, node, &definition.kind, params)
                    }
                },
                SpecBody::Task { .. } => None,
            })
            .collect()
    }

    /// Walk the sub-workflow nodes of `spec`, reached by `path`, into the
    /// children that the check can build (Python `_check_subworkflow_node`).
    /// The walk is at most [`MAX_SUBWORKFLOW_DEPTH`] levels deep: the
    /// decision stops it at the limit.
    fn walk(
        &self,
        registered: &Registered,
        spec: &WorkflowSpec,
        path: &[NestingStep],
        findings: &mut Vec<HorsiesError>,
    ) {
        for node in &spec.nodes {
            let SpecBody::SubWorkflow {
                definition_key,
                params,
            } = &node.body
            else {
                continue;
            };
            // A node with args_from takes params from results at run time:
            // its child is not built.
            if !node.args_from.is_empty() {
                continue;
            }
            let Some(definition) = registered.definitions.get(definition_key) else {
                continue;
            };
            let params_text = match params {
                NodeParams::NotGiven => NO_PARAMS_TEXT.to_owned(),
                NodeParams::Given(input) => input.to_text(),
            };
            let step = NestingStep {
                definition_key: definition_key.clone(),
                params_text: params_text.clone(),
                start: match node.dependencies.is_empty() {
                    true => ChildStart::WithItsWorkflow,
                    false => ChildStart::AfterDependencies,
                },
            };
            match decide_nesting(path, &node.node_id, step) {
                NestingDecision::StopWithoutFinding => {}
                NestingDecision::StopWithFinding(finding) => findings.push(finding),
                NestingDecision::WalkChild(child_path) => {
                    if let Some(child) = child_for_check(node, definition, &params_text, findings) {
                        findings.extend(self.spec_findings(registered, &child));
                        if let Err(refused) = client_spec(&child, &self.config) {
                            findings.push(refused);
                        }
                        self.walk(registered, &child, &child_path, findings);
                    }
                }
            }
        }
    }

    /// The answer of the child spec source for the sub-workflow node of
    /// `request` (Python `build_child_workflow`), in this order:
    ///
    /// 1. The definition under the node's definition key; none fails the
    ///    node with `SUBWORKFLOW_LOAD_FAILED`.
    /// 2. The builder input that the core merges ([`merged_child_input`]):
    ///    its node failures fail the node.
    /// 3. A definition without params gives its spec; one with params
    ///    decodes the input with its params type ([`decode_params`]) and runs
    ///    `build_with`. A member that does not decode, or that no params field
    ///    takes, fails the node with `WORKER_SERIALIZATION_ERROR`; params
    ///    that do not decode as a whole with `WORKFLOW_ENQUEUE_FAILED` and
    ///    the decode's text ([`params_decode_failed_error`]); a failed build,
    ///    or a spec that the conversion refuses, with
    ///    `WORKFLOW_ENQUEUE_FAILED` and the class of the error.
    /// 4. The spec, converted to the core's spec data with the classes of
    ///    the app's queues.
    ///
    /// Errors: [`ChildSpecUnavailable::Raised`], not retryable, for a stored
    /// node input that the core's merge refuses, and for a node error that
    /// does not encode. Runs the user's `build_with`; no I/O.
    pub(crate) fn child_spec_answer(
        &self,
        request: &ChildSpecRequest,
    ) -> Result<ClientChildSpec, ChildSpecUnavailable> {
        let Some(definition) = request
            .definition_key
            .as_deref()
            .and_then(|key| self.definition(key))
        else {
            return node_fails(
                request,
                subworkflow_load_failed_error(
                    &request.parent_workflow_name,
                    request.task_index,
                    request.definition_key.as_deref(),
                ),
            );
        };
        let input = match merged_child_input(request) {
            Ok(NodeInput::Input(input)) => input,
            Ok(NodeInput::NodeFails(error)) => return node_fails(request, error),
            Err(refused) => return Err(build_raised(request, refused.to_string())),
        };
        let built = match &definition.kind {
            DefinitionKind::WithoutParams(spec) => Ok((**spec).clone()),
            DefinitionKind::WithParams(build) => build(input.text(), input.args_from_keys()),
        };
        let failure = match built {
            Ok(spec) => match client_spec(&spec, &self.config) {
                Ok(client) => return Ok(ClientChildSpec::Built(client)),
                Err(refused) => built_failed(request, &refused),
            },
            Err(ParamsBuildRefused::Member { key, cause }) => params_refused_error(
                request.parent_workflow_id,
                request.task_index,
                RefusedMember {
                    key: &key,
                    cause: &cause,
                },
                &input,
                BUILD_WITH,
            ),
            Err(ParamsBuildRefused::Undecodable { text }) => params_decode_failed_error(
                request.parent_workflow_id,
                &request.parent_workflow_name,
                request.task_index,
                &text,
            ),
            Err(ParamsBuildRefused::Failed(error)) => built_failed(request, &error),
        };
        node_fails(request, failure)
    }

    fn read(&self) -> RwLockReadGuard<'_, Registered> {
        // A registration holds the lock only to insert prepared values, and
        // a read only to copy them; neither panics with it held.
        self.registered
            .read()
            .expect("the workflow registry lock is never held across a panic")
    }

    fn write(&self) -> RwLockWriteGuard<'_, Registered> {
        self.registered
            .write()
            .expect("the workflow registry lock is never held across a panic")
    }
}

/// The registry answers the core's child builds. The answer is ready when
/// the future is made: the build runs on the step's task.
impl ChildSpecSource for WorkflowRegistry {
    fn child_spec(&self, request: ChildSpecRequest) -> ChildSpecFuture<'_> {
        let answer = self.child_spec_answer(&request);
        Box::pin(async move { answer })
    }
}

/// The answer that fails the node of `request` with `error`, stored in its
/// result envelope.
///
/// Errors: [`ChildSpecUnavailable::Raised`] when the envelope does not
/// encode. Pure.
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

/// `WORKFLOW_ENQUEUE_FAILED` of the node of `request` for a build that
/// failed with `error`. Pure.
fn built_failed(request: &ChildSpecRequest, error: &HorsiesError) -> TaskError {
    build_failed_error(
        request.parent_workflow_id,
        &request.parent_workflow_name,
        request.task_index,
        &ChildSpecError::of_builder(error),
    )
}

/// The refusal of the build of the child of `request` for `cause`. Pure.
fn build_raised(request: &ChildSpecRequest, cause: String) -> ChildSpecUnavailable {
    ChildSpecUnavailable::Raised {
        subject: format!(
            "build of the child of {}:{}",
            request.parent_workflow_name, request.task_index
        ),
        cause,
    }
}

/// Run `build`, user code that builds a spec, with a panic caught at this
/// boundary: a panic is a refusal of the build with the panic text, as
/// Python records a raised exception of `build_with`. The core then stores
/// `WORKFLOW_ENQUEUE_FAILED` on the node, and the step goes on.
fn run_user_build(
    build: impl FnOnce() -> Result<WorkflowSpec, HorsiesError>,
) -> Result<WorkflowSpec, HorsiesError> {
    match std::panic::catch_unwind(AssertUnwindSafe(build)) {
        Ok(built) => built,
        Err(panic) => Err(HorsiesError::new(format!(
            "build_with panicked: {}",
            panic_text(panic.as_ref())
        ))),
    }
}

/// The text of a panic payload: the message of `panic!` with a literal or
/// a format string; other payloads name no text.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    match (
        payload.downcast_ref::<&'static str>(),
        payload.downcast_ref::<String>(),
    ) {
        (Some(text), _) => (*text).to_owned(),
        (None, Some(text)) => text.clone(),
        (None, None) => "a panic payload that is not text".to_owned(),
    }
}

/// The params of a definition, decoded from the params text `text` with the
/// params type `P`, with the rule of the static check of a node's params
/// ([`decode_check`](super::node::decode_check)):
///
/// - `()` reads `null` and `{}`, as a task with no parameters does, and
///   takes no member ([`decode_unit_params`]).
/// - Another type decodes `text`. Then each member that is not an
///   `args_from` member (named in `result_members`) must bind to a field of
///   `P` at every depth: the encoding of the decoded params holds it
///   ([`unread_node_member`]). This is Python's refusal of a key that no
///   parameter of `build_with` takes, also for a params type that does not
///   deny unknown fields. The `args_from` members hold results: they are
///   decoded and not compared.
///
/// Errors: [`ParamsBuildRefused::Member`] for a member whose value does not
/// decode or that binds to no field, named by the first key of its path;
/// [`ParamsBuildRefused::Undecodable`] for a failure of the params as a
/// whole (a field with no member, for one). Pure.
fn decode_params<P: Serialize + DeserializeOwned + 'static>(
    text: &str,
    result_members: &[String],
) -> Result<P, ParamsBuildRefused> {
    let refused = |failure: DecodeFailure| params_refusal::<P>(&failure);
    match TypeId::of::<P>() == TypeId::of::<()>() {
        true => decode_unit_params::<P>(text),
        false => {
            let params = decode_checked::<P>(text.as_bytes()).map_err(refused)?;
            let compared = static_members(text, result_members).map_err(refused)?;
            let compared: Vec<&str> = compared.iter().map(String::as_str).collect();
            unread_node_member(text.as_bytes(), &params, &compared).map_err(refused)?;
            Ok(params)
        }
    }
}

/// The params `()` from the params text `text`. An object with no member
/// reads as `null`; the first member of an object binds to no field, with
/// the text that the static check gives for a task with no parameters. A
/// value that is not an object goes to the decode as it is: `null` decodes,
/// and the decode refuses the others.
///
/// Errors: [`ParamsBuildRefused::Member`] for a member;
/// [`ParamsBuildRefused::Undecodable`] for a text that does not decode as
/// `()`. Pure.
fn decode_unit_params<P: DeserializeOwned>(text: &str) -> Result<P, ParamsBuildRefused> {
    use serde_json::Value;
    let refused = |failure: DecodeFailure| params_refusal::<P>(&failure);
    let unit_text = match decode_checked::<Value>(text.as_bytes()).map_err(refused)? {
        Value::Object(members) => match members.into_iter().next() {
            Some((key, _value)) => {
                return Err(ParamsBuildRefused::Member {
                    cause: format!("{key}: unknown field `{key}`, there are no fields"),
                    key,
                });
            }
            None => UNIT_PARAMS_TEXT,
        },
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            text
        }
    };
    decode_checked::<P>(unit_text.as_bytes()).map_err(refused)
}

/// The members of the params text `text` that are not named in
/// `result_members`: the static members of the node input. A text that is
/// not an object has none. Pure.
///
/// Errors: the decode failure of a text that does not read as JSON.
fn static_members(text: &str, result_members: &[String]) -> Result<Vec<String>, DecodeFailure> {
    use serde_json::Value;
    Ok(match decode_checked::<Value>(text.as_bytes())? {
        Value::Object(members) => members
            .into_iter()
            .map(|(key, _value)| key)
            .filter(|key| !result_members.contains(key))
            .collect(),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Vec::new()
        }
    })
}

/// The refusal of the params type `P` for `failure`: at a member, named by
/// the first key of its path ([`ParamsBuildRefused::Member`]); at the params
/// as a whole ([`ParamsBuildRefused::Undecodable`]). Pure.
fn params_refusal<P>(failure: &DecodeFailure) -> ParamsBuildRefused {
    let key = failure
        .path
        .split(['.', '['])
        .next()
        .filter(|key| !key.is_empty())
        .map(str::to_owned);
    match key {
        Some(key) => ParamsBuildRefused::Member {
            key,
            cause: failure.to_string(),
        },
        None => ParamsBuildRefused::Undecodable {
            text: format!(
                "the params do not decode as {}: {failure}",
                std::any::type_name::<P>()
            ),
        },
    }
}

/// The child spec of a sub-workflow node for the check, or `None` when the
/// check cannot build it; a refusal of the build is a finding (Python
/// `_build_child_spec_for_check`).
fn child_for_check(
    node: &SpecNode,
    definition: &Definition,
    params_text: &str,
    findings: &mut Vec<HorsiesError>,
) -> Option<WorkflowSpec> {
    match &definition.kind {
        DefinitionKind::WithoutParams(spec) => Some((**spec).clone()),
        // The walk builds no child of a node with `args_from` members: every
        // member of the params text is a static member.
        DefinitionKind::WithParams(build) => match build(params_text, &[]) {
            Ok(spec) => Some(spec),
            Err(ParamsBuildRefused::Member { key, cause }) => {
                findings.push(
                    HorsiesError::new("subworkflow params do not fit build_with")
                        .with_code(ErrorCode::WorkflowKwargsNotSerializable)
                        .with_note(format!("node '{}': param '{key}': {cause}", node.node_id))
                        .with_help("fix the param, or the params type of the definition"),
                );
                None
            }
            Err(ParamsBuildRefused::Undecodable { text }) => {
                findings.push(
                    HorsiesError::new(text).with_code(ErrorCode::WorkflowKwargsNotSerializable),
                );
                None
            }
            Err(ParamsBuildRefused::Failed(error)) => {
                findings.push(error);
                None
            }
        },
    }
}

fn duplicate(definition: &Definition, cause: String) -> HorsiesError {
    HorsiesError::new(format!(
        "workflow definition '{}' ({}) is registered twice: {cause}",
        definition.name, definition.definition_key
    ))
    .with_code(ErrorCode::WorkflowDuplicateDefinitionKey)
    .with_help("give each workflow definition its own name and definition key")
}

fn unregistered_child(spec: &WorkflowSpec, node: &SpecNode, definition_key: &str) -> HorsiesError {
    HorsiesError::new(format!(
        "workflow '{}' node '{}' references child workflow '{}' ({definition_key}), which is not \
         registered",
        spec.name, node.node_id, node.name
    ))
    .with_code(ErrorCode::WorkflowMissingRequiredParams)
    .with_help(
        "register the child definition on the app: register_workflow_definition or \
         workflow_template",
    )
}

/// `HRS-022` for a node that gives params (a member or an `args_from`
/// member) to a definition without params.
fn params_without_build_with(
    spec: &WorkflowSpec,
    node: &SpecNode,
    kind: &DefinitionKind,
    params: &NodeParams,
) -> Option<HorsiesError> {
    let given_members: Vec<String> = match params {
        NodeParams::NotGiven => Vec::new(),
        NodeParams::Given(input) => input.names().map(str::to_owned).collect(),
    };
    let gives_params = !given_members.is_empty() || !node.args_from.is_empty();
    match (kind, gives_params) {
        (DefinitionKind::WithoutParams(_), true) => Some(
            HorsiesError::new("subworkflow params require overriding build_with")
                .with_code(ErrorCode::WorkflowSubworkflowParamsRequireBuildWith)
                .with_note(format!(
                    "workflow '{}' node '{}': subworkflow '{}' gets params but its definition \
                     takes none",
                    spec.name, node.node_id, node.name
                ))
                .with_note(format!(
                    "params: {given_members:?}, args_from: {:?}",
                    node.args_from.keys().collect::<Vec<_>>()
                ))
                .with_help(
                    "give the definition a Params type and implement build_with, and register \
                     it with workflow_template",
                ),
        ),
        (DefinitionKind::WithoutParams(_), false) | (DefinitionKind::WithParams(_), _) => None,
    }
}

// ---------------------------------------------------------------------------
// The nesting decision (Python `nesting_check.py`)
// ---------------------------------------------------------------------------

/// When the engine starts the child workflow of a sub-workflow node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildStart {
    /// The node waits for no node: its child starts with its workflow.
    WithItsWorkflow,
    /// The node waits for other nodes; their results decide whether the
    /// child starts.
    AfterDependencies,
}

/// One sub-workflow node on the path from a root spec.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NestingStep {
    definition_key: String,
    /// The stored params text of the node.
    params_text: String,
    start: ChildStart,
}

/// What the check does with the child of one node.
#[derive(Debug)]
enum NestingDecision {
    /// Build the child and walk it with this path.
    WalkChild(Vec<NestingStep>),
    /// Do not build the child: results at run time decide whether the
    /// nesting reaches the depth limit.
    StopWithoutFinding,
    /// Do not build the child: the nesting reaches the depth limit for
    /// certain.
    StopWithFinding(HorsiesError),
}

/// The decision for the child of node `node_id`, whose step is `step`,
/// reached by `path` (Python `decide_nesting`), in this order:
///
/// - The same definition with the same params is on `path`: stop; report
///   `HRS-037` when no node from the earlier one to this one waits for
///   other nodes.
/// - The child would be deeper than the limit: stop; report `HRS-036` when
///   no node on the path waits for other nodes.
/// - Otherwise walk the child.
///
/// Pure.
fn decide_nesting(path: &[NestingStep], node_id: &str, step: NestingStep) -> NestingDecision {
    let mut walked = path.to_vec();
    walked.push(step);
    let step = &walked[walked.len() - 1];
    let earlier = path.iter().position(|earlier| {
        earlier.definition_key == step.definition_key && earlier.params_text == step.params_text
    });
    if let Some(index) = earlier {
        return match starts_for_certain(&walked[index + 1..]) {
            true => NestingDecision::StopWithFinding(
                HorsiesError::new("a subworkflow includes itself with the same params")
                    .with_code(ErrorCode::WorkflowSubworkflowSelfNesting)
                    .with_note(format!(
                        "node '{node_id}': definition '{}' with these params is already on the \
                         path from the root, and no node between the two waits for other nodes",
                        step.definition_key
                    ))
                    .with_help("change the params at each level, so that the nesting ends"),
            ),
            false => NestingDecision::StopWithoutFinding,
        };
    }
    if path.len() >= MAX_SUBWORKFLOW_DEPTH {
        return match starts_for_certain(&walked) {
            true => NestingDecision::StopWithFinding(
                HorsiesError::new("subworkflow nesting passes the depth limit")
                    .with_code(ErrorCode::WorkflowSubworkflowDepthLimit)
                    .with_note(format!(
                        "node '{node_id}': its child workflow would have depth {}; the limit is \
                         {MAX_SUBWORKFLOW_DEPTH}, and no node on the path waits for other nodes",
                        walked.len()
                    ))
                    .with_help(
                        "nest fewer levels; at run time this node fails with \
                         SUBWORKFLOW_DEPTH_LIMIT_REACHED",
                    ),
            ),
            false => NestingDecision::StopWithoutFinding,
        };
    }
    NestingDecision::WalkChild(walked)
}

/// Whether the engine starts the child of each step with no wait.
fn starts_for_certain(steps: &[NestingStep]) -> bool {
    steps
        .iter()
        .all(|step| step.start == ChildStart::WithItsWorkflow)
}

#[cfg(test)]
mod tests;
