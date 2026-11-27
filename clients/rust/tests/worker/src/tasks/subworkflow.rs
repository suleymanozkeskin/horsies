//! The sub-workflow definitions and their tasks (Python
//! `clients/python/tests/e2e/tasks/workflows.py`, the `E2ESub...` definitions).
//!
//! Every process of an instance registers the definitions, so the worker
//! builds the child of each sub-workflow node from the app's registry. The
//! tasks have the code type [`WfCode`], so a child reads the failure of a
//! parent's node with its code.

use std::time::Duration;

use horsies::{
    blocking_task, task, HorsiesError, NodeKey, TaskError, TaskOutcome, WorkflowContext,
    WorkflowDefinition, WorkflowDefinitionConfig, WorkflowInput, WorkflowSpec, WorkflowSpecBuilder,
};
use serde::{Deserialize, Serialize};

use super::workflow::{slow_mark, WfCode};

/// The minimum run of a node (Python `time.sleep(0.05)`).
const NODE_RUN: Duration = Duration::from_millis(50);

/// The value of [`SubOk`]'s node (Python `e2e_sub_int_ok` default).
pub const SUB_OK_VALUE: i64 = 11;

/// The code of [`SubFail`]'s node.
pub const SUB_CHILD_FAIL: &str = "E2E_SUB_CHILD_FAIL";

/// The node ids that the probe tasks read from their context.
pub const SUCCESS_CTX_CHILD: &str = "e2e_sub_success_ctx_child";
pub const FAILURE_PROBE_CHILD: &str = "e2e_sub_failure_probe_child";

fn node_run() {
    std::thread::sleep(NODE_RUN);
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

/// Python `e2e_sub_int_ok`: returns `value`.
#[blocking_task("e2e_sub_int_ok")]
pub fn sub_int_ok(value: i64) -> Result<i64, TaskError<WfCode>> {
    node_run();
    Ok(value)
}

/// Python `e2e_sub_int_fail`: fails with the code named `error_code`.
#[blocking_task("e2e_sub_int_fail")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn sub_int_fail(error_code: String) -> Result<i64, TaskError<WfCode>> {
    node_run();
    Err(TaskError::custom(
        WfCode::of_text(&error_code),
        format!("Subworkflow failed: {error_code}"),
    ))
}

/// Python `e2e_sub_report_input`: what `build_with` saw of its input.
#[blocking_task("e2e_sub_report_input")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn sub_report_input(
    input_is_err: bool,
    input_error_code: Option<String>,
    input_value: Option<i64>,
) -> Result<serde_json::Value, TaskError<WfCode>> {
    node_run();
    Ok(serde_json::json!({
        "input_is_err": input_is_err,
        "input_error_code": input_error_code,
        "input_value": input_value,
    }))
}

/// Python `e2e_sub_pet_report`: what `build_with` saw of its typed param.
#[blocking_task("e2e_sub_pet_report")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "the parameters of a task are its wire contract: the task attribute decodes each \
              input as an owned value"
)]
pub fn sub_pet_report(
    saw_dog_at_build: bool,
    type_name_at_build: String,
    kind_at_build: Option<String>,
    bark_volume_at_build: Option<i64>,
) -> Result<serde_json::Value, TaskError<WfCode>> {
    node_run();
    Ok(serde_json::json!({
        "saw_dog_at_build": saw_dog_at_build,
        "type_name_at_build": type_name_at_build,
        "kind_at_build": kind_at_build,
        "bark_volume_at_build": bark_volume_at_build,
    }))
}

/// The code text of a failed outcome, or `None` for a success.
fn failed_code<T>(outcome: &TaskOutcome<T, WfCode>) -> Option<String> {
    match outcome {
        TaskOutcome::Succeeded(_) => None,
        TaskOutcome::Failed(error) => Some(error.code().as_str().to_owned()),
    }
}

/// The child workflow id of a sub-workflow failure, or `None` for any other
/// outcome.
fn failed_child_id<T>(outcome: &TaskOutcome<T, WfCode>) -> Option<String> {
    match outcome {
        TaskOutcome::Succeeded(_) => None,
        TaskOutcome::Failed(error) => error
            .sub_workflow()
            .map(|failure| failure.sub_workflow_id.to_string()),
    }
}

/// Python `e2e_sub_probe_failed_child`: what the node saw of a failed child,
/// through `args_from` and through its context.
#[task("e2e_sub_probe_failed_child", workflow_context)]
pub async fn sub_probe_failed_child(
    child_result: TaskOutcome<i64, WfCode>,
    workflow_context: Option<WorkflowContext>,
) -> Result<serde_json::Value, TaskError<WfCode>> {
    let context = workflow_context
        .ok_or_else(|| TaskError::custom(WfCode::NoCtx, "WorkflowContext not provided"))?;
    let key = NodeKey::<i64>::new(FAILURE_PROBE_CHILD);
    let args_child = failed_child_id(&child_result);
    let ctx_child = match context.result_for::<i64, WfCode>(&key) {
        Ok(outcome) => failed_child_id(&outcome),
        Err(missing) => return Err(TaskError::custom(WfCode::NotInContext, missing.to_string())),
    };
    let summary = context
        .summary_for(&key)
        .map_err(|missing| TaskError::custom(WfCode::NotInContext, missing.to_string()))?;
    Ok(serde_json::json!({
        "args_is_subworkflow_error": args_child.is_some(),
        "args_error_code": failed_code(&child_result),
        "ctx_is_subworkflow_error": ctx_child.is_some(),
        "ids_match": args_child.is_some() && args_child == ctx_child,
        "summary_status": summary.status,
        "summary_failed_tasks": summary.failed_nodes,
    }))
}

/// Python `e2e_sub_probe_success_ctx`: what the node saw of a completed
/// child through its context.
#[task("e2e_sub_probe_success_ctx", workflow_context)]
pub async fn sub_probe_success_ctx(
    workflow_context: Option<WorkflowContext>,
) -> Result<serde_json::Value, TaskError<WfCode>> {
    let context = workflow_context
        .ok_or_else(|| TaskError::custom(WfCode::NoCtx, "WorkflowContext not provided"))?;
    let key = NodeKey::<i64>::new(SUCCESS_CTX_CHILD);
    let result = context
        .result_for::<i64, WfCode>(&key)
        .map_err(|missing| TaskError::custom(WfCode::NotInContext, missing.to_string()))?;
    let summary = context
        .summary_for(&key)
        .map_err(|missing| TaskError::custom(WfCode::NotInContext, missing.to_string()))?;
    let output = context
        .output_for(&key)
        .map_err(|unreadable| TaskError::custom(WfCode::NotInContext, unreadable.to_string()))?;
    let result_value = match &result {
        TaskOutcome::Succeeded(value) => Some(*value),
        TaskOutcome::Failed(_) => None,
    };
    Ok(serde_json::json!({
        "result_ok": result_value.is_some(),
        "result_value": result_value,
        "result_error_code": failed_code(&result),
        "summary_status": summary.status,
        "summary_output": output,
        "summary_completed_tasks": summary.completed_nodes,
    }))
}

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

/// `builder`, a builder of the app named after `D`, with the definition
/// key and the error policy of `D`.
fn definition_builder<D: WorkflowDefinition>(
    mut builder: WorkflowSpecBuilder,
) -> WorkflowSpecBuilder {
    let _key = builder.definition_key(D::definition_key());
    let _policy = builder.on_error(D::on_error());
    builder
}

/// Python `E2ESubOkWorkflow`: one node that returns [`SUB_OK_VALUE`].
pub struct SubOk;

impl WorkflowDefinition for SubOk {
    type Output = i64;
    type Parameters = ();

    fn name() -> &'static str {
        "e2e_sub_ok_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.ok.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        let child = builder.task(
            sub_int_ok::node()
                .node_id("child")
                .set(sub_int_ok::fields::value(), SUB_OK_VALUE)?,
        )?;
        Ok(WorkflowDefinitionConfig::new().output(child))
    }
}

/// Python `E2ESubFailWorkflow`: one node that fails with
/// [`SUB_CHILD_FAIL`].
pub struct SubFail;

impl WorkflowDefinition for SubFail {
    type Output = i64;
    type Parameters = ();

    fn name() -> &'static str {
        "e2e_sub_fail_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.fail.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        let child = builder.task(sub_int_fail::node().node_id("child").set(
            sub_int_fail::fields::error_code(),
            SUB_CHILD_FAIL.to_owned(),
        )?)?;
        Ok(WorkflowDefinitionConfig::new().output(child))
    }
}

/// Python `E2ESubNestedOkMiddleWorkflow`: one sub-workflow node of
/// [`SubOk`].
pub struct SubNestedOkMiddle;

impl WorkflowDefinition for SubNestedOkMiddle {
    type Output = i64;
    type Parameters = ();

    fn name() -> &'static str {
        "e2e_sub_nested_ok_middle_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.nested.ok.middle.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        let child = builder
            .sub_workflow(SubOk::node(horsies::SubWorkflowParameters::NotGiven)?.node_id("child"));
        Ok(WorkflowDefinitionConfig::new().output(child))
    }
}

/// Python `E2ESubNestedFailMiddleWorkflow`: one sub-workflow node of
/// [`SubFail`].
pub struct SubNestedFailMiddle;

impl WorkflowDefinition for SubNestedFailMiddle {
    type Output = i64;
    type Parameters = ();

    fn name() -> &'static str {
        "e2e_sub_nested_fail_middle_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.nested.fail.middle.v1"
    }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefinitionConfig, HorsiesError> {
        let child = builder.sub_workflow(
            SubFail::node(horsies::SubWorkflowParameters::NotGiven)?.node_id("child"),
        );
        Ok(WorkflowDefinitionConfig::new().output(child))
    }
}

/// The params of [`SubParam`] and [`SubFailedInput`]: the result of an
/// upstream node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, WorkflowInput)]
pub struct UpstreamParams {
    pub value: TaskOutcome<i64, WfCode>,
}

/// Python `E2ESubParamWorkflow`: one node that returns the upstream value,
/// or -1 for an upstream failure.
pub struct SubParam;

impl WorkflowDefinition for SubParam {
    type Output = i64;
    type Parameters = UpstreamParams;

    fn name() -> &'static str {
        "e2e_sub_param_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.param.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: UpstreamParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let value = match params.value {
            TaskOutcome::Succeeded(value) => value,
            TaskOutcome::Failed(_) => -1,
        };
        let mut builder = definition_builder::<Self>(builder);
        let child = builder.task(
            sub_int_ok::node()
                .node_id("child")
                .set(sub_int_ok::fields::value(), value)?,
        )?;
        let _output = builder.output(child);
        builder.build()
    }
}

/// Python `E2ESubFailedInputWorkflow`: one node that reports the upstream
/// outcome that `build_with` received.
pub struct SubFailedInput;

impl WorkflowDefinition for SubFailedInput {
    type Output = serde_json::Value;
    type Parameters = UpstreamParams;

    fn name() -> &'static str {
        "e2e_sub_failed_input_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.failed.input.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: UpstreamParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let (input_is_err, input_error_code, input_value) = match params.value {
            TaskOutcome::Succeeded(value) => (false, None, Some(value)),
            TaskOutcome::Failed(error) => (true, Some(error.code().as_str().to_owned()), None),
        };
        let mut builder = definition_builder::<Self>(builder);
        let child = builder.task(
            sub_report_input::node()
                .node_id("child")
                .set(sub_report_input::fields::input_is_err(), input_is_err)?
                .set(
                    sub_report_input::fields::input_error_code(),
                    input_error_code,
                )?
                .set(sub_report_input::fields::input_value(), input_value)?,
        )?;
        let _output = builder.output(child);
        builder.build()
    }
}

/// The params of [`SubLabel`]: a static label and the result of an
/// upstream node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, WorkflowInput)]
pub struct LabelParams {
    pub label: String,
    pub value: TaskOutcome<i64, WfCode>,
}

/// One node that returns `<label>=<value>`: the static param and the
/// `args_from` param that `build_with` received.
pub struct SubLabel;

impl WorkflowDefinition for SubLabel {
    type Output = String;
    type Parameters = LabelParams;

    fn name() -> &'static str {
        "e2e_sub_label_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.label.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: LabelParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let value = match params.value {
            TaskOutcome::Succeeded(value) => value.to_string(),
            TaskOutcome::Failed(error) => error.code().as_str().to_owned(),
        };
        let mut builder = definition_builder::<Self>(builder);
        let child = builder.task(super::workflow::mark::node().node_id("child").set(
            super::workflow::mark::fields::value(),
            format!("{}={value}", params.label),
        )?)?;
        let _output = builder.output(child);
        builder.build()
    }
}

/// Python `E2ESubPet`: a param whose variant the stored form keeps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Pet {
    Cat { name: String },
    Dog { name: String, bark_volume: i64 },
}

/// The params of [`SubTypedKwarg`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, WorkflowInput)]
pub struct PetParams {
    pub pet: Pet,
}

/// Python `E2ESubTypedKwargWorkflow`: one node that reports the variant of
/// the static param that `build_with` received.
pub struct SubTypedKwarg;

impl WorkflowDefinition for SubTypedKwarg {
    type Output = serde_json::Value;
    type Parameters = PetParams;

    fn name() -> &'static str {
        "e2e_sub_typed_kwarg_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.typed.kwarg.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: PetParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let (saw_dog, type_name, kind, bark_volume) = match params.pet {
            Pet::Cat { name: _ } => (false, "Cat", "cat", None),
            Pet::Dog {
                name: _,
                bark_volume,
            } => (true, "Dog", "dog", Some(bark_volume)),
        };
        let mut builder = definition_builder::<Self>(builder);
        let child = builder.task(
            sub_pet_report::node()
                .node_id("child")
                .set(sub_pet_report::fields::saw_dog_at_build(), saw_dog)?
                .set(
                    sub_pet_report::fields::type_name_at_build(),
                    type_name.to_owned(),
                )?
                .set(
                    sub_pet_report::fields::kind_at_build(),
                    Some(kind.to_owned()),
                )?
                .set(sub_pet_report::fields::bark_volume_at_build(), bark_volume)?,
        )?;
        let _output = builder.output(child);
        builder.build()
    }
}

/// The params of [`SubSlow`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, WorkflowInput)]
pub struct SlowParams {
    pub value: String,
    pub delay_ms: u64,
}

/// Python `E2ESubSlowWorkflow`: one node that returns `value` after
/// `delay_ms`.
pub struct SubSlow;

impl WorkflowDefinition for SubSlow {
    type Output = String;
    type Parameters = SlowParams;

    fn name() -> &'static str {
        "e2e_sub_slow_workflow"
    }

    fn definition_key() -> &'static str {
        "tests.e2e.sub.slow.v1"
    }

    fn build_with(
        builder: WorkflowSpecBuilder,
        params: SlowParams,
    ) -> Result<WorkflowSpec, HorsiesError> {
        let mut builder = definition_builder::<Self>(builder);
        let child = builder.task(
            slow_mark::node()
                .node_id("child")
                .set(slow_mark::fields::value(), params.value)?
                .set(slow_mark::fields::delay_ms(), params.delay_ms)?,
        )?;
        let _output = builder.output(child);
        builder.build()
    }
}

/// Register the sub-workflow tasks, then the definitions: a definition
/// without params builds its spec when it registers, from the nodes of
/// registered tasks.
pub fn register(app: &mut horsies::Horsies) -> Result<(), HorsiesError> {
    let _int_ok = sub_int_ok::register(app)?;
    let _int_fail = sub_int_fail::register(app)?;
    let _report_input = sub_report_input::register(app)?;
    let _pet_report = sub_pet_report::register(app)?;
    let _probe_failed = sub_probe_failed_child::register(app)?;
    let _probe_success = sub_probe_success_ctx::register(app)?;
    let _ok = app.register_workflow_definition::<SubOk>()?;
    let _fail = app.register_workflow_definition::<SubFail>()?;
    let _nested_ok = app.register_workflow_definition::<SubNestedOkMiddle>()?;
    let _nested_fail = app.register_workflow_definition::<SubNestedFailMiddle>()?;
    let _param = app.register_workflow_definition_with::<SubParam>()?;
    let _failed_input = app.register_workflow_definition_with::<SubFailedInput>()?;
    let _label = app.register_workflow_definition_with::<SubLabel>()?;
    let _typed_kwarg = app.register_workflow_definition_with::<SubTypedKwarg>()?;
    let _slow = app.register_workflow_definition_with::<SubSlow>()?;
    Ok(())
}
