//! The tasks of the workflow layers (Python `tests/e2e/tasks/workflows.py`).
//! Every node task has the code type [`WfCode`], so a node reads the
//! failure of any upstream node with its code.

use std::collections::BTreeMap;
use std::time::Duration;

use horsies::{
    blocking_task, task, NodeKey, TaskError, TaskOutcome, UserErrorCode, WorkflowContext,
};

/// The minimum run of a node: distinct timestamps between nodes (Python
/// `time.sleep(0.05)`).
const NODE_RUN: Duration = Duration::from_millis(50);

/// The codes of the workflow tasks' own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
pub enum WfCode {
    /// `e2e_wf_fail`: the deliberate failure of a node.
    DeliberateFailure,
    /// A context node ran without a workflow context.
    NoCtx,
    /// A context node did not find the node it reads.
    NotInContext,
}

fn node_run() {
    std::thread::sleep(NODE_RUN);
}

/// Python `e2e_wf_step`.
#[blocking_task("e2e_wf_step")]
pub fn step(step: String) -> Result<String, TaskError<WfCode>> {
    node_run();
    Ok(format!("completed_{step}"))
}

/// Python `e2e_wf_slow_step`.
#[blocking_task("e2e_wf_slow_step")]
pub fn slow_step(step: String, delay_ms: u64) -> Result<String, TaskError<WfCode>> {
    std::thread::sleep(Duration::from_millis(delay_ms));
    Ok(format!("completed_{step}"))
}

/// Python `e2e_wf_final_result`.
#[blocking_task("e2e_wf_final_result")]
pub fn final_result() -> Result<serde_json::Value, TaskError<WfCode>> {
    node_run();
    Ok(serde_json::json!({ "final": "result", "count": 42 }))
}

/// Python `e2e_wf_fail`: always fails with `DELIBERATE_FAILURE`.
#[blocking_task("e2e_wf_fail")]
pub fn fail() -> Result<i64, TaskError<WfCode>> {
    node_run();
    Err(TaskError::user(
        WfCode::DeliberateFailure,
        "Failed: DELIBERATE_FAILURE",
    ))
}

/// Python `e2e_wf_produce_int`.
#[blocking_task("e2e_wf_produce_int")]
pub fn produce_int(value: i64) -> Result<i64, TaskError<WfCode>> {
    node_run();
    Ok(value)
}

/// Python `e2e_wf_double`: twice its upstream value, or the upstream error.
#[task("e2e_wf_double")]
pub async fn double(input_result: TaskOutcome<i64, WfCode>) -> Result<i64, TaskError<WfCode>> {
    match input_result {
        TaskOutcome::Succeeded(value) => Ok(value * 2),
        TaskOutcome::Failed(error) => Err(error),
    }
}

/// Python `e2e_wf_sum_two`.
#[task("e2e_wf_sum_two")]
pub async fn sum_two(
    first: TaskOutcome<i64, WfCode>,
    second: TaskOutcome<i64, WfCode>,
) -> Result<i64, TaskError<WfCode>> {
    match (first, second) {
        (TaskOutcome::Succeeded(first), TaskOutcome::Succeeded(second)) => Ok(first + second),
        (TaskOutcome::Failed(error), _) | (_, TaskOutcome::Failed(error)) => Err(error),
    }
}

/// Python `e2e_wf_check_upstream_skipped`: which outcome reached it.
#[task("e2e_wf_check_upstream_skipped")]
pub async fn check_upstream_skipped(
    input_result: TaskOutcome<i64, WfCode>,
) -> Result<String, TaskError<WfCode>> {
    Ok(match input_result {
        TaskOutcome::Succeeded(value) => format!("received_value_{value}"),
        TaskOutcome::Failed(error) if error.code().text() == "UPSTREAM_SKIPPED" => {
            "received_upstream_skipped".to_owned()
        }
        TaskOutcome::Failed(error) => format!("received_error_{}", error.code().text()),
    })
}

/// Python `e2e_wf_produce_dict`.
#[task("e2e_wf_produce_dict")]
pub async fn produce_dict(value: String) -> Result<serde_json::Value, TaskError<WfCode>> {
    Ok(serde_json::json!({ "key": value, "count": 42, "nested": { "inner": true } }))
}

/// Python `e2e_wf_receive_dict`.
#[task("e2e_wf_receive_dict")]
pub async fn receive_dict(
    input_result: TaskOutcome<serde_json::Value, WfCode>,
) -> Result<serde_json::Value, TaskError<WfCode>> {
    match input_result {
        TaskOutcome::Succeeded(value) => Ok(value),
        TaskOutcome::Failed(error) => Err(error),
    }
}

/// The value of node `node_id` in `context`, read as an integer.
fn context_value(
    context: Option<&WorkflowContext>,
    node_id: &str,
) -> Result<i64, TaskError<WfCode>> {
    let context =
        context.ok_or_else(|| TaskError::user(WfCode::NoCtx, "WorkflowContext not provided"))?;
    match context.result_for::<i64, WfCode>(&NodeKey::new(node_id)) {
        Ok(TaskOutcome::Succeeded(value)) => Ok(value),
        Ok(TaskOutcome::Failed(error)) => Err(error),
        Err(missing) => Err(TaskError::user(WfCode::NotInContext, missing.to_string())),
    }
}

/// Python `e2e_wf_ctx_reader`: the value of node `source` from the context.
#[task("e2e_wf_ctx_reader", workflow_ctx)]
pub async fn ctx_reader(
    source: String,
    workflow_ctx: Option<WorkflowContext>,
) -> Result<i64, TaskError<WfCode>> {
    context_value(workflow_ctx.as_ref(), &source)
}

/// Python `e2e_wf_ctx_sum`: the sum of nodes `first` and `second` from the
/// context.
#[task("e2e_wf_ctx_sum", workflow_ctx)]
pub async fn ctx_sum(
    first: String,
    second: String,
    workflow_ctx: Option<WorkflowContext>,
) -> Result<i64, TaskError<WfCode>> {
    let first = context_value(workflow_ctx.as_ref(), &first)?;
    let second = context_value(workflow_ctx.as_ref(), &second)?;
    Ok(first + second)
}

/// Python `e2e_wf_mixed_reader` and `e2e_wf_dual_reader`: one value from
/// `args_from`, one from the context node `ctx_source`.
#[task("e2e_wf_mixed_reader", workflow_ctx)]
pub async fn mixed_reader(
    from_args: TaskOutcome<i64, WfCode>,
    ctx_source: String,
    workflow_ctx: Option<WorkflowContext>,
) -> Result<BTreeMap<String, i64>, TaskError<WfCode>> {
    let from_args = match from_args {
        TaskOutcome::Succeeded(value) => value,
        TaskOutcome::Failed(error) => return Err(error),
    };
    let from_ctx = context_value(workflow_ctx.as_ref(), &ctx_source)?;
    Ok(BTreeMap::from([
        ("from_args".to_owned(), from_args),
        ("from_ctx".to_owned(), from_ctx),
    ]))
}
