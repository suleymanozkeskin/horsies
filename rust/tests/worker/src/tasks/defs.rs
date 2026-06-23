/// E2E task definitions: input types, task functions, and `#[task]`-macro
/// wrappers. Shared by every queue-mode registrar in this crate.
///
/// Mirrors Python's `tests/e2e/tasks/basic.py`, `retry.py`, `workflows.py`.
use serde::{Deserialize, Serialize};

use horsies::{
    task, Horsies, HorsiesError, NodeKey, RetryPolicy, SubWorkflowNode, TaskError, TaskFunction,
    TaskResult, WorkflowInput, WorkflowSpec, WorkflowSpecBuilder,
};

// =============================================================================
// Input types
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct SimpleInput {
    pub x: i64,
}

#[derive(Debug, Deserialize)]
pub struct KwargsInput {
    pub required: i64,
    #[serde(default = "default_optional")]
    pub optional: String,
    #[serde(default = "default_one")]
    pub multiplier: i64,
}
fn default_optional() -> String {
    "default".to_owned()
}
fn default_one() -> i64 {
    1
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SlowInput {
    pub duration_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StepInput {
    pub step: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SlowStepInput {
    pub step: String,
    #[serde(default = "default_500")]
    pub delay_ms: u64,
}
fn default_500() -> u64 {
    500
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FailInput {
    pub error_code: String,
}

// =============================================================================
// Basic task fns (no macro)
// =============================================================================

pub async fn healthcheck(_input: ()) -> Result<String, TaskError> {
    Ok("ready".to_owned())
}

pub async fn simple_task(input: SimpleInput) -> Result<i64, TaskError> {
    Ok(input.x * 2)
}

pub async fn kwargs_task(input: KwargsInput) -> Result<String, TaskError> {
    Ok(format!(
        "{}_{}",
        input.required * input.multiplier,
        input.optional
    ))
}

pub async fn error_task(_input: ()) -> Result<i64, TaskError> {
    Err(TaskError::new("DELIBERATE_ERROR", "This is intentional"))
}

pub async fn slow_task(input: SlowInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(input.duration_ms)).await;
    Ok(format!("slept_{}", input.duration_ms))
}

pub async fn no_retry_task(_input: ()) -> Result<String, TaskError> {
    Err(TaskError::new("PERMANENT", "not retryable"))
}

/// Sleeps past its 1s `timeout_ms` so the worker-side deadline fires and the
/// task fails with `TASK_TIMEOUT` (no retry opt-in). Parity with horsies PR #102.
#[task("e2e_timeout_sleeper", timeout_ms = 1000)]
pub async fn timeout_sleeper(input: SlowInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(input.duration_ms)).await;
    Ok(format!("slept_{}", input.duration_ms))
}

/// Same deadline, but opts into retry on `TASK_TIMEOUT` (one retry, 1s delay).
/// It sleeps past the deadline every attempt, so it retries once then exhausts
/// to FAILED.
#[task(
    "e2e_timeout_retry",
    timeout_ms = 1000,
    auto_retry_for = ["TASK_TIMEOUT"],
    retry_policy = RetryPolicy::fixed(vec![1], false).unwrap()
)]
pub async fn timeout_retry_sleeper(input: SlowInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(input.duration_ms)).await;
    Ok(format!("slept_{}", input.duration_ms))
}

// =============================================================================
// Workflow tasks (#[task] macro)
// =============================================================================

#[task("e2e_wf_step")]
pub async fn wf_step(input: StepInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(format!("completed_{}", input.step))
}

#[task("e2e_wf_slow_step")]
pub async fn wf_slow_step(input: SlowStepInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(input.delay_ms)).await;
    Ok(format!("completed_{}", input.step))
}

#[task("e2e_wf_final_result")]
pub async fn wf_final_result(_input: ()) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(serde_json::json!({"final": "result", "count": 42}))
}

#[task("e2e_wf_fail")]
pub async fn wf_fail(input: FailInput) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Err(TaskError::new(
        &input.error_code,
        format!("Failed: {}", input.error_code),
    ))
}

#[task("e2e_wf_fail_int")]
pub async fn wf_fail_int(input: FailInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Err(TaskError::new(
        &input.error_code,
        format!("Failed: {}", input.error_code),
    ))
}

// =============================================================================
// Dynamic workflow start from inside a task (TaskRuntime injection)
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct DynamicStartInput {
    pub value: i64,
}

#[task("e2e_rt_ping")]
pub async fn rt_ping(_: ()) -> Result<String, TaskError> {
    Ok("pong".to_owned())
}

#[task("e2e_dynamic_rt_start")]
pub async fn dynamic_rt_start(
    rt: horsies::TaskRuntime,
    input: DynamicStartInput,
) -> Result<String, TaskError> {
    let mut builder = WorkflowSpecBuilder::new("e2e_dynamic_rt_child");
    builder.definition_key("tests.e2e_dynamic_rt_child.v1");

    let produce = builder.task(
        wf_produce_int::node()
            .map_err(|e| TaskError::new("NODE_ERROR", e.to_string()))?
            .node_id("produce")
            .kwargs_json(serde_json::json!({ "value": input.value }).to_string()),
    );
    let doubled = builder.task(
        wf_double::node()
            .map_err(|e| TaskError::new("NODE_ERROR", e.to_string()))?
            .node_id("double")
            .arg_from(DoubleInput::field_input_result(), produce),
    );
    builder.output(doubled);

    let spec = builder
        .build()
        .map_err(|err| TaskError::new("WF_BUILD_FAILED", err.to_string()))?;
    let handle = rt
        .start::<serde_json::Value>(spec)
        .await
        .map_err(|err| TaskError::new("WF_START_FAILED", err.message))?;
    Ok(handle.workflow_id().to_owned())
}

#[task("e2e_runtime_helper_dispatch")]
pub async fn runtime_helper_dispatch(_rt: horsies::TaskRuntime) -> Result<String, TaskError> {
    let handle = rt_ping::send(())
        .await
        .map_err(|err| TaskError::new("SEND_FAILED", err.message))?;
    Ok(handle.task_id().to_owned())
}

#[task("e2e_runtime_helper_schedule")]
pub async fn runtime_helper_schedule(_rt: horsies::TaskRuntime) -> Result<String, TaskError> {
    let handle = rt_ping::schedule(std::time::Duration::from_secs(5), ())
        .await
        .map_err(|err| TaskError::new("SCHEDULE_FAILED", err.message))?;
    Ok(handle.task_id().to_owned())
}

#[task("e2e_runtime_helper_handle")]
pub async fn runtime_helper_handle(rt: horsies::TaskRuntime) -> Result<String, TaskError> {
    let ping = rt_ping::handle(&rt)?;
    let handle = ping
        .send(())
        .await
        .map_err(|err| TaskError::new("SEND_FAILED", err.message))?;
    Ok(handle.task_id().to_owned())
}

#[task("e2e_dynamic_rt_start_no_args")]
pub async fn dynamic_rt_start_no_args(rt: horsies::TaskRuntime) -> Result<String, TaskError> {
    let mut builder = WorkflowSpecBuilder::new("e2e_dynamic_rt_child_no_args");
    builder.definition_key("tests.e2e_dynamic_rt_child_no_args.v1");

    let produce = builder.task(
        wf_produce_int::node()
            .map_err(|e| TaskError::new("NODE_ERROR", e.to_string()))?
            .node_id("produce")
            .kwargs_json(serde_json::json!({ "value": 21 }).to_string()),
    );
    let doubled = builder.task(
        wf_double::node()
            .map_err(|e| TaskError::new("NODE_ERROR", e.to_string()))?
            .node_id("double")
            .arg_from(DoubleInput::field_input_result(), produce),
    );
    builder.output(doubled);

    let spec = builder
        .build()
        .map_err(|err| TaskError::new("WF_BUILD_FAILED", err.to_string()))?;
    let handle = rt
        .start::<serde_json::Value>(spec)
        .await
        .map_err(|err| TaskError::new("WF_START_FAILED", err.message))?;
    Ok(handle.workflow_id().to_owned())
}

// =============================================================================
// Retry tasks
// =============================================================================

pub async fn retry_exhausted(_input: ()) -> Result<String, TaskError> {
    Err(TaskError::new("TRANSIENT", "always fails"))
}

pub async fn retry_success(_input: ()) -> Result<String, TaskError> {
    let state_path = std::env::var("E2E_RETRY_SUCCESS_PATH").unwrap_or_default();
    if state_path.is_empty() {
        return Err(TaskError::new(
            "CONFIG_ERROR",
            "E2E_RETRY_SUCCESS_PATH not set",
        ));
    }

    let count = tokio::fs::read_to_string(&state_path)
        .await
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(0)
        + 1;

    let _ = tokio::fs::write(&state_path, count.to_string()).await;

    if count < 3 {
        Err(TaskError::new("TRANSIENT", format!("attempt {}", count)))
    } else {
        Ok(format!("succeeded_on_attempt_{}", count))
    }
}

// =============================================================================
// Idempotent file-token task
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct IdempotentInput {
    pub token: String,
}

pub async fn idempotent_task(input: IdempotentInput) -> Result<String, TaskError> {
    let log_dir = std::env::var("E2E_IDEMPOTENT_LOG_DIR").unwrap_or_default();
    if log_dir.is_empty() {
        return Err(TaskError::new(
            "CONFIG_ERROR",
            "E2E_IDEMPOTENT_LOG_DIR not set",
        ));
    }

    let token_file = std::path::PathBuf::from(&log_dir).join(&input.token);

    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&token_file)
    {
        Ok(mut f) => {
            use std::io::Write;
            let _ = f.write_all(b"executed");
            Ok(format!("executed:{}", input.token))
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(TaskError::new(
            "DOUBLE_EXECUTION",
            format!("Token {} already executed", input.token),
        )),
        Err(e) => Err(TaskError::new("IO_ERROR", format!("{}", e))),
    }
}

// =============================================================================
// Slow idempotent task (softcap lease contention tests)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct SlowIdempotentInput {
    pub token: String,
    #[serde(default = "default_100")]
    pub duration_ms: u64,
}
fn default_100() -> u64 {
    100
}

pub async fn slow_idempotent_task(input: SlowIdempotentInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(input.duration_ms)).await;

    let log_dir = std::env::var("E2E_IDEMPOTENT_LOG_DIR").unwrap_or_default();
    if log_dir.is_empty() {
        return Err(TaskError::new(
            "CONFIG_ERROR",
            "E2E_IDEMPOTENT_LOG_DIR not set",
        ));
    }

    let token_file = std::path::PathBuf::from(&log_dir).join(&input.token);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&token_file)
    {
        Ok(mut f) => {
            use std::io::Write;
            let _ = f.write_all(b"executed");
            Ok(format!("executed:{}", input.token))
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(TaskError::new(
            "DOUBLE_EXECUTION",
            format!("Token {} already executed", input.token),
        )),
        Err(e) => Err(TaskError::new("IO_ERROR", format!("{}", e))),
    }
}

// =============================================================================
// Complex result task
// =============================================================================

pub async fn complex_result_task(_input: ()) -> Result<serde_json::Value, TaskError> {
    Ok(serde_json::json!({
        "value": 42,
        "nested": {"a": [1, 2, 3], "b": [4, 5]}
    }))
}

// =============================================================================
// Library error code task
// =============================================================================

pub async fn error_code_task(_input: ()) -> Result<serde_json::Value, TaskError> {
    Err(TaskError::builtin(
        horsies::OperationalErrorCode::TaskError,
        "boom",
    ))
}

// =============================================================================
// Workflow retry tasks
// =============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub struct RetryThenOkInput {
    pub counter_file: String,
    #[serde(default = "default_succeed_on_3")]
    pub succeed_on_attempt: i32,
}
fn default_succeed_on_3() -> i32 {
    3
}

#[task("e2e_wf_retry_then_ok")]
pub async fn wf_retry_then_ok(input: RetryThenOkInput) -> Result<String, TaskError> {
    let count = tokio::fs::read_to_string(&input.counter_file)
        .await
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(0)
        + 1;
    let _ = tokio::fs::write(&input.counter_file, count.to_string()).await;

    if count < input.succeed_on_attempt {
        Err(TaskError::new(
            "TRANSIENT",
            format!("attempt {} of {}", count, input.succeed_on_attempt),
        ))
    } else {
        Ok(format!("succeeded_on_attempt_{}", count))
    }
}

#[task(
    "e2e_wf_retry_via_registration",
    auto_retry_for = ["TRANSIENT"],
    retry_policy = horsies::RetryPolicy::fixed(vec![1, 1, 1], false).unwrap()
)]
pub async fn wf_retry_via_registration(input: RetryThenOkInput) -> Result<String, TaskError> {
    let count = tokio::fs::read_to_string(&input.counter_file)
        .await
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(0)
        + 1;
    let _ = tokio::fs::write(&input.counter_file, count.to_string()).await;

    if count < input.succeed_on_attempt {
        Err(TaskError::new(
            "TRANSIENT",
            format!("attempt {} of {}", count, input.succeed_on_attempt),
        ))
    } else {
        Ok(format!("succeeded_on_attempt_{}", count))
    }
}

// =============================================================================
// Dict serialization tasks
// =============================================================================

#[task("e2e_wf_produce_dict")]
pub async fn wf_produce_dict(_input: ()) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(serde_json::json!({
        "name": "Alice",
        "scores": [95, 87, 92],
        "nested": {"key": "value"}
    }))
}

#[derive(Debug, Deserialize, Serialize, WorkflowInput)]
pub struct ReadDictInput {
    pub input_result: TaskResult<serde_json::Value>,
}

#[task("e2e_wf_read_dict")]
pub async fn wf_read_dict(input: ReadDictInput) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    match input.input_result {
        TaskResult::Ok(v) => {
            let mut obj = v.as_object().cloned().unwrap_or_default();
            obj.insert("received".to_owned(), serde_json::json!(true));
            Ok(serde_json::Value::Object(obj))
        }
        TaskResult::Err(e) => Err(e),
    }
}

// =============================================================================
// DB ledger task (softcap race detection)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LedgerInput {
    pub token: String,
}

fn resolve_db_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let root = std::path::Path::new(manifest_dir)
        .ancestors()
        .find(|p| p.join(".env").exists());
    if let Some(root) = root {
        if let Ok(contents) = std::fs::read_to_string(root.join(".env")) {
            for line in contents.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    if key.trim() == "DB_PASSWORD" {
                        return format!(
                            "postgresql://postgres:{}@localhost:5432/horsies-rust-port",
                            value.trim(),
                        );
                    }
                }
            }
        }
    }
    panic!("database URL not found: set DATABASE_URL or add DB_PASSWORD to .env");
}

pub async fn db_ledger_task(input: LedgerInput) -> Result<String, TaskError> {
    let db_url = resolve_db_url();

    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .map_err(|e| TaskError::new("LEDGER_DB_ERROR", format!("connect failed: {}", e)))?;

    let worker_identity = format!(
        "{}:{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id(),
    );
    let worker_pid = std::process::id() as i32;

    sqlx::query(
        "INSERT INTO e2e_execution_attempts(token, worker_identity, worker_pid) VALUES ($1, $2, $3)",
    )
    .bind(&input.token)
    .bind(&worker_identity)
    .bind(worker_pid)
    .execute(&pool)
    .await
    .map_err(|e| TaskError::new("LEDGER_DB_ERROR", format!("{}", e)))?;

    let winner: Option<(String,)> = sqlx::query_as(
        "INSERT INTO e2e_execution_winner(token, worker_identity, worker_pid) \
         VALUES ($1, $2, $3) ON CONFLICT (token) DO NOTHING RETURNING token",
    )
    .bind(&input.token)
    .bind(&worker_identity)
    .bind(worker_pid)
    .fetch_optional(&pool)
    .await
    .map_err(|e| TaskError::new("LEDGER_DB_ERROR", format!("{}", e)))?;

    pool.close().await;

    if winner.is_none() {
        return Err(TaskError::new(
            "DOUBLE_EXECUTION",
            format!("Token {} already won by another worker", input.token),
        ));
    }

    Ok(format!("winner:{}:{}", input.token, worker_identity))
}

// =============================================================================
// Requeue-guard task (fault-injection tests)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RequeueGuardInput {
    pub token: String,
}

pub async fn requeue_guard_task(input: RequeueGuardInput) -> Result<String, TaskError> {
    let log_dir = std::env::var("E2E_REQUEUE_GUARD_LOG_DIR").unwrap_or_default();
    if log_dir.is_empty() {
        return Err(TaskError::new(
            "CONFIG_ERROR",
            "E2E_REQUEUE_GUARD_LOG_DIR not set",
        ));
    }

    let token_file = std::path::PathBuf::from(&log_dir).join(&input.token);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&token_file)
    {
        Ok(mut f) => {
            use std::io::Write;
            let _ = writeln!(f, "executed_by_pid_{}", std::process::id());
            Ok(format!("done:{}", input.token))
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(TaskError::new(
            "DOUBLE_EXECUTION",
            format!("Token {} already executed", input.token),
        )),
        Err(e) => Err(TaskError::new("IO_ERROR", format!("{}", e))),
    }
}

// =============================================================================
// Workflow context tasks
// =============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub struct CtxReaderInput {
    #[serde(default)]
    pub workflow_ctx: Option<horsies::WorkflowContext>,
}

#[task("e2e_wf_ctx_reader", workflow_ctx)]
pub async fn wf_ctx_reader(input: CtxReaderInput) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let ctx = input
        .workflow_ctx
        .ok_or_else(|| TaskError::new("NO_CTX", "WorkflowContext not provided"))?;

    let mut results = serde_json::Map::new();
    let ctx_json = serde_json::to_value(&ctx).unwrap_or(serde_json::Value::Null);
    if let Some(by_id) = ctx_json.get("results_by_id") {
        if let Some(obj) = by_id.as_object() {
            for (node_id, result_val) in obj {
                results.insert(node_id.clone(), result_val.clone());
            }
        }
    }

    Ok(serde_json::json!({
        "workflow_id": ctx.workflow_id,
        "task_index": ctx.task_index,
        "result_count": results.len(),
        "results": results,
    }))
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CtxSumInput {
    #[serde(default)]
    pub workflow_ctx: Option<horsies::WorkflowContext>,
}

#[task("e2e_wf_ctx_sum", workflow_ctx)]
pub async fn wf_ctx_sum(input: CtxSumInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let ctx = input
        .workflow_ctx
        .ok_or_else(|| TaskError::new("NO_CTX", "WorkflowContext not provided"))?;

    let ctx_json = serde_json::to_value(&ctx).unwrap_or(serde_json::Value::Null);
    let mut total: i64 = 0;
    if let Some(by_id) = ctx_json.get("results_by_id").and_then(|v| v.as_object()) {
        for (_node_id, result_val) in by_id {
            if let Ok(TaskResult::Ok(v)) =
                serde_json::from_value::<TaskResult<serde_json::Value>>(result_val.clone())
            {
                if let Some(n) = v.as_i64() {
                    total += n;
                }
            }
        }
    }
    Ok(total)
}

#[derive(Debug, Deserialize, Serialize, WorkflowInput)]
pub struct MixedInput {
    pub input_result: TaskResult<i64>,
    #[serde(default)]
    pub workflow_ctx: Option<horsies::WorkflowContext>,
}

#[task("e2e_wf_mixed", workflow_ctx)]
pub async fn wf_mixed(input: MixedInput) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let has_args_from = true;
    let has_ctx = input.workflow_ctx.is_some();

    let args_from_value = match input.input_result {
        TaskResult::Ok(val) => Some(val),
        TaskResult::Err(_) => None,
    };

    Ok(serde_json::json!({
        "has_args_from": has_args_from,
        "has_ctx": has_ctx,
        "args_from_value": args_from_value,
    }))
}

// =============================================================================
// Data flow tasks
// =============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub struct ProduceIntInput {
    pub value: i64,
}

#[task("e2e_wf_produce_int")]
pub async fn wf_produce_int(input: ProduceIntInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(input.value)
}

#[derive(Debug, Deserialize, Serialize, WorkflowInput)]
pub struct DoubleInput {
    pub input_result: TaskResult<i64>,
}

#[task("e2e_wf_double")]
pub async fn wf_double(input: DoubleInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    match input.input_result {
        TaskResult::Ok(v) => Ok(v * 2),
        TaskResult::Err(e) => Err(e),
    }
}

#[derive(Debug, Deserialize, Serialize, WorkflowInput)]
pub struct SumTwoInput {
    pub first: TaskResult<i64>,
    pub second: TaskResult<i64>,
}

#[task("e2e_wf_sum_two")]
pub async fn wf_sum_two(input: SumTwoInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let a = match input.first {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => return Err(e),
    };
    let b = match input.second {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => return Err(e),
    };
    Ok(a + b)
}

// Multi-parameter task: exercises the macro-generated `Input` struct (the
// "Wrapped" shape), which carries `#[serde(deny_unknown_fields)]`. Used to
// prove (a) `args_from` injection of declared fields still deserializes, and
// (b) an extra/undeclared kwarg is rejected at execution.
#[task("e2e_wf_combine_wrapped")]
pub async fn wf_combine_wrapped(
    first: TaskResult<i64>,
    second: TaskResult<i64>,
) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let a = match first {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => return Err(e),
    };
    let b = match second {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => return Err(e),
    };
    Ok(a + b)
}

// =============================================================================
// Child-label workflow (sub-workflow tests)
// =============================================================================

#[derive(Debug, Deserialize, Serialize, WorkflowInput, Clone)]
pub struct ChildLabelInput {
    pub input_result: TaskResult<i64>,
    pub label: String,
}

// Registered manually so tests can use it in both Default and Custom queue modes.
pub async fn wf_child_label(input: ChildLabelInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    match input.input_result {
        TaskResult::Ok(v) => Ok(format!("{}={}", input.label, v)),
        TaskResult::Err(e) => Err(e),
    }
}

fn build_child_label_workflow(
    task: &TaskFunction<ChildLabelInput, String>,
    params: ChildLabelInput,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("e2e_child_label_pipeline");
    builder.definition_key("tests.e2e_child_label_pipeline.v1");
    let render = builder.task(task.node().set_input(params)?.node_id("render"));
    builder.output(render);
    builder.build()
}

pub(crate) fn register_child_label_workflow(
    app: &mut Horsies,
    task: &TaskFunction<ChildLabelInput, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let task = task.clone();
    let _template = app.register_parameterized_workflow::<ChildLabelInput, String, _>(
        "e2e_child_label_pipeline",
        "tests.e2e_child_label_pipeline.v1",
        move |params| build_child_label_workflow(&task, params),
    )?;
    Ok(())
}

// =============================================================================
// Sub-workflow context reader (layer-7 e2e matrix)
// =============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub struct SubwfCtxReaderInput {
    #[serde(default)]
    pub workflow_ctx: Option<horsies::WorkflowContext>,
}

/// Reads every sub-workflow read surface for the single upstream sub-workflow
/// node injected into its context: `summary_for`, `output_for`, `result_for`.
/// The reader declares `workflow_ctx_from = [child]`, so exactly one summary is
/// present; it discovers that node_id from the context rather than hard-coding
/// the `node_id:index` format.
#[task("e2e_wf_subwf_ctx_reader", workflow_ctx)]
pub async fn wf_subwf_ctx_reader(
    input: SubwfCtxReaderInput,
) -> Result<serde_json::Value, TaskError> {
    let ctx = input
        .workflow_ctx
        .ok_or_else(|| TaskError::new("NO_CTX", "WorkflowContext not provided"))?;

    let ctx_json = serde_json::to_value(&ctx).unwrap_or(serde_json::Value::Null);
    let node_id = ctx_json
        .get("summaries_by_id")
        .and_then(|v| v.as_object())
        .and_then(|m| m.keys().next().cloned())
        .ok_or_else(|| TaskError::new("NO_SUMMARY", "no sub-workflow summary in context"))?;

    let key: NodeKey<String> = NodeKey::new(node_id.clone());
    let summary = ctx.summary_for(&key)?;
    let output = ctx.output_for(&key)?;
    let result = ctx.result_for(&key);
    let result_is_ok = matches!(result, Ok(TaskResult::Ok(_)));

    Ok(serde_json::json!({
        "node_id": node_id,
        "summary_status": summary.status,
        "summary_failed_tasks": summary.failed_tasks,
        "summary_completed_tasks": summary.completed_tasks,
        "summary_is_success": summary.is_success,
        "summary_child_workflow_id": summary.child_workflow_id,
        "output": output,
        "result_is_ok": result_is_ok,
    }))
}

// =============================================================================
// Nested sub-workflow pipelines (2-level nesting; layer-7 e2e matrix)
// =============================================================================

#[derive(Debug, Deserialize, Serialize, WorkflowInput, Clone)]
pub struct NestedParentInput {
    pub value: i64,
    pub label: String,
}

/// Success path: `produce_int(value)` -> grandchild `e2e_child_label_pipeline`
/// (label + produced int). Output is the grandchild's `"label=value"` string.
/// Two sub-workflow levels below the top workflow.
fn build_nested_parent_workflow(params: NestedParentInput) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("e2e_nested_parent_pipeline");
    builder.definition_key("tests.e2e_nested_parent_pipeline.v1");
    let produce = builder.task(
        wf_produce_int::node()?
            .node_id("n_produce")
            .set_input(ProduceIntInput { value: params.value })?,
    );
    let grandchild = builder.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("n_child")
            .queue("default")
            .set(ChildLabelInput::field_label(), params.label)?
            .arg_from(ChildLabelInput::field_input_result(), produce),
    );
    builder.output(grandchild);
    builder.build()
}

/// Failure path: `wf_fail_int` -> grandchild `e2e_child_label_pipeline`
/// (allow_failed_deps), so the failed `TaskResult` flows into the grandchild,
/// whose `wf_child_label` re-raises it. Failure surfaces at every level.
fn build_nested_failing_workflow(params: NestedParentInput) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("e2e_nested_failing_pipeline");
    builder.definition_key("tests.e2e_nested_failing_pipeline.v1");
    let failing = builder.task(
        wf_fail_int::node()?
            .node_id("n_fail")
            .set_input(FailInput {
                error_code: "NESTED_INNER_FAIL".to_owned(),
            })?,
    );
    let grandchild = builder.sub_workflow(
        SubWorkflowNode::<ChildLabelInput, String>::typed("e2e_child_label_pipeline")
            .node_id("n_child")
            .queue("default")
            .set(ChildLabelInput::field_label(), params.label)?
            .arg_from(ChildLabelInput::field_input_result(), failing)
            .allow_failed_deps(true),
    );
    builder.output(grandchild);
    builder.build()
}

#[derive(Debug, Deserialize, Serialize, WorkflowInput, Clone)]
pub struct FailingChildInput {
    pub error_code: String,
}

/// A child workflow that always fails intrinsically (single `wf_fail` node), so
/// a top-level sub-workflow node referencing it is the first/only failure —
/// surfacing `SUBWORKFLOW_FAILED` at the parent rather than an upstream task's
/// error.
fn build_failing_child_workflow(
    params: FailingChildInput,
) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("e2e_child_failing_pipeline");
    builder.definition_key("tests.e2e_child_failing_pipeline.v1");
    let boom = builder.task(
        wf_fail::node()?
            .node_id("boom")
            .set_input(FailInput {
                error_code: params.error_code,
            })?,
    );
    builder.output(boom);
    builder.build()
}

pub(crate) fn register_nested_workflows(
    app: &mut Horsies,
) -> Result<(), Box<dyn std::error::Error>> {
    app.register_parameterized_workflow::<FailingChildInput, serde_json::Value, _>(
        "e2e_child_failing_pipeline",
        "tests.e2e_child_failing_pipeline.v1",
        build_failing_child_workflow,
    )?;
    app.register_parameterized_workflow::<NestedParentInput, String, _>(
        "e2e_nested_parent_pipeline",
        "tests.e2e_nested_parent_pipeline.v1",
        build_nested_parent_workflow,
    )?;
    app.register_parameterized_workflow::<NestedParentInput, String, _>(
        "e2e_nested_failing_pipeline",
        "tests.e2e_nested_failing_pipeline.v1",
        build_nested_failing_workflow,
    )?;
    Ok(())
}
