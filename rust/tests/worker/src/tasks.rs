/// E2E test task definitions.
///
/// Mirrors Python's `tests/e2e/tasks/basic.py`, `retry.py`, `workflows.py`.
use serde::{Deserialize, Serialize};

use horsies::{
    async_task_fn, task, Horsies, TaskError, TaskErrorCode, TaskFunction, TaskNode, TaskOptions,
    TaskRuntime, WorkflowSpecBuilder,
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

#[derive(Debug, Deserialize)]
pub struct SlowInput {
    pub duration_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct StepInput {
    pub step: String,
}

#[derive(Debug, Deserialize)]
pub struct SlowStepInput {
    pub step: String,
    #[serde(default = "default_500")]
    pub delay_ms: u64,
}
fn default_500() -> u64 {
    500
}

#[derive(Debug, Deserialize)]
pub struct FailInput {
    pub error_code: String,
}

// =============================================================================
// Basic tasks
// =============================================================================

async fn healthcheck(_input: ()) -> Result<String, TaskError> {
    Ok("ready".to_owned())
}

async fn simple_task(input: SimpleInput) -> Result<i64, TaskError> {
    Ok(input.x * 2)
}

async fn kwargs_task(input: KwargsInput) -> Result<String, TaskError> {
    Ok(format!(
        "{}_{}",
        input.required * input.multiplier,
        input.optional
    ))
}

async fn error_task(_input: ()) -> Result<i64, TaskError> {
    Err(TaskError::user("DELIBERATE_ERROR", "This is intentional"))
}

async fn slow_task(input: SlowInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(input.duration_ms)).await;
    Ok(format!("slept_{}", input.duration_ms))
}

async fn no_retry_task(_input: ()) -> Result<String, TaskError> {
    Err(TaskError::user("PERMANENT", "not retryable"))
}

// =============================================================================
// Workflow tasks
// =============================================================================

async fn wf_step(input: StepInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(format!("completed_{}", input.step))
}

async fn wf_slow_step(input: SlowStepInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(input.delay_ms)).await;
    Ok(format!("completed_{}", input.step))
}

async fn wf_final_result(_input: ()) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(serde_json::json!({"final": "result", "count": 42}))
}

async fn wf_fail(input: FailInput) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Err(TaskError::user(
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

pub struct RuntimeDispatchState {
    pub ping: TaskFunction<(), String>,
}

#[task("e2e_rt_ping")]
async fn rt_ping(_: ()) -> Result<String, TaskError> {
    Ok("pong".to_owned())
}

#[task("e2e_dynamic_rt_start")]
async fn dynamic_rt_start(rt: TaskRuntime, input: DynamicStartInput) -> Result<String, TaskError> {
    let mut builder = WorkflowSpecBuilder::new("e2e_dynamic_rt_child");
    builder.definition_key("tests.e2e_dynamic_rt_child.v1");

    let produce = builder.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("produce")
            .kwargs_json(serde_json::json!({ "value": input.value }).to_string()),
    );
    let doubled = builder.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_double")
            .node_id("double")
            .args_from("input_result", produce),
    );
    builder.output(doubled);

    let spec = builder
        .build()
        .map_err(|err| TaskError::user("WF_BUILD_FAILED", err.to_string()))?;
    let handle = rt
        .start::<serde_json::Value>(spec)
        .await
        .map_err(|err| TaskError::user("WF_START_FAILED", err.message))?;
    Ok(handle.workflow_id().to_owned())
}

#[task("e2e_runtime_state_dispatch")]
async fn runtime_state_dispatch(rt: TaskRuntime) -> Result<String, TaskError> {
    let state = rt.state::<RuntimeDispatchState>()?;
    let handle = state
        .ping
        .send(())
        .await
        .map_err(|err| TaskError::user("SEND_FAILED", err.message))?;
    Ok(handle.task_id().to_owned())
}

#[task("e2e_dynamic_rt_start_no_args")]
async fn dynamic_rt_start_no_args(rt: TaskRuntime) -> Result<String, TaskError> {
    let mut builder = WorkflowSpecBuilder::new("e2e_dynamic_rt_child_no_args");
    builder.definition_key("tests.e2e_dynamic_rt_child_no_args.v1");

    let produce = builder.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_produce_int")
            .node_id("produce")
            .kwargs_json(serde_json::json!({ "value": 21 }).to_string()),
    );
    let doubled = builder.task(
        TaskNode::<serde_json::Value>::new("e2e_wf_double")
            .node_id("double")
            .args_from("input_result", produce),
    );
    builder.output(doubled);

    let spec = builder
        .build()
        .map_err(|err| TaskError::user("WF_BUILD_FAILED", err.to_string()))?;
    let handle = rt
        .start::<serde_json::Value>(spec)
        .await
        .map_err(|err| TaskError::user("WF_START_FAILED", err.message))?;
    Ok(handle.workflow_id().to_owned())
}

// =============================================================================
// Retry tasks
// =============================================================================

async fn retry_exhausted(_input: ()) -> Result<String, TaskError> {
    Err(TaskError::user("TRANSIENT", "always fails"))
}

async fn retry_success(_input: ()) -> Result<String, TaskError> {
    let state_path = std::env::var("E2E_RETRY_SUCCESS_PATH").unwrap_or_default();
    if state_path.is_empty() {
        return Err(TaskError::user(
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
        Err(TaskError::user("TRANSIENT", format!("attempt {}", count)))
    } else {
        Ok(format!("succeeded_on_attempt_{}", count))
    }
}

// =============================================================================
// Idempotent file-token task (for double-execution detection)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct IdempotentInput {
    pub token: String,
}

async fn idempotent_task(input: IdempotentInput) -> Result<String, TaskError> {
    let log_dir = std::env::var("E2E_IDEMPOTENT_LOG_DIR").unwrap_or_default();
    if log_dir.is_empty() {
        return Err(TaskError::user(
            "CONFIG_ERROR",
            "E2E_IDEMPOTENT_LOG_DIR not set",
        ));
    }

    let token_file = std::path::PathBuf::from(&log_dir).join(&input.token);

    // O_CREAT | O_EXCL equivalent: create file exclusively, fails if exists.
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
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(TaskError::user(
            "DOUBLE_EXECUTION",
            format!("Token {} already executed", input.token),
        )),
        Err(e) => Err(TaskError::user("IO_ERROR", format!("{}", e))),
    }
}

// =============================================================================
// Slow idempotent task (for softcap lease contention tests)
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

async fn slow_idempotent_task(input: SlowIdempotentInput) -> Result<String, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(input.duration_ms)).await;

    let log_dir = std::env::var("E2E_IDEMPOTENT_LOG_DIR").unwrap_or_default();
    if log_dir.is_empty() {
        return Err(TaskError::user(
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
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(TaskError::user(
            "DOUBLE_EXECUTION",
            format!("Token {} already executed", input.token),
        )),
        Err(e) => Err(TaskError::user("IO_ERROR", format!("{}", e))),
    }
}

// =============================================================================
// Complex result task (nested struct serialization)
// =============================================================================

async fn complex_result_task(_input: ()) -> Result<serde_json::Value, TaskError> {
    Ok(serde_json::json!({
        "value": 42,
        "nested": {"a": [1, 2, 3], "b": [4, 5]}
    }))
}

// =============================================================================
// Library error code task (returns OperationalErrorCode)
// =============================================================================

async fn error_code_task(_input: ()) -> Result<serde_json::Value, TaskError> {
    Err(TaskError::builtin(
        horsies::OperationalErrorCode::TaskError,
        "boom",
    ))
}

// =============================================================================
// Workflow retry task (counter-file based, succeeds on attempt N)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RetryThenOkInput {
    pub counter_file: String,
    #[serde(default = "default_succeed_on_3")]
    pub succeed_on_attempt: i32,
}
fn default_succeed_on_3() -> i32 {
    3
}

async fn wf_retry_then_ok(input: RetryThenOkInput) -> Result<String, TaskError> {
    // Read current attempt count, increment, write back.
    let count = tokio::fs::read_to_string(&input.counter_file)
        .await
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(0)
        + 1;
    let _ = tokio::fs::write(&input.counter_file, count.to_string()).await;

    if count < input.succeed_on_attempt {
        Err(TaskError::user(
            "TRANSIENT",
            format!("attempt {} of {}", count, input.succeed_on_attempt),
        ))
    } else {
        Ok(format!("succeeded_on_attempt_{}", count))
    }
}

// =============================================================================
// Dict serialization tasks (for args_from dict roundtrip tests)
// =============================================================================

async fn wf_produce_dict(_input: ()) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(serde_json::json!({
        "name": "Alice",
        "scores": [95, 87, 92],
        "nested": {"key": "value"}
    }))
}

#[derive(Debug, Deserialize)]
pub struct ReadDictInput {
    pub input_result: serde_json::Value,
}

async fn wf_read_dict(input: ReadDictInput) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let tr: horsies::TaskResult<serde_json::Value> = serde_json::from_value(input.input_result)
        .map_err(|e| TaskError::user("DESER_ERROR", format!("{}", e)))?;
    match tr {
        horsies::TaskResult::Ok(v) => {
            // Return the dict with an added field proving we read it.
            let mut obj = v.as_object().cloned().unwrap_or_default();
            obj.insert("received".to_owned(), serde_json::json!(true));
            Ok(serde_json::Value::Object(obj))
        }
        horsies::TaskResult::Err(e) => Err(e),
    }
}

// =============================================================================
// DB ledger task (for softcap race detection)
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

async fn db_ledger_task(input: LedgerInput) -> Result<String, TaskError> {
    let db_url = resolve_db_url();

    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .map_err(|e| TaskError::user("LEDGER_DB_ERROR", format!("connect failed: {}", e)))?;

    let worker_identity = format!(
        "{}:{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id(),
    );
    let worker_pid = std::process::id() as i32;

    // Record body entry.
    sqlx::query(
        "INSERT INTO e2e_execution_attempts(token, worker_identity, worker_pid) VALUES ($1, $2, $3)",
    )
    .bind(&input.token)
    .bind(&worker_identity)
    .bind(worker_pid)
    .execute(&pool)
    .await
    .map_err(|e| TaskError::user("LEDGER_DB_ERROR", format!("{}", e)))?;

    // Claim winner (ON CONFLICT DO NOTHING — first one wins).
    let winner: Option<(String,)> = sqlx::query_as(
        "INSERT INTO e2e_execution_winner(token, worker_identity, worker_pid) \
         VALUES ($1, $2, $3) ON CONFLICT (token) DO NOTHING RETURNING token",
    )
    .bind(&input.token)
    .bind(&worker_identity)
    .bind(worker_pid)
    .fetch_optional(&pool)
    .await
    .map_err(|e| TaskError::user("LEDGER_DB_ERROR", format!("{}", e)))?;

    pool.close().await;

    if winner.is_none() {
        return Err(TaskError::user(
            "DOUBLE_EXECUTION",
            format!("Token {} already won by another worker", input.token),
        ));
    }

    Ok(format!("winner:{}:{}", input.token, worker_identity))
}

// =============================================================================
// Requeue-guard task (for fault injection tests)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RequeueGuardInput {
    pub token: String,
}

async fn requeue_guard_task(input: RequeueGuardInput) -> Result<String, TaskError> {
    let log_dir = std::env::var("E2E_REQUEUE_GUARD_LOG_DIR").unwrap_or_default();
    if log_dir.is_empty() {
        return Err(TaskError::user(
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
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(TaskError::user(
            "DOUBLE_EXECUTION",
            format!("Token {} already executed", input.token),
        )),
        Err(e) => Err(TaskError::user("IO_ERROR", format!("{}", e))),
    }
}

// =============================================================================
// Workflow context tasks (for workflow_ctx_from tests)
// =============================================================================

/// Task that reads an integer from workflow context by node_id.
#[derive(Debug, Deserialize)]
pub struct CtxReaderInput {
    #[serde(default)]
    pub workflow_ctx: Option<horsies::WorkflowContext>,
}

async fn wf_ctx_reader(input: CtxReaderInput) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let ctx = input
        .workflow_ctx
        .ok_or_else(|| TaskError::user("NO_CTX", "WorkflowContext not provided"))?;

    // Return a summary of what's in the context.
    let mut results = serde_json::Map::new();
    // Access internal results — iterate the node_ids we can see.
    // We use has_result to check, then serialize what we find.
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

/// Task that sums all integer Ok values from workflow context.
#[derive(Debug, Deserialize)]
pub struct CtxSumInput {
    #[serde(default)]
    pub workflow_ctx: Option<horsies::WorkflowContext>,
}

async fn wf_ctx_sum(input: CtxSumInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let ctx = input
        .workflow_ctx
        .ok_or_else(|| TaskError::user("NO_CTX", "WorkflowContext not provided"))?;

    let ctx_json = serde_json::to_value(&ctx).unwrap_or(serde_json::Value::Null);
    let mut total: i64 = 0;
    if let Some(by_id) = ctx_json.get("results_by_id").and_then(|v| v.as_object()) {
        for (_node_id, result_val) in by_id {
            // Parse as TaskResult<Value>
            if let Ok(horsies::TaskResult::Ok(v)) =
                serde_json::from_value::<horsies::TaskResult<serde_json::Value>>(result_val.clone())
            {
                if let Some(n) = v.as_i64() {
                    total += n;
                }
            }
        }
    }
    Ok(total)
}

/// Task that receives both args_from and workflow_ctx.
#[derive(Debug, Deserialize)]
pub struct MixedInput {
    #[serde(default)]
    pub input_result: Option<serde_json::Value>,
    #[serde(default)]
    pub workflow_ctx: Option<horsies::WorkflowContext>,
}

async fn wf_mixed(input: MixedInput) -> Result<serde_json::Value, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let has_args_from = input.input_result.is_some();
    let has_ctx = input.workflow_ctx.is_some();

    let mut args_from_value: Option<i64> = None;
    if let Some(ref v) = input.input_result {
        if let Ok(horsies::TaskResult::Ok(val)) =
            serde_json::from_value::<horsies::TaskResult<serde_json::Value>>(v.clone())
        {
            args_from_value = val.as_i64();
        }
    }

    Ok(serde_json::json!({
        "has_args_from": has_args_from,
        "has_ctx": has_ctx,
        "args_from_value": args_from_value,
    }))
}

// =============================================================================
// Data flow tasks (for workflow args_from tests)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct ProduceIntInput {
    pub value: i64,
}

async fn wf_produce_int(input: ProduceIntInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Ok(input.value)
}

/// Receives a TaskResult via args_from and doubles the Ok value.
/// The input comes as the serialized TaskResult JSON from the upstream task.
#[derive(Debug, Deserialize)]
pub struct DoubleInput {
    pub input_result: serde_json::Value,
}

async fn wf_double(input: DoubleInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Parse the injected TaskResult.
    let tr: horsies::TaskResult<serde_json::Value> = serde_json::from_value(input.input_result)
        .map_err(|e| TaskError::user("DESER_ERROR", format!("{}", e)))?;
    match tr {
        horsies::TaskResult::Ok(v) => {
            let n = v.as_i64().unwrap_or(0);
            Ok(n * 2)
        }
        horsies::TaskResult::Err(e) => Err(e),
    }
}

/// Receives two TaskResults via args_from and sums their Ok values.
#[derive(Debug, Deserialize)]
pub struct SumTwoInput {
    pub first: serde_json::Value,
    pub second: serde_json::Value,
}

async fn wf_sum_two(input: SumTwoInput) -> Result<i64, TaskError> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let first: horsies::TaskResult<serde_json::Value> = serde_json::from_value(input.first)
        .map_err(|e| TaskError::user("DESER_ERROR", format!("{}", e)))?;
    let second: horsies::TaskResult<serde_json::Value> = serde_json::from_value(input.second)
        .map_err(|e| TaskError::user("DESER_ERROR", format!("{}", e)))?;

    let a = match first {
        horsies::TaskResult::Ok(v) => v.as_i64().unwrap_or(0),
        horsies::TaskResult::Err(e) => return Err(e),
    };
    let b = match second {
        horsies::TaskResult::Ok(v) => v.as_i64().unwrap_or(0),
        horsies::TaskResult::Err(e) => return Err(e),
    };
    Ok(a + b)
}

// =============================================================================
// Registration
// =============================================================================

pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
    // Basic
    app.register("e2e_healthcheck", async_task_fn!(healthcheck, ()))?;
    app.register("e2e_simple", async_task_fn!(simple_task, SimpleInput))?;
    app.register("e2e_kwargs", async_task_fn!(kwargs_task, KwargsInput))?;
    app.register("e2e_error", async_task_fn!(error_task, ()))?;
    app.register("e2e_slow", async_task_fn!(slow_task, SlowInput))?;
    app.register("e2e_no_retry", async_task_fn!(no_retry_task, ()))?;
    app.register(
        "e2e_idempotent",
        async_task_fn!(idempotent_task, IdempotentInput),
    )?;

    // Workflow
    app.register("e2e_wf_step", async_task_fn!(wf_step, StepInput))?;
    app.register(
        "e2e_wf_slow_step",
        async_task_fn!(wf_slow_step, SlowStepInput),
    )?;
    app.register("e2e_wf_final_result", async_task_fn!(wf_final_result, ()))?;
    app.register("e2e_wf_fail", async_task_fn!(wf_fail, FailInput))?;
    let ping = rt_ping::register(app)?;
    dynamic_rt_start::register(app)?;
    dynamic_rt_start_no_args::register(app)?;
    runtime_state_dispatch::register(app)?;
    app.provide(RuntimeDispatchState { ping })?;

    // Complex result + error code
    app.register(
        "e2e_complex_result",
        async_task_fn!(complex_result_task, ()),
    )?;
    app.register("e2e_error_code", async_task_fn!(error_code_task, ()))?;

    // Retry
    app.register("e2e_retry_exhausted", async_task_fn!(retry_exhausted, ()))?;
    app.register("e2e_retry_success", async_task_fn!(retry_success, ()))?;

    // Workflow context tasks (with_workflow_ctx)
    app.register(
        "e2e_wf_ctx_reader",
        async_task_fn!(wf_ctx_reader, CtxReaderInput).with_workflow_ctx(),
    )?;
    app.register(
        "e2e_wf_ctx_sum",
        async_task_fn!(wf_ctx_sum, CtxSumInput).with_workflow_ctx(),
    )?;
    app.register(
        "e2e_wf_mixed",
        async_task_fn!(wf_mixed, MixedInput).with_workflow_ctx(),
    )?;

    // Data flow (args_from tests)
    app.register(
        "e2e_wf_produce_int",
        async_task_fn!(wf_produce_int, ProduceIntInput),
    )?;
    app.register("e2e_wf_double", async_task_fn!(wf_double, DoubleInput))?;
    app.register("e2e_wf_sum_two", async_task_fn!(wf_sum_two, SumTwoInput))?;
    app.register("e2e_wf_produce_dict", async_task_fn!(wf_produce_dict, ()))?;
    app.register(
        "e2e_wf_read_dict",
        async_task_fn!(wf_read_dict, ReadDictInput),
    )?;

    // Workflow retry task
    app.register(
        "e2e_wf_retry_then_ok",
        async_task_fn!(wf_retry_then_ok, RetryThenOkInput),
    )?;

    // Softcap-specific (same functions, different names for instance parity)
    app.register("e2e_softcap_blocker", async_task_fn!(slow_task, SlowInput))?;
    app.register(
        "e2e_softcap_slow_idempotent",
        async_task_fn!(slow_idempotent_task, SlowIdempotentInput),
    )?;

    // Custom queue tasks (all use the same slow function — queue routing is config-driven)
    app.register("e2e_custom_slow", async_task_fn!(slow_task, SlowInput))?;
    app.register("e2e_high", async_task_fn!(healthcheck, ()))?;
    app.register("e2e_normal", async_task_fn!(healthcheck, ()))?;
    app.register("e2e_low", async_task_fn!(healthcheck, ()))?;

    // Cluster cap tasks
    app.register("e2e_cluster_cap_slow", async_task_fn!(slow_task, SlowInput))?;
    app.register("e2e_cluster_cap_fail", async_task_fn!(error_task, ()))?;

    // Recovery tasks
    app.register("e2e_recovery_healthcheck", async_task_fn!(healthcheck, ()))?;
    app.register("e2e_recovery_slow", async_task_fn!(slow_task, SlowInput))?;

    // DB ledger task (for softcap race tests)
    app.register(
        "e2e_softcap_db_ledger",
        async_task_fn!(db_ledger_task, LedgerInput),
    )?;

    // Requeue guard task (for fault injection tests)
    app.register(
        "e2e_requeue_guard",
        async_task_fn!(requeue_guard_task, RequeueGuardInput),
    )?;

    // Workflow task with retry options on the REGISTRATION (not on the node).
    // Used by T6.10c to verify that workflow nodes inherit task-level retry config.
    app.register(
        "e2e_wf_retry_via_registration",
        async_task_fn!(wf_retry_then_ok, RetryThenOkInput).with_task_options(TaskOptions {
            task_name: "e2e_wf_retry_via_registration".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User("TRANSIENT".to_owned())]),
            retry_policy: Some(horsies::RetryPolicy::fixed(vec![1, 1, 1], false).unwrap()),
        }),
    )?;

    // Scheduler tasks
    app.register("e2e_scheduled_simple", async_task_fn!(healthcheck, ()))?;
    app.register(
        "e2e_scheduled_with_args",
        async_task_fn!(simple_task, SimpleInput),
    )?;
    app.register("e2e_catch_up_task", async_task_fn!(healthcheck, ()))?;

    Ok(())
}
