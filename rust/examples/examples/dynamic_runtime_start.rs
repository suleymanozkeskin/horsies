//! Dynamic workflow start from inside a task via `TaskRuntime`.
//!
//! This example is intentionally small: it shows the task signature and the
//! runtime-built workflow shape without requiring a full worker setup.

use horsies::{
    task, AppConfig, Horsies, HorsiesError, PostgresConfig, QueueMode, TaskError, TaskResult,
    TaskRuntime, WorkflowInput, WorkflowSpec, WorkflowSpecBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProduceInput {
    value: i64,
}

#[task("produce_value")]
async fn produce_value(input: ProduceInput) -> Result<serde_json::Value, TaskError> {
    Ok(serde_json::json!({ "value": input.value }))
}

/// Input for double_value via args_from — receives TaskResult wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, WorkflowInput)]
struct DoubleInput {
    input_result: TaskResult<serde_json::Value>,
}

#[task("double_value")]
async fn double_value(input: DoubleInput) -> Result<serde_json::Value, TaskError> {
    let value = match input.input_result {
        TaskResult::Ok(v) => v,
        TaskResult::Err(e) => {
            return Err(TaskError::new(
                "UPSTREAM_FAILED",
                format!("upstream failed: {:?}", e.error_code),
            ))
        }
    };
    Ok(serde_json::json!({ "doubled": value }))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceInput {
    value: i64,
}

fn build_child_spec(input: &SourceInput) -> Result<WorkflowSpec, HorsiesError> {
    let mut builder = WorkflowSpecBuilder::new("example_dynamic_child");
    builder.definition_key("examples.dynamic_child.v1");

    let produce = builder.task(
        produce_value::node()?
            .set_input(ProduceInput { value: input.value })?
            .node_id("produce"),
    );
    let doubled = builder.task(
        double_value::node()?
            .node_id("double")
            .arg_from(DoubleInput::field_input_result(), produce),
    );
    builder.output(doubled);
    builder.build()
}

#[task("start_dynamic_child")]
async fn start_dynamic_child(rt: TaskRuntime, input: SourceInput) -> Result<String, TaskError> {
    let spec = build_child_spec(&input)
        .map_err(|err| TaskError::new("WF_BUILD_FAILED", err.to_string()))?;
    let handle = rt
        .start::<serde_json::Value>(spec)
        .await
        .map_err(|err| TaskError::new("WF_START_FAILED", err.message))?;
    Ok(handle.workflow_id().to_owned())
}

fn config() -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Default,
        custom_queues: None,
        broker: PostgresConfig {
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://localhost/horsies_example".to_owned()),
            pool_pre_ping: true,
            pool_size: 5,
            max_overflow: 5,
            pool_timeout: 30,
            pool_recycle: 1800,
            echo: false,
        },
        resend_on_transient_err: false,
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: horsies::RecoveryConfig::default(),
        resilience: horsies::WorkerResilienceConfig::default(),
        schedule: None,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = Horsies::new(config())?;
    produce_value::register(&mut app)?;
    double_value::register(&mut app)?;
    let _task = start_dynamic_child::register(&mut app)?;

    println!("registered `start_dynamic_child` with typed workflow nodes");
    println!("run this task inside a worker and call `rt.start(spec)` for dynamic workflows");

    Ok(())
}
